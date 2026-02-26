#!/usr/bin/env python3
"""
Arbitrage Insights — WebSocket Collector
=========================================
Connects to exchange WebSocket feeds for real-time price updates.
Maintains in-memory price cache and computes spreads every 300 ms.
Writes pre-built JSON snapshots to Redis (arb:snap:*) so the API
process (app.py with COLLECTOR_ONLY=1) can serve them instantly.

Architecture:
  ┌────────────────────────┐   in-memory    ┌───────────────────────────┐
  │  MEXC   WS  (bulk)     │ ─────────────▶ │  prices[exch][norm_sym]   │
  │  Bybit  WS  (batched)  │ ─────────────▶ │  = MarketRow              │
  │  BingX  WS  (per-sym)  │ ─────────────▶ │                           │
  └────────────────────────┘                └──────────┬────────────────┘
                                                        │ every 300 ms
                                                        ▼
                                            best_pairs() + _rebuild_data_cache()
                                            → arb:snap:guest / paid / admin  (Redis)
                                            → arb:sse  pub/sub  (→ SSE clients)

Setup:
  # Terminal / systemd service 1 — WS collector:
  REDIS_URL=redis://localhost:6379/0 python ws_collector.py

  # Terminal / systemd service 2 — API (HTTP only):
  REDIS_URL=redis://localhost:6379/0 COLLECTOR_ONLY=1 python app.py

Notes:
  • All three WS loops auto-reconnect with exponential backoff (1–60 s).
  • Bybit delta updates are merged into snapshot state before creating a MarketRow.
  • BingX messages are gzip-compressed; both TEXT and BINARY frames are handled.
  • Snapshot writes are throttled to SNAPSHOT_THROTTLE_SEC (default 0.3 s) and
    only pushed when the ETag changes (data actually changed).
  • MEXC funding intervals are still loaded from Redis / REST in background.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import math
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Optional

# ─── Bootstrap: must happen before importing app.py ──────────────────────────
sys.path.insert(0, str(Path(__file__).parent))
os.environ["COLLECTOR_ONLY"] = "1"   # prevent app.py lifespan from starting updater_loop

import aiohttp  # noqa: E402  (after sys.path insert)

try:
    import orjson as _json   # noqa: E402  — fast JSON: 3-5× faster than stdlib
    def _loads(s: str) -> dict:
        return _json.loads(s)
except ImportError:
    import json as _stdlib_json
    def _loads(s: str) -> dict:  # type: ignore[misc]
        return _stdlib_json.loads(s)

# ─── Logging ─────────────────────────────────────────────────────────────────
_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
for _uv in ("uvicorn", "uvicorn.error", "uvicorn.access"):
    logging.getLogger(_uv).propagate = False

logger = logging.getLogger("ws_collector")

# ─── Config ──────────────────────────────────────────────────────────────────
SNAPSHOT_THROTTLE    = float(os.getenv("SNAPSHOT_THROTTLE_SEC", "0.3"))   # seconds
MAX_RECONNECT_DELAY  = float(os.getenv("MAX_RECONNECT_DELAY",    "60.0"))  # seconds
WS_PING_MEXC         = float(os.getenv("WS_PING_MEXC",           "15.0"))  # seconds
WS_PING_BYBIT        = float(os.getenv("WS_PING_BYBIT",          "20.0"))  # seconds
WS_PING_BINGX        = float(os.getenv("WS_PING_BINGX",          "20.0"))  # seconds
MAX_BYBIT_BATCH      = int(os.getenv("MAX_BYBIT_BATCH",           "10"))    # args per subscribe
MAX_BINGX_SYMS       = int(os.getenv("MAX_BINGX_SYMS",            "200"))   # symbols to subscribe
MAX_ROWS             = int(os.getenv("MAX_ROWS",                   "300"))   # cap result set

MEXC_WS_URL  = "wss://contract.mexc.com/edge"
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"
BINGX_WS_URL = "wss://open-api-ws.bingx.com/market"

# ─── In-memory price cache ────────────────────────────────────────────────────
# prices[exchange][norm_symbol] = MarketRow  (norm_symbol = "BTCUSDT")
prices: Dict[str, Dict[str, object]] = {"MEXC": {}, "Bybit": {}, "BingX": {}}

# Incremental spread computation:
# _dirty_symbols: symbols that received new WS price data since last snapshot.
# _all_pairs: persisted spread cache — pair_key → pair_dict.
# _snapshot_loop processes only dirty symbols on each tick (O(N_dirty) not O(N_all)).
_dirty_symbols: Set[str] = set()
_all_pairs: Dict[str, dict] = {}

# Bybit WS: last snapshot state per symbol for delta merging
_bybit_snap: Dict[str, dict] = {}

# BingX: separate dicts for price data, funding data, and mark price
_bingx_price: Dict[str, dict] = {}   # norm → {bid, ask, last, vol}
_bingx_fund:  Dict[str, dict] = {}   # norm → {fund_rate, next_ts}
_bingx_mark:  Dict[str, float] = {}  # norm → mark_price (from @markPrice stream)

# BingX raw symbols list — populated during bootstrap in main()
_bingx_raw_symbols: List[str] = []

# Reference to app module — set in main() after import
_a = None  # type: ignore[assignment]

# Timestamp of last MEXC WS message — used to detect stale WS feed
_mexc_ws_last_msg: float = 0.0
# Interval between MEXC REST fallback refreshes (seconds)
MEXC_REST_FALLBACK_INTERVAL = float(os.getenv("MEXC_REST_FALLBACK_SEC", "10.0"))
# If MEXC WS has been silent for this long, trigger a REST refresh
MEXC_WS_STALE_SEC = float(os.getenv("MEXC_WS_STALE_SEC", "15.0"))


# ─── Helpers ─────────────────────────────────────────────────────────────────

# Maximum abs(log10(price)) — prices outside this range are likely wrong field
# picks (e.g. volume picked instead of price).
# Real futures prices: BTC ~$65k (log10≈4.8), SLP ~0.0005 (log10≈-3.3),
# BTT ~8.8e-7 (log10≈-6.06). Volumes for low-price tokens: 8M+ (log10≈6.9).
# Threshold 6.5 accepts [3e-7, 3e6] — rejects 8,361,110 (vol) but keeps BTT.
_PRICE_MAX_LOG10 = 6.5


def _price_ok(p: float) -> bool:
    """Return True iff p is a plausible price (finite, positive, reasonable magnitude)."""
    return math.isfinite(p) and p > 0 and abs(math.log10(p)) <= _PRICE_MAX_LOG10


def _backoff(attempt: int) -> float:
    """Exponential backoff: 1, 2, 4, 8, 16, 32, 60 seconds."""
    return min(MAX_RECONNECT_DELAY, 1.0 * (2 ** min(attempt, 6)))


def _bingx_raw_to_norm(raw: str) -> Optional[str]:
    """Convert BingX raw symbol to normalized form.

    'BTC-USDT' → 'BTCUSDT'   (dash format, most common)
    'BTCUSDT'  → 'BTCUSDT'   (already normalized)
    Other      → None
    """
    if "-" in raw:
        base, quote = raw.split("-", 1)
        if quote.upper() == "USDT":
            return _a.normalize_usdt(base)
        return None
    upper = raw.upper()
    if upper.endswith("USDT"):
        return upper
    return None


# ─── MEXC WS ─────────────────────────────────────────────────────────────────

async def _mexc_ws(session: aiohttp.ClientSession) -> None:
    """MEXC futures WebSocket loop with auto-reconnect.

    Subscribes to ``sub.tickers`` for a bulk push of ALL symbol tickers.
    Each push.tickers message contains bid1, ask1, lastPrice, fundingRate,
    nextSettleTime (ms delta), amount24 for every USDT-margined contract.
    """
    attempt = 0
    while True:
        try:
            async with session.ws_connect(MEXC_WS_URL) as ws:
                attempt = 0
                logger.info("[MEXC WS] Connected — subscribing to all tickers")
                await ws.send_json({"method": "sub.tickers", "param": {}})

                # Ping coroutine keeps connection alive
                async def _ping() -> None:
                    while not ws.closed:
                        await asyncio.sleep(WS_PING_MEXC)
                        if ws.closed:
                            break
                        try:
                            await ws.send_json({"method": "ping"})
                        except Exception:
                            break

                ping_task = asyncio.create_task(_ping())
                try:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            _on_mexc(msg.data)
                        elif msg.type in (aiohttp.WSMsgType.ERROR,
                                          aiohttp.WSMsgType.CLOSE):
                            logger.debug("[MEXC WS] Connection closed")
                            break
                finally:
                    ping_task.cancel()

        except Exception as exc:
            delay = _backoff(attempt)
            logger.warning("[MEXC WS] %s — reconnecting in %.0fs", exc, delay)
            await asyncio.sleep(delay)
            attempt += 1
            continue

        # Clean disconnect (no exception): brief reconnect delay
        await asyncio.sleep(_backoff(attempt))
        attempt += 1


def _on_mexc(raw: str) -> None:
    """Handle a single MEXC WS text message."""
    global _mexc_ws_last_msg
    try:
        d = _loads(raw)
    except Exception:
        return
    channel = d.get("channel", "")
    # Skip ACK and pong messages
    if "pong" in channel or channel.startswith("rs."):
        return
    data = d.get("data")
    if data is None:
        return
    items: list = data if isinstance(data, list) else [data]
    stored = 0
    for it in items:
        if not isinstance(it, dict):
            continue
        symbol = str(it.get("symbol") or "")
        if "_" not in symbol:
            continue
        base, quote = symbol.split("_", 1)
        if quote.upper() != "USDT":
            continue
        bid  = _a.to_float(it.get("bid1"))
        ask  = _a.to_float(it.get("ask1"))
        last = _a.to_float(it.get("lastPrice"))
        # bid1/ask1 are often 0 in MEXC WS push.tickers for low-volume symbols.
        # Fall back to lastPrice (bid=ask=last) which is always present.
        if not (math.isfinite(bid) and bid > 0):
            bid = last
        if not (math.isfinite(ask) and ask > 0):
            ask = last
        if not (math.isfinite(bid) and bid > 0):
            continue
        fund     = _a.to_float(it.get("fundingRate"))
        next_ts  = _a._pick_ts_or_delta(
            it, ["nextFundingTime", "nextSettleTime", "fundingTime"]
        )
        norm      = _a.normalize_usdt(base)
        # _MEXC_INTERVALS populated by _mexc_intervals_refresher background task
        interval_h = _a._MEXC_INTERVALS.get(symbol) or 8
        from models import MarketRow
        prices["MEXC"][norm] = MarketRow(
            exchange="MEXC",
            bid=bid,
            ask=ask,
            last=last,
            vol24_usd=_a.to_float(it.get("amount24")),
            fund_rate=fund,
            fund24_est=_a.funding_24h_estimate(fund, interval_h),
            url=_a.mexc_trade_url(symbol),
            next_funding_ts=next_ts,
            funding_interval_h=interval_h,
        )
        _dirty_symbols.add(norm)
        stored += 1
    if stored > 0:
        _mexc_ws_last_msg = time.monotonic()


# ─── Bybit WS ────────────────────────────────────────────────────────────────

async def _bybit_ws(session: aiohttp.ClientSession) -> None:
    """Bybit v5 linear futures WebSocket loop with auto-reconnect.

    Subscribes to tickers.{symbol} for all USDT-perpetuals in batches of
    MAX_BYBIT_BATCH.  Handles both ``snapshot`` (initial) and ``delta``
    (incremental) messages — delta fields are merged into the last snapshot.
    """
    attempt = 0
    while True:
        try:
            symbols = [s for s in _a._BYBIT_INTERVALS if s.endswith("USDT")]
            if not symbols:
                logger.warning("[Bybit WS] No symbols in _BYBIT_INTERVALS yet — waiting 5s")
                await asyncio.sleep(5)
                continue

            async with session.ws_connect(BYBIT_WS_URL) as ws:
                attempt = 0
                logger.info("[Bybit WS] Connected — subscribing to %d symbols", len(symbols))

                # Subscribe in batches of MAX_BYBIT_BATCH to respect rate limits
                args = [f"tickers.{s}" for s in symbols]
                for i in range(0, len(args), MAX_BYBIT_BATCH):
                    await ws.send_json({"op": "subscribe", "args": args[i:i + MAX_BYBIT_BATCH]})
                    await asyncio.sleep(0.05)

                async def _ping() -> None:
                    while not ws.closed:
                        await asyncio.sleep(WS_PING_BYBIT)
                        if ws.closed:
                            break
                        try:
                            await ws.send_json({"op": "ping"})
                        except Exception:
                            break

                ping_task = asyncio.create_task(_ping())
                try:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            _on_bybit(msg.data)
                        elif msg.type in (aiohttp.WSMsgType.ERROR,
                                          aiohttp.WSMsgType.CLOSE):
                            logger.debug("[Bybit WS] Connection closed")
                            break
                finally:
                    ping_task.cancel()

        except Exception as exc:
            delay = _backoff(attempt)
            logger.warning("[Bybit WS] %s — reconnecting in %.0fs", exc, delay)
            await asyncio.sleep(delay)
            attempt += 1
            continue

        # Clean disconnect (no exception): brief reconnect delay
        await asyncio.sleep(_backoff(attempt))
        attempt += 1


def _on_bybit(raw: str) -> None:
    """Handle a single Bybit v5 WS text message."""
    try:
        d = _loads(raw)
    except Exception:
        return
    topic = d.get("topic", "")
    if not topic.startswith("tickers."):
        return
    symbol = topic[len("tickers."):]
    if not symbol.endswith("USDT"):
        return
    msg_type = d.get("type", "snapshot")
    data = d.get("data")
    if not isinstance(data, dict):
        return
    # Merge delta fields into last snapshot state
    if msg_type == "snapshot":
        _bybit_snap[symbol] = dict(data)
    elif msg_type == "delta":
        _bybit_snap.setdefault(symbol, {}).update(data)
    else:
        return
    it = _bybit_snap.get(symbol, {})
    bid = _a.to_float(it.get("bid1Price") or it.get("bidPrice"))
    ask = _a.to_float(it.get("ask1Price") or it.get("askPrice"))
    if not (math.isfinite(bid) and bid > 0
            and math.isfinite(ask) and ask > 0):
        return
    fund     = _a.to_float(it.get("fundingRate"))
    next_ts  = _a._pick_ts(it, ["nextFundingTime", "nextFundingTimestamp"])
    vol      = _a.to_float(it.get("turnover24h") or it.get("turnover24H"))
    interval_h = _a._BYBIT_INTERVALS.get(symbol, 0) or 8
    from models import MarketRow
    prices["Bybit"][symbol] = MarketRow(
        exchange="Bybit",
        bid=bid,
        ask=ask,
        last=_a.to_float(it.get("lastPrice")),
        vol24_usd=vol,
        fund_rate=fund,
        fund24_est=_a.funding_24h_estimate(fund, interval_h),
        url=_a.bybit_trade_url(symbol),
        next_funding_ts=next_ts,
        funding_interval_h=interval_h,
    )
    _dirty_symbols.add(symbol)


# ─── BingX WS ────────────────────────────────────────────────────────────────

async def _bingx_ws(session: aiohttp.ClientSession) -> None:
    """BingX perpetual futures WebSocket loop with auto-reconnect.

    Subscribes to ``{symbol}@ticker`` (bid/ask/last/vol) and
    ``{symbol}@markPrice`` (fundingRate, nextFundingTime) for each symbol.
    BingX messages are gzip-compressed; both BINARY (gzip) and TEXT frames
    are handled.  MarketRow is assembled when both price and funding data
    are available for a symbol.
    """
    attempt = 0
    while True:
        try:
            raw_syms = list(_bingx_raw_symbols)
            if not raw_syms:
                # Fallback: derive from known MEXC/Bybit symbols
                all_norm = set(prices["MEXC"]) | set(prices["Bybit"])
                raw_syms = [
                    f"{norm[:-4]}-USDT"
                    for norm in all_norm
                    if norm.endswith("USDT")
                ][:MAX_BINGX_SYMS]
            if not raw_syms:
                logger.warning("[BingX WS] No symbols yet — waiting 5s")
                await asyncio.sleep(5)
                continue

            async with session.ws_connect(BINGX_WS_URL) as ws:
                attempt = 0
                logger.info("[BingX WS] Connected — subscribing to %d symbols (2 streams each)",
                            len(raw_syms))
                sid = 1
                for raw in raw_syms:
                    await ws.send_json({
                        "id": str(sid), "reqType": "sub",
                        "dataType": f"{raw}@ticker",
                    })
                    sid += 1
                    await ws.send_json({
                        "id": str(sid), "reqType": "sub",
                        "dataType": f"{raw}@markPrice",
                    })
                    sid += 1
                    # Throttle subscription bursts to avoid rate-limiting
                    if sid % 40 == 0:
                        await asyncio.sleep(0.1)

                async def _ping() -> None:
                    while not ws.closed:
                        await asyncio.sleep(WS_PING_BINGX)
                        if ws.closed:
                            break
                        try:
                            await ws.send_json({"ping": int(time.time() * 1000)})
                        except Exception:
                            break

                ping_task = asyncio.create_task(_ping())
                try:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.BINARY:
                            try:
                                text = gzip.decompress(msg.data).decode("utf-8", errors="replace")
                                _on_bingx(text)
                            except Exception:
                                pass
                        elif msg.type == aiohttp.WSMsgType.TEXT:
                            _on_bingx(msg.data)
                        elif msg.type in (aiohttp.WSMsgType.ERROR,
                                          aiohttp.WSMsgType.CLOSE):
                            logger.debug("[BingX WS] Connection closed")
                            break
                finally:
                    ping_task.cancel()

        except Exception as exc:
            delay = _backoff(attempt)
            logger.warning("[BingX WS] %s — reconnecting in %.0fs", exc, delay)
            await asyncio.sleep(delay)
            attempt += 1
            continue

        # Clean disconnect (no exception): brief reconnect delay
        await asyncio.sleep(_backoff(attempt))
        attempt += 1


def _on_bingx(raw: str) -> None:
    """Handle a single BingX WS message (already decompressed if binary)."""
    try:
        d = _loads(raw)
    except Exception:
        return
    # Pong response
    if "pong" in d:
        return
    data_type = d.get("dataType", "")
    data = d.get("data", {})
    if not isinstance(data, dict) or not data_type or "@" not in data_type:
        return
    raw_sym, stream = data_type.rsplit("@", 1)
    norm = _bingx_raw_to_norm(raw_sym)
    if norm is None:
        return

    if stream == "ticker":
        # BingX @ticker only gives c (last price). Fields b/a may contain volume or
        # be absent — never use them as price.  We set bid=ask=last.
        raw_last = _a.to_float(data.get("c") or data.get("lastPrice") or 0)

        # Determine the price to use: last from ticker, then mark from @markPrice stream
        if _price_ok(raw_last):
            price = raw_last
        else:
            # c=0 or out-of-range — try markPrice fallback
            price = _bingx_mark.get(norm, 0.0)
            if not _price_ok(price):
                # No valid price yet — wait for @markPrice event (cold start, normal)
                logger.debug(
                    "[BingX ticker] sym=%s: c=%.6g not ok, no markPrice yet — skipping",
                    norm, raw_last,
                )
                # Preserve existing vol so markPrice handler can combine later
                vol = _a.to_float(data.get("q") or data.get("quoteVolume") or 0)
                if vol > 0:
                    existing = _bingx_price.get(norm, {})
                    _bingx_price[norm] = {**existing, "vol": vol}
                return
            # Warn only if c was positive but out-of-range (likely wrong field bug)
            if math.isfinite(raw_last) and raw_last > 0:
                logger.warning(
                    "[BingX WS] sym=%s: c=%.6g rejected by _price_ok — using markPrice=%.6g",
                    norm, raw_last, price,
                )

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("[BingX ticker] sym=%s last=%.8g bid=ask=%.8g raw=%s",
                         norm, raw_last, price, data)

        _bingx_price[norm] = {
            "bid":  price,
            "ask":  price,
            "last": raw_last if _price_ok(raw_last) else price,
            "vol":  _a.to_float(data.get("q") or data.get("quoteVolume") or 0),
        }
    elif stream == "markPrice":
        # Extract mark price and store as fallback for when c=0 in ticker
        mark = _a.to_float(data.get("p") or data.get("markPrice") or 0)
        if _price_ok(mark):
            _bingx_mark[norm] = mark
            # Bootstrap price entry if ticker hasn't sent a valid price yet
            existing = _bingx_price.get(norm)
            if existing is None or not _price_ok(existing.get("bid", 0.0)):
                _bingx_price[norm] = {
                    "bid":  mark,
                    "ask":  mark,
                    "last": mark,
                    "vol":  (existing or {}).get("vol", math.nan),
                }
        _bingx_fund[norm] = {
            "fund_rate": _a.to_float(data.get("fundingRate") or data.get("r")),
            # BingX WS uses "T" for next funding timestamp (ms); REST uses "nextFundingTime"
            "next_ts":   _a._pick_ts(data, ["nextFundingTime", "nextSettleTime", "T"]),
        }
    else:
        return  # unknown stream

    # Build MarketRow only when we have both price and funding data
    pd = _bingx_price.get(norm)
    fd = _bingx_fund.get(norm)
    if pd is None:
        return
    bid = pd.get("bid", math.nan)
    ask = pd.get("ask", math.nan)
    if not (math.isfinite(bid) and bid > 0
            and math.isfinite(ask) and ask > 0):
        return
    fund    = fd["fund_rate"] if fd else math.nan
    next_ts = fd["next_ts"]   if fd else math.nan
    interval_h = _a._BINGX_INTERVALS.get(norm, 0) or 8
    from models import MarketRow
    prices["BingX"][norm] = MarketRow(
        exchange="BingX",
        bid=bid,
        ask=ask,
        last=pd.get("last", math.nan),
        vol24_usd=pd.get("vol", math.nan),
        fund_rate=fund if math.isfinite(fund) else math.nan,
        fund24_est=(_a.funding_24h_estimate(fund, interval_h)
                    if math.isfinite(fund) else math.nan),
        url=_a.bingx_trade_url(raw_sym),
        next_funding_ts=next_ts,
        funding_interval_h=interval_h,
    )
    _dirty_symbols.add(norm)


# ─── MEXC REST fallback ───────────────────────────────────────────────────────

async def _mexc_rest_loop(session: aiohttp.ClientSession) -> None:
    """Periodic MEXC REST fallback.

    Runs every MEXC_REST_FALLBACK_INTERVAL seconds.  When MEXC WS has been
    silent for MEXC_WS_STALE_SEC (WS not delivering any tickers), or when
    prices["MEXC"] is empty, fetches data from the REST API and populates
    prices["MEXC"] so the spread table still shows MEXC pairs.

    WS data takes priority: if WS is working, it updates prices["MEXC"]
    independently and the REST data only serves as a warm-start / fallback.
    """
    # Brief startup delay so WS has a chance to connect first
    await asyncio.sleep(8.0)
    while True:
        try:
            ws_stale = (time.monotonic() - _mexc_ws_last_msg) > MEXC_WS_STALE_SEC
            mexc_empty = len(prices["MEXC"]) < 5
            if ws_stale or mexc_empty:
                logger.info(
                    "[MEXC REST] Fetching REST data (ws_stale=%s, mexc_empty=%s)",
                    ws_stale, mexc_empty,
                )
                rest_rows = await _a.load_mexc(session)
                if rest_rows:
                    # Only overwrite symbols not recently updated by WS
                    ws_cutoff = time.monotonic() - MEXC_WS_STALE_SEC
                    if ws_stale or mexc_empty:
                        prices["MEXC"].update(rest_rows)
                        logger.info("[MEXC REST] Updated %d symbols from REST", len(rest_rows))
        except Exception as exc:
            logger.warning("[MEXC REST] %s", exc)
        await asyncio.sleep(MEXC_REST_FALLBACK_INTERVAL)


# ─── Snapshot loop ────────────────────────────────────────────────────────────

async def _snapshot_loop() -> None:
    """Compute cross-exchange spreads every SNAPSHOT_THROTTLE seconds.

    Incremental design:
      - First tick: full scan of all symbols (warm-up, builds _all_pairs cache).
      - Subsequent ticks: ONLY recompute spreads for symbols in _dirty_symbols
        (those that received new WS price data since the last snapshot).
        This means when BTC WS msg arrives, only BTC spread is recomputed —
        not all 600 symbols.  CPU drops from O(N_all) to O(N_dirty) per tick.
      - Event-loop yields every 50 symbols: login/logout never blocks.
      - Results capped to MAX_ROWS (default 300) to limit Redis payload size.

    Only writes to Redis and broadcasts SSE when the data actually changes
    (ETag comparison).  Skips computation when fewer than 2 exchanges have data.
    """
    last_etag = ""
    while True:
        await asyncio.sleep(SNAPSHOT_THROTTLE)
        try:
            t0 = time.monotonic()
            n_mexc  = len(prices["MEXC"])
            n_bybit = len(prices["Bybit"])
            n_bingx = len(prices["BingX"])
            exchanges_with_data = sum(1 for n in (n_mexc, n_bybit, n_bingx) if n > 0)
            if exchanges_with_data < 2:
                continue   # not enough data yet

            min_vol    = float(_a.CFG.get("min_vol",    _a.DEFAULT_MIN_VOL_USD))
            min_spread = float(_a.CFG.get("min_spread", _a.DEFAULT_MIN_SPREAD))

            # First tick: full scan (warm-up). Subsequent ticks: only dirty symbols.
            if not _all_pairs:
                symbols_to_compute = (
                    set(prices["MEXC"]) | set(prices["Bybit"]) | set(prices["BingX"])
                )
                _dirty_symbols.clear()   # discard accumulated pre-first-tick dirty
            else:
                # Atomically snapshot and clear the dirty set
                symbols_to_compute = _dirty_symbols.copy()
                _dirty_symbols.clear()

            for i, symbol in enumerate(symbols_to_compute):
                # Yield to event loop every 50 symbols — login/logout never blocks
                if i > 0 and i % 50 == 0:
                    await asyncio.sleep(0)
                # Remove stale pairs for this symbol before recomputing
                _all_pairs = {k: v for k, v in _all_pairs.items()
                              if not k.startswith(f"{symbol}|")}
                rows = [r for r in (
                    prices["MEXC"].get(symbol),
                    prices["Bybit"].get(symbol),
                    prices["BingX"].get(symbol),
                ) if r is not None]
                if len(rows) < 2:
                    continue
                pairs = _a.best_pairs(rows, min_vol=min_vol)
                for p in pairs:
                    if min_spread > 0 and p["spread"] < min_spread:
                        continue
                    p["symbol"] = symbol
                    p["pair_key"] = f"{symbol}|{p['buy_ex']}|{p['sell_ex']}"
                    _all_pairs[p["pair_key"]] = p

            # Sort all pairs by spread desc, cap to MAX_ROWS
            rows_out = sorted(
                _all_pairs.values(),
                key=lambda r: r.get("spread", 0),
                reverse=True,
            )
            if len(rows_out) > MAX_ROWS:
                rows_out = rows_out[:MAX_ROWS]
                # Trim _all_pairs to cap memory growth (dict comprehension, single pass)
                keep_keys = {r["pair_key"] for r in rows_out}
                _all_pairs = {k: v for k, v in _all_pairs.items() if k in keep_keys}

            cache_meta = {
                "updated_at": time.strftime("%H:%M:%S"),
                "dbg": {
                    "mexc":    n_mexc,
                    "bybit":   n_bybit,
                    "bingx":   n_bingx,
                    "kept":    len(rows_out),
                    "took_ms": int((time.monotonic() - t0) * 1000),
                    "ws_mode": True,
                },
            }
            _a._rebuild_data_cache(rows_out, cache_meta)

            # Update arb:cache_meta so dashboard() page render shows fresh dbg data.
            # (compute_once calls _rcache_set but ws_collector bypasses compute_once.)
            # Awaited directly — _rcache_set catches all exceptions internally.
            await _a._rcache_set(cache_meta)

            # Only push to Redis + SSE when data changed (ETag differs)
            new_etag = _a._DATA_ETAG.get("paid", "")
            if new_etag != last_etag:
                last_etag = new_etag
                await _a._rsnapshot_write()
                # Direct await publish — more reliable than _broadcast_sse() from
                # ws_collector context (_broadcast_sse silently swallows exceptions
                # in create_task, causing no PUBLISH even though SET works fine).
                # ws_collector ALWAYS has _REDIS (exits at startup if Redis missing).
                sse_payload = json.dumps({"t": "upd", "at": cache_meta["updated_at"]})
                try:
                    await _a._REDIS.publish(_a._REDIS_CHANNEL_SSE, sse_payload)
                except Exception as pub_exc:
                    logger.warning("[snapshot] Redis PUBLISH failed: %s", pub_exc)
                logger.debug(
                    "[snapshot] MEXC:%d Bybit:%d BingX:%d pairs:%d dirty:%d took:%dms",
                    n_mexc, n_bybit, n_bingx, len(rows_out),
                    len(symbols_to_compute),
                    int((time.monotonic() - t0) * 1000),
                )

        except Exception as exc:
            logger.error("[snapshot_loop] %s", exc)


# ─── Main ────────────────────────────────────────────────────────────────────

async def main() -> None:
    global _a, _bingx_raw_symbols

    # Lazy import after setting COLLECTOR_ONLY env var
    import app as _app  # noqa: PLC0415
    _a = _app

    # Thread pool sized for this server
    cpu_count = os.cpu_count() or 2
    asyncio.get_running_loop().set_default_executor(
        ThreadPoolExecutor(max_workers=cpu_count * 2)
    )

    # Redis is required for ws_collector (snapshots + SSE pub/sub)
    await _a._redis_connect()
    if _a._REDIS is None:
        logger.error(
            "ws_collector requires Redis. Set REDIS_URL, e.g.:\n"
            "  export REDIS_URL=redis://localhost:6379/0"
        )
        sys.exit(1)
    logger.info("Redis connected — ws_collector starting")

    # Shared HTTP session for REST bootstrapping (not for polling — only one-time)
    connector = aiohttp.TCPConnector(limit=30, ttl_dns_cache=300)
    session = aiohttp.ClientSession(connector=connector)
    _a._HTTP_SESSION = session

    # ── Bootstrap REST (one-time at startup) ─────────────────────────────────
    # 1. Bybit instruments-info → populate _BYBIT_INTERVALS (symbol list for WS)
    logger.info("[Bootstrap] Loading Bybit instruments-info...")
    try:
        inst_data = await _a.fetch_json(
            session, _a.BYBIT_INSTRUMENTS, params={"category": "linear"}
        )
        if isinstance(inst_data, dict):
            inst_items = inst_data.get("result", {}).get("list", [])
            for item in (inst_items if isinstance(inst_items, list) else []):
                if not isinstance(item, dict):
                    continue
                sym = str(item.get("symbol") or "").upper()
                ih = _a._pick_int(
                    item,
                    ["fundingInterval", "fundingIntervalHour", "fundingIntervalHours"],
                    default=0,
                )
                if sym and ih > 0:
                    _a._BYBIT_INTERVALS[sym] = ih
        logger.info("[Bootstrap] Bybit intervals: %d symbols", len(_a._BYBIT_INTERVALS))
    except Exception as exc:
        logger.warning("[Bootstrap] Bybit REST failed: %s", exc)

    # 2. BingX contracts → populate _bingx_raw_symbols + _BINGX_INTERVALS
    logger.info("[Bootstrap] Loading BingX contracts...")
    try:
        contracts_resp = await _a.fetch_json(session, _a.BINGX_CONTRACTS)
        contracts: list = (
            contracts_resp if isinstance(contracts_resp, list)
            else (contracts_resp.get("data") if isinstance(contracts_resp, dict) else [])
            or []
        )
        for c in contracts:
            if not isinstance(c, dict):
                continue
            raw = str(c.get("symbol") or "")
            if not raw:
                continue
            norm = _bingx_raw_to_norm(raw)
            if norm is None:
                continue
            _bingx_raw_symbols.append(raw)
            # Pre-populate _BINGX_INTERVALS from contract metadata
            ih = _a._pick_int(
                c,
                ["fundingIntervalHours", "fundingInterval", "fundingTime", "settleCycle"],
                default=0,
            )
            if ih > 0 and norm not in _a._BINGX_INTERVALS:
                _a._BINGX_INTERVALS[norm] = ih
        _bingx_raw_symbols = _bingx_raw_symbols[:MAX_BINGX_SYMS]
        logger.info("[Bootstrap] BingX symbols: %d", len(_bingx_raw_symbols))
    except Exception as exc:
        logger.warning("[Bootstrap] BingX REST failed: %s", exc)

    # 3. BingX funding rates — REST bootstrap so F Spread (adj) shows immediately
    #    (WS @markPrice stream populates _bingx_fund on-the-fly, but only after
    #     the first markPrice event per symbol arrives, which can take 30+ seconds.
    #     The bulk fundingRate endpoint returns all symbols at once in ~200ms.)
    logger.info("[Bootstrap] Loading BingX funding rates...")
    try:
        fund_resp = await _a.fetch_json(session, _a.BINGX_FUNDING_RATE)
        fund_data: list = (
            fund_resp if isinstance(fund_resp, list)
            else (fund_resp.get("data") if isinstance(fund_resp, dict) else [])
            or []
        )
        bootstrapped = 0
        for item in fund_data:
            if not isinstance(item, dict):
                continue
            raw_sym = str(item.get("symbol") or "")
            norm = _bingx_raw_to_norm(raw_sym)
            if norm is None:
                continue
            fr = _a.to_float(item.get("fundingRate") or item.get("lastFundingRate"))
            nts = _a._pick_ts(item, ["nextFundingTime", "nextSettleTime", "T"])
            _bingx_fund[norm] = {"fund_rate": fr, "next_ts": nts}
            # Also store interval from REST if not already known
            ih = _a._pick_int(
                item,
                ["fundingIntervalHours", "fundingInterval", "settleCycle"],
                default=0,
            )
            if ih > 0 and norm not in _a._BINGX_INTERVALS:
                _a._BINGX_INTERVALS[norm] = ih
            bootstrapped += 1
        logger.info("[Bootstrap] BingX funding: %d symbols pre-loaded", bootstrapped)
    except Exception as exc:
        logger.warning("[Bootstrap] BingX funding REST failed: %s", exc)

    # 4. MEXC intervals: try Redis warm-start first (avoids 200-symbol REST loop)
    asyncio.create_task(_a._mexc_intervals_refresher(), name="mexc-intervals")
    logger.info("[Bootstrap] MEXC intervals refresher started (first run in 25s)")

    # ── Start WebSocket + snapshot tasks ──────────────────────────────────────
    tasks = [
        asyncio.create_task(_mexc_ws(session),         name="mexc-ws"),
        asyncio.create_task(_mexc_rest_loop(session),   name="mexc-rest"),
        asyncio.create_task(_bybit_ws(session),        name="bybit-ws"),
        asyncio.create_task(_bingx_ws(session),        name="bingx-ws"),
        asyncio.create_task(_snapshot_loop(),           name="snapshot"),
    ]
    logger.info("ws_collector tasks started: %s", [t.get_name() for t in tasks])

    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("ws_collector shutting down…")
    finally:
        for t in tasks:
            t.cancel()
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception:
            pass
        if not session.closed:
            await session.close()
        await _a._redis_disconnect()
        logger.info("ws_collector stopped.")


if __name__ == "__main__":
    try:
        import uvloop  # noqa: F401
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        logger.info("Using uvloop event loop")
    except ImportError:
        pass
    asyncio.run(main())
