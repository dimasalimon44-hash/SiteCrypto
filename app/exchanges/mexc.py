"""MEXC perpetual futures data loader and funding-interval background refresher."""
import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional

import aiohttp

from app.config import (
    INTERVAL_FETCH_TIMEOUT,
    MEXC_CONTRACT_DETAIL,
    MEXC_INTERVALS_TTL,
    MEXC_TICKERS,
)
from app.exchanges import (
    fetch_json,
    mexc_trade_url,
    normalize_usdt,
    to_float,
    _pick_ts,
    _pick_ts_or_delta,
)
from app.funding import (
    _norm_interval_h,
    _pick_int,
    funding_24h_estimate,
)
from models import MarketRow

logger = logging.getLogger("arb_dashboard")

# ---------------------------------------------------------------------------
# Per-symbol MEXC caches (module-level mutable state)
# ---------------------------------------------------------------------------

_MEXC_INTERVALS: Dict[str, int] = {}
_MEXC_INTERVALS_AT: float = 0.0
_MEXC_FUND_CACHE: dict = {"ts_ms": 0, "at": 0.0}
_MEXC_SYM_FUND_CACHE: Dict[str, dict] = {}


# ---------------------------------------------------------------------------
# Background interval refresh
# ---------------------------------------------------------------------------

async def _refresh_mexc_intervals(session: aiohttp.ClientSession, symbols: List[str], semaphore: int = 10) -> None:
    """Fetch collectCycle per MEXC symbol from funding_rate/{sym} endpoint.

    Called by _mexc_intervals_refresher() background task — NOT on the critical
    path of load_mexc/compute_once.  semaphore (default 10) controls concurrency;
    caller can pass semaphore=5 for gentler hourly background refresh.
    """
    global _MEXC_INTERVALS, _MEXC_INTERVALS_AT
    sem = asyncio.Semaphore(semaphore)

    async def _one(sym: str) -> None:
        async with sem:
            try:
                async with session.get(
                    f"https://contract.mexc.com/api/v1/contract/funding_rate/{sym}",
                    timeout=aiohttp.ClientTimeout(total=INTERVAL_FETCH_TIMEOUT),
                ) as resp:
                    d = await resp.json(content_type=None)
                if isinstance(d, dict) and d.get("success"):
                    cc = (d.get("data") or {}).get("collectCycle", 0)
                    ih = _norm_interval_h(cc)
                    if ih > 0:
                        _MEXC_INTERVALS[sym] = ih
            except Exception as exc:
                logger.debug("[MEXC] funding_rate/%s failed: %s", sym, exc)

    await asyncio.gather(*[_one(s) for s in symbols])
    _MEXC_INTERVALS_AT = time.time()


async def _mexc_intervals_refresher() -> None:
    """Background task: refresh MEXC per-symbol funding intervals once per hour.

    Strategy (fast startup):
    1. Try MEXC_CONTRACT_DETAIL first (ONE bulk call, has ``fundingInterval`` seconds
       for every symbol) — populates _MEXC_INTERVALS within ~500 ms of startup.
    2. For any symbols NOT covered by detail, fall back to per-symbol funding_rate/{sym}.
    This means after one refresh cycle _MEXC_INTERVALS contains all intervals and
    the first compute_once() already shows correct per-coin funding periods.

    NOTE: Session uses sock_read/sock_connect timeouts (no ``total``).
    A session-level ``total`` timeout kills ALL pending requests after N seconds —
    with 200 per-symbol calls, the per-symbol fallback loop would die after ~40s
    having cached only ~8 symbols.  Per-request timeouts in _refresh_mexc_intervals
    handle individual call limits correctly without a hard session deadline.
    """
    global _MEXC_INTERVALS_AT
    from app.config import MEXC_INTERVALS_TTL
    # Delay startup: wait until after the first compute_once() cycle finishes
    # (fires at t+5s and takes 5-15s on a slow VPS).  Firing immediately caused
    # 200 concurrent HTTP requests to MEXC to compete with the first data cycle
    # and all early login/logout requests, making auth feel frozen for 20-30s.
    await asyncio.sleep(25)

    # Step 0: try to warm _MEXC_INTERVALS from Redis on first run
    import app as _a
    _REDIS_KEY_MEXC_INT = "arb:mexc:intervals"
    if _a._REDIS is not None and not _MEXC_INTERVALS:
        try:
            cached_json = await _a._REDIS.get(_REDIS_KEY_MEXC_INT)
            if cached_json:
                loaded = json.loads(cached_json)
                if isinstance(loaded, dict):
                    _MEXC_INTERVALS.update({k: iv for k, v in loaded.items() if (iv := int(v)) > 0})
                    logger.info("[MEXC] %d intervals loaded from Redis arb:mexc:intervals", len(_MEXC_INTERVALS))
        except Exception as exc:
            logger.debug("[MEXC] Redis interval load failed: %s", exc)

    still_missing_count = 0
    while True:
        try:
            timeout = aiohttp.ClientTimeout(sock_connect=5, sock_read=15)
            _shared = _a._HTTP_SESSION
            _own_session = None
            if _shared is None or _shared.closed:
                _own_session = aiohttp.ClientSession(timeout=timeout)
            bg_session = _shared if _own_session is None else _own_session
            try:
                detail_found = 0
                try:
                    detail_data = await fetch_json(bg_session, MEXC_CONTRACT_DETAIL)
                    for c in (detail_data.get("data") or [] if isinstance(detail_data, dict) else []):
                        sym = str(c.get("symbol") or "")
                        if not sym:
                            continue
                        ih = _pick_int(
                            c,
                            ["settlePeriod", "fundingInterval", "collectCycle",
                             "settleTime", "settleCycle", "fundingRateInterval"],
                            default=0,
                        )
                        if ih > 0:
                            _MEXC_INTERVALS[sym] = ih
                            detail_found += 1
                    if detail_found:
                        _MEXC_INTERVALS_AT = time.time()
                        logger.info("[MEXC] %d intervals from contract/detail", detail_found)
                    else:
                        logger.warning("[MEXC] contract/detail returned 0 interval fields")
                except Exception as exc:
                    logger.warning("[MEXC] contract/detail fetch failed: %s", exc)

                ticker_data = await fetch_json(bg_session, MEXC_TICKERS)
                items = (ticker_data.get("data") if isinstance(ticker_data, dict) else ticker_data) or []
                still_missing_count = 0
                if isinstance(items, list):
                    all_syms = [
                        str(it.get("symbol", ""))
                        for it in items
                        if isinstance(it, dict)
                        and "_" in str(it.get("symbol", ""))
                        and str(it.get("symbol", "")).split("_", 1)[1].upper() == "USDT"
                    ]
                    missing = [s for s in all_syms if s not in _MEXC_INTERVALS]
                    if missing:
                        await _refresh_mexc_intervals(bg_session, missing, semaphore=5)
                        logger.info("[MEXC] per-symbol fallback filled %d missing intervals", len(missing))
                    still_missing_count = sum(1 for s in all_syms if s not in _MEXC_INTERVALS)
                    logger.info("[MEXC] interval refresh done (%d total cached, %d still missing)",
                                len(_MEXC_INTERVALS), still_missing_count)
                    if _a._REDIS is not None and _MEXC_INTERVALS:
                        try:
                            await _a._REDIS.set(_REDIS_KEY_MEXC_INT, json.dumps(_MEXC_INTERVALS), ex=86400)
                            logger.debug("[MEXC] %d intervals saved to Redis arb:mexc:intervals", len(_MEXC_INTERVALS))
                        except Exception as exc:
                            logger.debug("[MEXC] Redis interval save failed: %s", exc)
            finally:
                if _own_session is not None:
                    await _own_session.close()
        except Exception as exc:
            logger.warning("[MEXC] _mexc_intervals_refresher error: %s", exc)
            still_missing_count = 1
        sleep_time = 120 if still_missing_count > 0 else MEXC_INTERVALS_TTL
        if still_missing_count:
            logger.info("[MEXC] %d symbols missing; retrying in %ds", still_missing_count, sleep_time)
        await asyncio.sleep(sleep_time)


# ---------------------------------------------------------------------------
# Data loader
# ---------------------------------------------------------------------------

async def load_mexc(session: aiohttp.ClientSession) -> Dict[str, MarketRow]:
    out: Dict[str, MarketRow] = {}
    try:
        ticker_data = await fetch_json(session, MEXC_TICKERS)
        data = ticker_data
        items = data.get("data") if isinstance(data, dict) else data
        if not isinstance(items, list):
            return out

        for it in items:
            if not isinstance(it, dict):
                continue
            symbol = str(it.get("symbol") or "")
            if "_" not in symbol:
                continue
            base, quote = symbol.split("_", 1)
            if quote.upper() != "USDT":
                continue
            fund = to_float(it.get("fundingRate"))
            interval_h = (
                _MEXC_INTERVALS.get(symbol)
                or _pick_int(
                    it,
                    ["collectCycle", "fundingInterval", "settlePeriod",
                     "fundingRateInterval", "settleInterval", "settleCycle"],
                    default=0,
                )
                or 8
            )
            next_ts = _pick_ts_or_delta(it, ["nextFundingTime", "nextSettleTime"])
            # If nextFundingTime was absent, advance the last fundingTime by one
            # interval so we store the *next* event, not the previous one.
            if not math.isfinite(next_ts):
                last_ts = _pick_ts(it, ["fundingTime"])
                if math.isfinite(last_ts):
                    next_ts = last_ts + interval_h * 3600.0
            out[normalize_usdt(base)] = MarketRow(
                exchange="MEXC",
                bid=to_float(it.get("bid1")),
                ask=to_float(it.get("ask1")),
                last=to_float(it.get("lastPrice")),
                vol24_usd=to_float(it.get("amount24")),
                fund_rate=fund,
                fund24_est=funding_24h_estimate(fund, interval_h),
                url=mexc_trade_url(symbol),
                next_funding_ts=next_ts,
                funding_interval_h=interval_h,
            )
    except Exception as e:
        logger.error("MEXC load error: %s: %s", type(e).__name__, e)
        return {}
    return out
