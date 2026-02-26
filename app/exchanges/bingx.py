"""BingX perpetual futures data loader."""
import asyncio
import logging
import math
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiohttp

from app.config import (
    BINGX_BOOK_TICKER,
    BINGX_CONCURRENCY,
    BINGX_CONTRACTS,
    BINGX_CONTRACTS_TTL,
    BINGX_FUNDING_RATE,
    BINGX_PREMIUM_INDEX,
    BINGX_TICKER_24H,
    MAX_BINGX_SYMBOLS,
)
from app.exchanges import (
    _as_list,
    _match_symbol_entry,
    _pick_float,
    _pick_ts,
    bingx_trade_url,
    fetch_json,
    is_pos,
    normalize_symbol_key,
    normalize_usdt,
    to_float,
)
from app.funding import _infer_bingx_interval_h, _norm_interval_h, _pick_int, funding_24h_estimate
from models import MarketRow

logger = logging.getLogger("arb_dashboard")

# ---------------------------------------------------------------------------
# Module-level caches (mutable, shared across compute cycles)
# ---------------------------------------------------------------------------

_BINGX_INTERVALS: Dict[str, int] = {}
_BINGX_CONTRACTS_CACHE: List[dict] = []
_BINGX_CONTRACTS_AT: float = 0.0


# ---------------------------------------------------------------------------
# Data loader
# ---------------------------------------------------------------------------

async def load_bingx(
    session: aiohttp.ClientSession,
    candidate_norm: List[str],
    on_symbol: Optional[Callable] = None,
) -> Dict[str, MarketRow]:
    global _BINGX_CONTRACTS_CACHE, _BINGX_CONTRACTS_AT
    out: Dict[str, MarketRow] = {}
    try:
        dbg = {"selected": 0, "from_bulk": 0, "from_fallback": 0, "rejected_no_quote": 0}
        if (time.time() - _BINGX_CONTRACTS_AT) > BINGX_CONTRACTS_TTL or not _BINGX_CONTRACTS_CACHE:
            contracts_resp = await fetch_json(session, BINGX_CONTRACTS)
            fetched = _as_list(contracts_resp)
            if fetched:
                _BINGX_CONTRACTS_CACHE = fetched
                _BINGX_CONTRACTS_AT = time.time()
        contracts = _BINGX_CONTRACTS_CACHE

        norm_to_raw: Dict[str, str] = {}
        contract_by_raw: Dict[str, dict] = {}
        for c in contracts:
            raw = str(c.get("symbol") or "")
            if not raw:
                continue
            contract_by_raw[raw] = c
            if "-" in raw:
                base, quote = raw.split("-", 1)
                if quote.upper() == "USDT":
                    norm_to_raw[normalize_usdt(base)] = raw
            else:
                upper_raw = raw.upper()
                if upper_raw.endswith("USDT"):
                    norm_to_raw[upper_raw] = raw

        selected = [s for s in candidate_norm if s in norm_to_raw][:MAX_BINGX_SYMBOLS]
        if len(selected) < 120:
            for s in norm_to_raw:
                if s not in selected:
                    selected.append(s)
                if len(selected) >= MAX_BINGX_SYMBOLS:
                    break

        sem = asyncio.Semaphore(BINGX_CONCURRENCY)
        dbg["selected"] = len(selected)

        for raw_sym, c in contract_by_raw.items():
            ih = _pick_int(c, ["fundingIntervalHours", "fundingInterval", "fundingTime", "settleCycle"], default=0)
            if ih <= 0:
                continue
            if "-" in raw_sym:
                base, quote = raw_sym.split("-", 1)
                if quote.upper() != "USDT":
                    continue
                norm = normalize_usdt(base)
            elif raw_sym.upper().endswith("USDT"):
                norm = raw_sym.upper()
            else:
                continue
            if norm and norm not in _BINGX_INTERVALS:
                _BINGX_INTERVALS[norm] = ih

        bulk_book_resp, bulk_tick_resp, bulk_prem_resp, bulk_fund_resp = await asyncio.gather(
            fetch_json(session, BINGX_BOOK_TICKER),
            fetch_json(session, BINGX_TICKER_24H),
            fetch_json(session, BINGX_PREMIUM_INDEX),
            fetch_json(session, BINGX_FUNDING_RATE),
            return_exceptions=True,
        )
        bulk_book: Dict[str, dict] = {}
        bulk_tick: Dict[str, dict] = {}
        bulk_prem: Dict[str, dict] = {}
        bulk_fund: Dict[str, dict] = {}
        if not isinstance(bulk_book_resp, Exception):
            bulk_book = {normalize_symbol_key(str(x.get("symbol") or "")): x for x in _as_list(bulk_book_resp)}
        if not isinstance(bulk_tick_resp, Exception):
            bulk_tick = {normalize_symbol_key(str(x.get("symbol") or "")): x for x in _as_list(bulk_tick_resp)}
        if not isinstance(bulk_prem_resp, Exception):
            bulk_prem = {normalize_symbol_key(str(x.get("symbol") or "")): x for x in _as_list(bulk_prem_resp)}
        if not isinstance(bulk_fund_resp, Exception):
            bulk_fund = {normalize_symbol_key(str(x.get("symbol") or "")): x for x in _as_list(bulk_fund_resp)}
            for k, fr in bulk_fund.items():
                ih = _norm_interval_h(_pick_float(fr, ["fundingInterval"]))
                if ih > 0 and k not in _BINGX_INTERVALS:
                    _BINGX_INTERVALS[k] = ih

        async def one(norm_sym: str) -> Optional[Tuple[str, MarketRow]]:
            raw = norm_to_raw.get(norm_sym)
            if not raw:
                return None
            contract = contract_by_raw.get(raw, {})

            async def fetch_symbol(url: str) -> dict:
                variants = [raw]
                compact = raw.replace("-", "")
                undersc = raw.replace("-", "_")
                for v in (compact, undersc):
                    if v not in variants:
                        variants.append(v)
                for sym in variants:
                    resp = await fetch_json(session, url, params={"symbol": sym})
                    lst = _as_list(resp)
                    if lst:
                        rec = _match_symbol_entry(lst, variants)
                        if rec:
                            return rec
                return {}

            try:
                raw_key = normalize_symbol_key(raw)
                book = dict(bulk_book.get(raw_key, {}))
                tick = dict(bulk_tick.get(raw_key, {}))
                prem = dict(bulk_prem.get(raw_key, {}))
                fnd = dict(bulk_fund.get(raw_key, {}))

                used_fallback = False
                if not (book and tick):
                    used_fallback = True
                    async with sem:
                        fb, ft, fp, ff = await asyncio.gather(
                            fetch_symbol(BINGX_BOOK_TICKER),
                            fetch_symbol(BINGX_TICKER_24H),
                            fetch_symbol(BINGX_PREMIUM_INDEX),
                            fetch_symbol(BINGX_FUNDING_RATE),
                            return_exceptions=True,
                        )
                    if isinstance(fb, dict) and fb:
                        book = fb
                    if isinstance(ft, dict) and ft:
                        tick = ft
                    if isinstance(fp, dict) and fp:
                        prem = fp
                    if isinstance(ff, dict) and ff:
                        fnd = ff
                        ih = _norm_interval_h(_pick_float(fnd, ["fundingInterval"]))
                        if ih > 0 and norm_sym not in _BINGX_INTERVALS:
                            _BINGX_INTERVALS[norm_sym] = ih

                bid = _pick_float(book, ["bidPrice", "bid", "bestBidPrice", "bestBid"])
                ask = _pick_float(book, ["askPrice", "ask", "bestAskPrice", "bestAsk"])
                if not is_pos(bid):
                    bid = _pick_float(tick, ["bidPrice", "bid", "bestBidPrice", "bestBid"])
                if not is_pos(ask):
                    ask = _pick_float(tick, ["askPrice", "ask", "bestAskPrice", "bestAsk"])
                last = _pick_float(tick, ["lastPrice", "last", "close", "markPrice", "indexPrice"])

                if not is_pos(bid) and is_pos(last):
                    bid = last
                if not is_pos(ask) and is_pos(last):
                    ask = last

                vol_quote = _pick_float(tick, [
                    "quoteVolume", "quoteQty", "turnover", "turnover24h", "turnover24H", "quoteVolume24h", "quoteVolume24H",
                    "amountQuote", "volumeQuote",
                ])
                vol_base = _pick_float(tick, ["volume", "baseVolume", "qty", "baseQty", "amount", "vol", "amountBase", "volumeBase", "volume24h"])
                vol = vol_quote
                if not is_pos(vol):
                    price = last if is_pos(last) else (bid + ask) / 2 if is_pos(bid) and is_pos(ask) else math.nan
                    if is_pos(vol_base) and is_pos(price):
                        vol = vol_base * price
                if not is_pos(vol):
                    vol = _pick_float(contract, ["quoteVolume", "quoteVolume24h", "turnover", "turnover24h", "amount24", "volumeQuote"])

                fund = _pick_float(fnd, ["fundingRate", "lastFundingRate"])
                if not math.isfinite(fund):
                    fund = _pick_float(prem, ["fundingRate", "lastFundingRate", "funding"])
                if not math.isfinite(fund):
                    fund = _pick_float(tick, ["fundingRate", "lastFundingRate", "funding"])
                next_ts = _pick_ts(fnd, ["nextFundingTime", "nextFundingTimestamp", "fundingTime"])
                if not math.isfinite(next_ts):
                    next_ts = _pick_ts(prem, ["nextFundingTime", "nextFundingTimestamp", "nextSettleTime"])
                if not math.isfinite(next_ts):
                    next_ts = _pick_ts(contract, ["nextFundingTime", "nextFundingTimestamp", "nextSettleTime"])

                if not (is_pos(bid) and is_pos(ask)):
                    dbg["rejected_no_quote"] += 1
                    return None

                if (math.isfinite(bid) and bid > 0 and abs(math.log10(bid)) > 6.5) or \
                   (math.isfinite(ask) and ask > 0 and abs(math.log10(ask)) > 6.5):
                    logger.debug("[BingX REST] Suspicious bid=%.6g ask=%.6g for %s — discarding",
                                 bid, ask, norm_sym)
                    return None

                if used_fallback:
                    dbg["from_fallback"] += 1
                else:
                    dbg["from_bulk"] += 1

                fnd_interval_h = _norm_interval_h(_pick_float(fnd, ["fundingInterval"]))
                bingx_interval_h = (
                    fnd_interval_h
                    or _BINGX_INTERVALS.get(norm_sym, 0)
                    or _pick_int(contract, ["fundingIntervalHours", "fundingInterval", "fundingTime", "settleCycle"], default=0)
                )
                if not bingx_interval_h:
                    bingx_interval_h = _infer_bingx_interval_h(next_ts)
                if bingx_interval_h and norm_sym not in _BINGX_INTERVALS:
                    _BINGX_INTERVALS[norm_sym] = bingx_interval_h

                market_row = MarketRow(
                    exchange="BingX",
                    bid=bid,
                    ask=ask,
                    last=last,
                    vol24_usd=vol,
                    fund_rate=fund,
                    fund24_est=funding_24h_estimate(fund, bingx_interval_h),
                    url=bingx_trade_url(raw),
                    next_funding_ts=next_ts,
                    funding_interval_h=bingx_interval_h,
                )
                if on_symbol is not None:
                    try:
                        await on_symbol(norm_sym, market_row)
                    except Exception:
                        pass
                return norm_sym, market_row
            except Exception:
                return None

        res = await asyncio.gather(*[one(s) for s in selected], return_exceptions=True)
        for item in res:
            if isinstance(item, tuple):
                out[item[0]] = item[1]
        logger.info(
            "[BingX] selected=%d ok=%d bulk=%d fallback=%d rejected=%d",
            dbg["selected"], len(out), dbg["from_bulk"], dbg["from_fallback"], dbg["rejected_no_quote"]
        )
        return out
    except Exception as e:
        logger.error("BingX load error: %s: %s", type(e).__name__, e)
        return {}
