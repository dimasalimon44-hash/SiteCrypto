"""Bybit perpetual futures data loader."""
import asyncio
import logging
import time
from typing import Dict

import aiohttp

from app.config import BYBIT_INST_TTL, BYBIT_INSTRUMENTS, BYBIT_TICKERS
from app.exchanges import bybit_trade_url, fetch_json, to_float, _pick_ts
from app.funding import _pick_int, funding_24h_estimate
from models import MarketRow

logger = logging.getLogger("arb_dashboard")

# ---------------------------------------------------------------------------
# Module-level caches (mutable, shared across compute cycles)
# ---------------------------------------------------------------------------

_BYBIT_INTERVALS: Dict[str, int] = {}
_BYBIT_INST_AT: float = 0.0


# ---------------------------------------------------------------------------
# Data loader
# ---------------------------------------------------------------------------

async def load_bybit(session: aiohttp.ClientSession) -> Dict[str, MarketRow]:
    global _BYBIT_INST_AT
    out: Dict[str, MarketRow] = {}
    try:
        need_inst = (time.time() - _BYBIT_INST_AT) > BYBIT_INST_TTL
        ticker_fut = fetch_json(session, BYBIT_TICKERS, params={"category": "linear"})
        if need_inst:
            inst_fut = fetch_json(session, BYBIT_INSTRUMENTS, params={"category": "linear"})
            ticker_data, inst_data = await asyncio.gather(ticker_fut, inst_fut, return_exceptions=True)
        else:
            ticker_data = await ticker_fut
            inst_data = None

        # Build symbol → interval_h from instruments-info (only when freshly fetched).
        # fundingInterval is in MINUTES (e.g. 480 = 8h, 240 = 4h, 60 = 1h).
        # _pick_int while-loop divides by 60 while val > 24 and divisible by 60:
        # 480 → 8h, 240 → 4h, 60 → 1h.
        if isinstance(inst_data, Exception):
            logger.warning("Bybit instruments-info fetch failed (intervals defaulting to 8h): %s", inst_data)
        elif isinstance(inst_data, dict):
            inst_items = inst_data.get("result", {}).get("list", [])
            if isinstance(inst_items, list):
                for d in inst_items:
                    if not isinstance(d, dict):
                        continue
                    isym = str(d.get("symbol") or "").upper()
                    ih = _pick_int(d, ["fundingInterval", "fundingIntervalHour", "fundingIntervalHours"], default=0)
                    if isym and ih > 0:
                        _BYBIT_INTERVALS[isym] = ih
            _BYBIT_INST_AT = time.time()

        if isinstance(ticker_data, Exception):
            raise ticker_data
        items = ticker_data.get("result", {}).get("list", []) if isinstance(ticker_data, dict) else []
        if not isinstance(items, list):
            return out

        for it in items:
            if not isinstance(it, dict):
                continue
            symbol = str(it.get("symbol") or "").upper()
            if not symbol.endswith("USDT"):
                continue
            fund = to_float(it.get("fundingRate"))
            next_ts = _pick_ts(it, ["nextFundingTime", "nextFundingTimestamp"])
            interval_h = _BYBIT_INTERVALS.get(symbol, 0) or 8
            out[symbol] = MarketRow(
                exchange="Bybit",
                bid=to_float(it.get("bid1Price") or it.get("bidPrice")),
                ask=to_float(it.get("ask1Price") or it.get("askPrice")),
                last=to_float(it.get("lastPrice")),
                vol24_usd=to_float(it.get("turnover24h") or it.get("turnover24H") or it.get("volume24h")),
                fund_rate=fund,
                fund24_est=funding_24h_estimate(fund, interval_h),
                url=bybit_trade_url(symbol),
                next_funding_ts=next_ts,
                funding_interval_h=interval_h,
            )
    except Exception as e:
        logger.error("Bybit load error: %s: %s", type(e).__name__, e)
        return {}
    return out
