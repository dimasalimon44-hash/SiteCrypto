"""Shared exchange utilities: type helpers, symbol normalisers, HTTP fetch."""
import math
from typing import Any, Dict, List, Optional

import aiohttp

from app.config import HTTP_TIMEOUT


# ---------------------------------------------------------------------------
# Numeric helpers
# ---------------------------------------------------------------------------

def to_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return math.nan


def is_pos(x: float) -> bool:
    return math.isfinite(x) and x > 0


# ---------------------------------------------------------------------------
# Symbol normalisation
# ---------------------------------------------------------------------------

def normalize_usdt(base: str) -> str:
    b = (base or "").upper()
    if b == "XBT":
        b = "BTC"
    return f"{b}USDT"


def normalize_symbol_key(symbol: str) -> str:
    return (symbol or "").upper().replace("-", "").replace("_", "").replace("/", "")


# ---------------------------------------------------------------------------
# Response parsing helpers
# ---------------------------------------------------------------------------

def _as_list(resp: Any) -> List[dict]:
    if isinstance(resp, dict):
        d = resp.get("data")
        if isinstance(d, list):
            return [x for x in d if isinstance(x, dict)]
        if isinstance(d, dict):
            if isinstance(d.get("list"), list):
                return [x for x in d.get("list") if isinstance(x, dict)]
            return [d]
    if isinstance(resp, list):
        return [x for x in resp if isinstance(x, dict)]
    return []


def _pick_float(d: dict, keys: List[str]) -> float:
    for key in keys:
        value = to_float(d.get(key))
        if math.isfinite(value):
            return value
    return math.nan


def _match_symbol_entry(items: List[dict], variants: List[str]) -> Optional[dict]:
    if not items:
        return None
    wanted = {normalize_symbol_key(v) for v in variants if v}
    if not wanted:
        return items[0]
    for it in items:
        s = str(it.get("symbol") or it.get("s") or "")
        if normalize_symbol_key(s) in wanted:
            return it
    if len(items) == 1:
        return items[0]
    return None


def _pick_ts(d: dict, keys: List[str]) -> float:
    for key in keys:
        raw = d.get(key)
        val = to_float(raw)
        if not math.isfinite(val):
            continue
        if val > 1e12:
            val /= 1000.0
        if val > 1e9:
            return val
    return math.nan


def _pick_ts_or_delta(d: dict, keys: List[str]) -> float:
    """Like _pick_ts but also handles MEXC-style remaining-time deltas.

    MEXC bulk ticker and funding_rate endpoints return ``nextSettleTime``
    as milliseconds *remaining* until settlement (a delta), not an absolute
    Unix timestamp.  Values below 86 400 000 ms (1 day) cannot be a valid
    absolute timestamp in seconds, so they are interpreted as a delta:
        abs_ts_sec = now + delta_ms / 1000
    """
    import time
    ts = _pick_ts(d, keys)
    if math.isfinite(ts):
        return ts
    one_day_ms = 86_400_000.0
    for key in keys:
        val = to_float(d.get(key))
        if math.isfinite(val) and 0 < val < one_day_ms:
            return time.time() + val / 1000.0
    return math.nan


# ---------------------------------------------------------------------------
# Trade URL builders
# ---------------------------------------------------------------------------

def mexc_trade_url(symbol_mexc: str) -> str:
    return f"https://www.mexc.com/futures/{symbol_mexc}"


def bybit_trade_url(symbol_bybit: str) -> str:
    return f"https://www.bybit.com/trade/usdt/{symbol_bybit}"


def bingx_trade_url(symbol_bingx: str) -> str:
    return f"https://bingx.com/en/perpetual/{symbol_bingx}"


# ---------------------------------------------------------------------------
# HTTP fetch
# ---------------------------------------------------------------------------

async def fetch_json(session: aiohttp.ClientSession, url: str, params: Optional[dict] = None) -> Any:
    async with session.get(url, params=params, timeout=HTTP_TIMEOUT) as response:
        return await response.json(content_type=None)
