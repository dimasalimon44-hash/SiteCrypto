"""Funding-related calculations: intervals, ETAs, spread math, best_pairs."""
import logging
import math
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from models import MarketRow

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Interval helpers
# ---------------------------------------------------------------------------

def _norm_interval_h(raw_val: Any) -> int:
    """Normalise a raw funding interval value to hours (int).

    Handles milliseconds (>=3_600_000), seconds (3600..86400), minutes (60..1440),
    and direct hours (1..24).  Returns 0 for invalid/zero values.
    """
    val: float
    try:
        val = float(raw_val)
    except (TypeError, ValueError):
        return 0
    if not (math.isfinite(val) and val > 0):
        return 0
    if val >= 3_600_000:          # ms → seconds
        val = val / 1000.0
    ival = int(round(val))
    while ival > 24 and ival % 60 == 0:   # seconds or minutes → hours
        ival = ival // 60
    return max(1, ival)


def _pick_int(d: dict, keys: List[str], default: int = 8) -> int:
    for key in keys:
        raw = d.get(key)
        if raw is None:
            continue
        if isinstance(raw, str):
            txt = raw.strip().lower().replace("hours", "h").replace("hour", "h")
            if txt.endswith("h"):
                txt = txt[:-1]
            raw = txt
        ih = _norm_interval_h(raw)
        if ih > 0:
            return ih
    return default


def _infer_bingx_interval_h(next_ts: float) -> int:
    """Infer BingX funding interval from nextFundingTime UTC alignment.

    BingX schedules funding payments at fixed UTC boundaries:
      - 8h cycle: 00:00, 08:00, 16:00 UTC  (ts_sec divisible by 8*3600 = 28800)
      - 4h cycle: +04:00, +12:00, +20:00 UTC (ts_sec divisible by 4*3600 = 14400)
      - 1h cycle: every hour               (ts_sec divisible by 3600)

    Check from LARGEST to SMALLEST so that 00:00/08:00/16:00 (which divide by
    all three) are correctly identified as 8h, not 4h or 1h.

    No extra API call needed — nextFundingTime is already in the premiumIndex
    response.  Returns 8 when next_ts is unavailable (safe default for BingX).
    """
    if not (math.isfinite(next_ts) and next_ts > 0):
        return 8
    ts_sec = int(next_ts)  # already in seconds (after _pick_ts conversion)
    for h in (8, 4, 1):   # largest first so 8h boundaries aren't misclassified as 4h/1h
        if ts_sec % (h * 3600) == 0:
            return h
    return 8


# ---------------------------------------------------------------------------
# Funding time helpers
# ---------------------------------------------------------------------------

def funding_24h_estimate(rate: float, interval_h: int = 8) -> float:
    return rate * (24.0 / interval_h) if math.isfinite(rate) else math.nan


def funding_eta_str(next_ts: float, fallback_hours: int = 8) -> str:
    now = datetime.now(timezone.utc)
    if math.isfinite(next_ts) and next_ts > time.time():
        target = datetime.fromtimestamp(next_ts, tz=timezone.utc)
    else:
        base = now.replace(minute=0, second=0, microsecond=0)
        nxt = ((base.hour // fallback_hours) + 1) * fallback_hours
        day = 0
        if nxt >= 24:
            nxt -= 24
            day = 1
        target = (base + timedelta(days=day)).replace(hour=nxt)
    sec = max(0, int((target - now).total_seconds()))
    return f"{sec//3600:02d}:{(sec%3600)//60:02d}:{sec%60:02d}"


# ---------------------------------------------------------------------------
# Spread / pairs math
# ---------------------------------------------------------------------------

_DEFAULT_INTERVAL_H = 8  # safe default when interval is unknown


def _normalize_to_hourly(rate: float, interval_h: int) -> float:
    """Return the per-hour equivalent of *rate* for a given funding *interval_h*.

    Different exchanges settle funding at different cadences (1h, 4h, 8h …).
    To compare two rates fairly, both must be expressed on the same time unit
    before computing a spread.

    Unit safety guard: if ``abs(rate) > 1`` the value is almost certainly
    expressed as a percentage in whole-number form (e.g. 150 meaning 150%
    rather than 1.5 as a decimal fraction).  Divide by 100 to normalise.

    Returns 0.0 for invalid inputs instead of raising.
    """
    if not math.isfinite(rate):
        return 0.0
    iv = interval_h
    if not iv or iv <= 0:
        logger.warning("[funding] missing/zero interval — defaulting to %dh", _DEFAULT_INTERVAL_H)
        iv = _DEFAULT_INTERVAL_H
    # Unit safety: values with abs > 1 are almost certainly in percent form
    if abs(rate) > 1:
        rate = rate / 100.0
    return rate / iv


def _adjusted_fund(rate: float, next_ts: float, interval_h: int) -> float:
    """Return funding rate scaled by the fraction of the current period remaining.

    Adjusted = rate × (time_left / interval)
    This gives the expected funding payment until the nearest settlement.
    If next_ts is unknown, returns the full rate (worst-case assumption).
    """
    if not math.isfinite(rate):
        return math.nan
    if math.isfinite(next_ts) and next_ts > time.time():
        time_left_h = (next_ts - time.time()) / 3600.0
        ratio = min(1.0, max(0.0, time_left_h / interval_h)) if interval_h > 0 else 1.0
    else:
        ratio = 1.0  # unknown next_ts → assume full period remaining
    return rate * ratio


def _safe_float(v: Any) -> Optional[float]:
    """Return v as float, or None (JSON null) if not finite or not a number.

    Starlette's JSONResponse uses allow_nan=False, so math.nan / inf in
    a response body causes a 500 error.  Wrap all exchange-sourced floats
    that may be nan/None with this helper before putting them in a row dict.
    """
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def exec_spread(buy: MarketRow, sell: MarketRow) -> float:
    from app.exchanges import is_pos
    if not (is_pos(buy.ask) and is_pos(sell.bid)):
        return math.nan
    return (sell.bid - buy.ask) / buy.ask


def best_pairs(rows: List[MarketRow], min_vol: float) -> List[Dict[str, Any]]:
    from app.exchanges import is_pos

    def _vol_ok(row: MarketRow) -> bool:
        if row.exchange == "BingX":
            return True
        if math.isfinite(row.vol24_usd):
            return row.vol24_usd >= min_vol
        return False

    valid = [r for r in rows if is_pos(r.ask) and is_pos(r.bid) and _vol_ok(r)]
    if len(valid) < 2:
        return []

    out: List[Dict[str, Any]] = []
    for buy in valid:
        for sell in valid:
            if buy.exchange == sell.exchange:
                continue
            spread = exec_spread(buy, sell)
            if not math.isfinite(spread):
                continue
            adj_buy = _adjusted_fund(buy.fund_rate, buy.next_funding_ts, buy.funding_interval_h)
            adj_sell = _adjusted_fund(sell.fund_rate, sell.next_funding_ts, sell.funding_interval_h)
            # Normalize both rates to per-hour before computing the spread so
            # that exchanges with different funding intervals (1h, 4h, 8h …)
            # are compared on the same time-unit basis.
            hourly_buy = _normalize_to_hourly(buy.fund_rate, buy.funding_interval_h)
            hourly_sell = _normalize_to_hourly(sell.fund_rate, sell.funding_interval_h)
            fund_spread = hourly_sell - hourly_buy
            out.append({
                "spread": spread,
                "pair_key": "",
                "buy_ex": buy.exchange,
                "sell_ex": sell.exchange,
                "buy_ask": buy.ask,
                "sell_bid": sell.bid,
                "buy_funding": _safe_float(buy.fund_rate),
                "sell_funding": _safe_float(sell.fund_rate),
                "buy_funding_adjusted": _safe_float(adj_buy),
                "sell_funding_adjusted": _safe_float(adj_sell),
                "funding_spread": _safe_float(fund_spread),
                "funding_eta_buy": funding_eta_str(buy.next_funding_ts, fallback_hours=buy.funding_interval_h),
                "funding_eta_sell": funding_eta_str(sell.next_funding_ts, fallback_hours=sell.funding_interval_h),
                "buy_next_ts_ms": int(buy.next_funding_ts * 1000) if math.isfinite(buy.next_funding_ts) else 0,
                "sell_next_ts_ms": int(sell.next_funding_ts * 1000) if math.isfinite(sell.next_funding_ts) else 0,
                "buy_funding_interval": f"{buy.funding_interval_h}h",
                "sell_funding_interval": f"{sell.funding_interval_h}h",
                "buy_vol": _safe_float(buy.vol24_usd),
                "sell_vol": _safe_float(sell.vol24_usd),
                "buy_url": buy.url,
                "sell_url": sell.url,
            })
    return out


def _spread_sort_key(r: dict) -> float:
    """Safe sort key for rows — converts 'spread' to float, returns 0.0 on error."""
    try:
        return float(r.get("spread") or 0.0)
    except (TypeError, ValueError):
        return 0.0
