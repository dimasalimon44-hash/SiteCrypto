import asyncio
import base64
import json
import hashlib
import logging
import math
import os
import secrets
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

# --- Logging setup: stdout always; optional rotating file via LOG_FILE env var ---
_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
_log_fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(name)s: %(message)s", datefmt="%H:%M:%S")
_root = logging.getLogger()
_root.setLevel(getattr(logging, _LOG_LEVEL, logging.INFO))
_stdout_h = logging.StreamHandler(sys.stdout)
_stdout_h.setFormatter(_log_fmt)
_root.addHandler(_stdout_h)
_log_file = os.getenv("LOG_FILE", "")
if _log_file:
    from logging.handlers import RotatingFileHandler as _RFH
    _file_h = _RFH(_log_file, maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8")
    _file_h.setFormatter(_log_fmt)
    _root.addHandler(_file_h)
# Suppress duplicate log lines from uvicorn — it has its own handlers;
# without this, each log line appears twice (once via root, once via uvicorn handler).
for _uv_log in ("uvicorn", "uvicorn.error", "uvicorn.access"):
    logging.getLogger(_uv_log).propagate = False
logger = logging.getLogger("arb_dashboard")
logger.propagate = False

import aiohttp
import httpx
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.gzip import GZipMiddleware
from starlette.datastructures import MutableHeaders
import uvicorn

from models import MarketRow

try:
    import redis.asyncio as aioredis  # type: ignore[import]
    _REDIS_AVAILABLE = True
except ImportError:
    aioredis = None  # type: ignore[assignment]
    _REDIS_AVAILABLE = False

if getattr(sys, "frozen", False):
    if sys.stdout is None:
        sys.stdout = open(os.devnull, "w")
    if sys.stderr is None:
        sys.stderr = open(os.devnull, "w")


def app_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


BASE_DIR = app_dir()
ASSETS_DIR = os.path.join(BASE_DIR, "assets")
STATIC_DIR = os.path.join(BASE_DIR, "static")
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
LOGOS_DIR = os.path.join(ASSETS_DIR, "logos")
SOUNDS_DIR = os.path.join(ASSETS_DIR, "sounds")
CONFIG_PATH = os.path.join(BASE_DIR, "arb_dashboard_config.json")
AUTH_KEY_PATH = os.path.join(BASE_DIR, "auth_secret.key")
USERS_DB_PATH = os.path.join(BASE_DIR, "users.db.enc")
REFRESH_SEC = int(os.getenv("REFRESH_SEC", "3"))       # collector cycle interval (seconds); override via env
CYCLE_WARN_MS = 2000                                    # log warning when compute_once exceeds this
COLLECTOR_ONLY: bool = os.getenv("COLLECTOR_ONLY") == "1"  # True when running as API-only (no exchange fetch)
DEFAULT_MIN_VOL_USD = 5_000_000.0
DEFAULT_MIN_SPREAD = 0.0
HTTP_TIMEOUT = 12
# Shorter timeout used for background interval-refresh fetches (best-effort, not critical path)
INTERVAL_FETCH_TIMEOUT = 5
MAX_BINGX_SYMBOLS = 260
BINGX_CONCURRENCY = 8
DEFAULT_EXCH_ENABLED = {"MEXC": True, "Bybit": True, "BingX": True}
MAX_FREE_SPREAD = 0.02
SESSION_TTL_SEC = 7 * 24 * 3600
# Telegram bot integration — token must be set via TELEGRAM_BOT_TOKEN env var (never in source)
TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_BOT_USERNAME: str = os.getenv("TELEGRAM_BOT_USERNAME", "arbitrageinsights_bot").lstrip("@")
_TG_API = "https://api.telegram.org/bot{token}/{method}"

MEXC_TICKERS = "https://contract.mexc.com/api/v1/contract/ticker"
MEXC_CONTRACT_DETAIL = "https://contract.mexc.com/api/v1/contract/detail"
MEXC_FUNDING_RATE_BTC = "https://contract.mexc.com/api/v1/contract/funding_rate/BTC_USDT"
BYBIT_TICKERS = "https://api.bybit.com/v5/market/tickers"
BYBIT_INSTRUMENTS = "https://api.bybit.com/v5/market/instruments-info"
BINGX_CONTRACTS = "https://open-api.bingx.com/openApi/swap/v2/quote/contracts"
BINGX_BOOK_TICKER = "https://open-api.bingx.com/openApi/swap/v2/quote/bookTicker"
BINGX_TICKER_24H = "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"
BINGX_PREMIUM_INDEX = "https://open-api.bingx.com/openApi/swap/v2/quote/premiumIndex"
# fundingRate endpoint (bulk, no symbol) returns fundingRate + nextFundingTime + fundingInterval
BINGX_FUNDING_RATE = "https://open-api.bingx.com/openApi/swap/v2/quote/fundingRate"
# Timestamps > this value are in milliseconds; divide by 1000 to get seconds
TIMESTAMP_MS_THRESHOLD = 1e12
# How long to reuse a cached MEXC next-funding-time (seconds)
MEXC_FUNDING_CACHE_TTL_SEC = 60
# Cache for MEXC next-funding time fetched directly (avoids per-row API calls)
_MEXC_FUND_CACHE: dict = {"ts_ms": 0, "at": 0.0}
# Per-symbol MEXC funding time cache  key = symbol e.g. "BTC_USDT"
_MEXC_SYM_FUND_CACHE: Dict[str, dict] = {}
# Per-symbol MEXC funding interval cache  key = symbol e.g. "BTC_USDT" → hours
# Populated by _mexc_intervals_refresher() background task (once per hour)
_MEXC_INTERVALS: Dict[str, int] = {}
# Unix timestamp of last full _MEXC_INTERVALS refresh (refetch when > TTL stale)
_MEXC_INTERVALS_AT: float = 0.0
MEXC_INTERVALS_TTL = 21600  # seconds; funding intervals rarely change — refresh every 6h
# Per-symbol Bybit funding interval cache, key = symbol e.g. "BTCUSDT" → hours.
# Populated from /v5/market/instruments-info (TTL-cached — see BYBIT_INST_TTL).
_BYBIT_INTERVALS: Dict[str, int] = {}
# Timestamp of last instruments-info fetch; skip re-fetch when TTL is fresh.
_BYBIT_INST_AT: float = 0.0
BYBIT_INST_TTL = 3600  # instruments-info has 1000+ items, changes ≈ monthly — cache 1h
# Per-symbol BingX funding interval cache, key = norm_sym e.g. "BTCUSDT" → hours.
# Filled from: contracts endpoint (fundingIntervalHours if present) → per-symbol
# premiumIndex (when bulk prem is absent for a symbol) → _infer_bingx_interval_h.
_BINGX_INTERVALS: Dict[str, int] = {}
# Cached BingX contracts list + fetch timestamp (contracts rarely change — cache 1h).
_BINGX_CONTRACTS_CACHE: List[dict] = []
_BINGX_CONTRACTS_AT: float = 0.0
BINGX_CONTRACTS_TTL = 3600


def _get_or_create_auth_key() -> bytes:
    env_key = os.environ.get("ARB_AUTH_KEY")
    if env_key:
        return env_key.encode("utf-8")
    if os.path.exists(AUTH_KEY_PATH):
        with open(AUTH_KEY_PATH, "rb") as fh:
            return fh.read().strip()
    key = Fernet.generate_key()
    with open(AUTH_KEY_PATH, "wb") as fh:
        fh.write(key)
    return key


PBKDF2_ITERS = 100_000  # iterations for new passwords; legacy hashes stored with 250k


def _hash_password(password: str, salt_b64: str, iters: int = PBKDF2_ITERS) -> str:
    salt = base64.b64decode(salt_b64.encode("utf-8"))
    raw = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iters)
    return base64.b64encode(raw).decode("utf-8")


def _make_password_record(password: str) -> Tuple[str, str]:
    salt_b64 = base64.b64encode(secrets.token_bytes(16)).decode("utf-8")
    return salt_b64, _hash_password(password, salt_b64, PBKDF2_ITERS)


def _verify_password(password: str, salt_b64: str, expected_hash: str, iters: int = PBKDF2_ITERS) -> bool:
    return secrets.compare_digest(_hash_password(password, salt_b64, iters), expected_hash)


def _do_login_verify(payload: Dict[str, Any], users_snapshot: Dict[str, Any]) -> Optional[Tuple[str, str, str, bool]]:
    """Run in a thread: RSA decrypt + PBKDF2 verify in ONE call (avoids two thread pool round-trips).
    Returns (username, password, tg, needs_hash_upgrade) or None if invalid credentials."""
    username, password, tg = _extract_auth_credentials(payload)
    user = users_snapshot.get(username)
    if not user:
        return None
    stored_iters = int(user.get("pbkdf2_iters", 250_000))  # legacy hashes used 250k
    if not _verify_password(password, user.get("salt", ""), user.get("password_hash", ""), stored_iters):
        return None
    needs_upgrade = stored_iters != PBKDF2_ITERS
    return username, password, tg, needs_upgrade


def _normalize_username(username: str) -> str:
    return "".join(ch for ch in (username or "").strip().lower() if ch.isalnum() or ch in "._-")[:32]


def _normalize_tg_username(raw: str) -> str:
    """Strip @ prefix, lowercase, allow alphanumeric + underscore, max 32 chars."""
    stripped = (raw or "").strip().lstrip("@")
    return "".join(ch for ch in stripped.lower() if ch.isalnum() or ch == "_")[:32]


async def _tg_send(chat_id: int | str, text: str) -> bool:
    """Send a Telegram message via Bot API. Returns True on success.
    text must be valid Telegram HTML (use _tg_escape() for user-supplied strings)."""
    if not TELEGRAM_BOT_TOKEN:
        logger.debug("TELEGRAM_BOT_TOKEN not set — skipping tg_send")
        return False
    url = _TG_API.format(token=TELEGRAM_BOT_TOKEN, method="sendMessage")
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"})
            if resp.status_code != 200:
                logger.warning("tg_send failed: status=%s body=%s", resp.status_code, resp.text[:200])
                return False
        return True
    except Exception as exc:
        logger.warning("tg_send exception: %s", exc)
        return False


def _tg_escape(s: str) -> str:
    """Escape user-supplied text for Telegram HTML parse_mode to prevent injection."""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


async def _tg_resolve_chat_id(tg_username: str) -> Optional[int]:
    """Ask Telegram to resolve @username → chat_id. Returns None on failure."""
    if not TELEGRAM_BOT_TOKEN or not tg_username:
        return None
    url = _TG_API.format(token=TELEGRAM_BOT_TOKEN, method="getChat")
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.post(url, json={"chat_id": f"@{tg_username}"})
            data = resp.json()
            if data.get("ok"):
                return int(data["result"]["id"])
    except Exception as exc:
        logger.debug("tg_resolve_chat_id(%s) failed: %s", tg_username, exc)
    return None


def ensure_assets() -> None:
    os.makedirs(LOGOS_DIR, exist_ok=True)
    os.makedirs(SOUNDS_DIR, exist_ok=True)
    os.makedirs(STATIC_DIR, exist_ok=True)

    logos_readme = os.path.join(LOGOS_DIR, "README.txt")
    if not os.path.exists(logos_readme):
        with open(logos_readme, "w", encoding="utf-8") as f:
            f.write(
                "Put exchange logos here by naming:\n"
                "- mexc.png / mexc.svg\n"
                "- bybit.png / bybit.svg\n"
                "- bingx.png / bingx.svg\n"
            )

    sounds_readme = os.path.join(SOUNDS_DIR, "README.txt")
    if not os.path.exists(sounds_readme):
        with open(sounds_readme, "w", encoding="utf-8") as f:
            f.write("Put notification sounds here (wav/mp3/ogg), e.g. sms.wav\n")


def find_logo(exchange: str) -> str:
    base = exchange.lower()
    for ext in (".svg", ".png", ".jpg", ".jpeg", ".webp"):
        p = os.path.join(LOGOS_DIR, base + ext)
        if os.path.exists(p):
            return f"/assets/logos/{base}{ext}"
    return ""


def list_sounds() -> List[str]:
    out: List[str] = []
    if not os.path.isdir(SOUNDS_DIR):
        return out
    for name in sorted(os.listdir(SOUNDS_DIR)):
        if name.lower().endswith((".wav", ".mp3", ".ogg")):
            out.append(name)
    return out


def mexc_trade_url(symbol_mexc: str) -> str:
    return f"https://www.mexc.com/futures/{symbol_mexc}"


def bybit_trade_url(symbol_bybit: str) -> str:
    return f"https://www.bybit.com/trade/usdt/{symbol_bybit}"


def bingx_trade_url(symbol_bingx: str) -> str:
    return f"https://bingx.com/en/perpetual/{symbol_bingx}"


def to_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return math.nan


def is_pos(x: float) -> bool:
    return math.isfinite(x) and x > 0


def funding_24h_estimate(rate: float, interval_h: int = 8) -> float:
    return rate * (24.0 / interval_h) if math.isfinite(rate) else math.nan


def normalize_usdt(base: str) -> str:
    b = (base or "").upper()
    if b == "XBT":
        b = "BTC"
    return f"{b}USDT"


def normalize_symbol_key(symbol: str) -> str:
    return (symbol or "").upper().replace("-", "").replace("_", "").replace("/", "")


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


def _pick_ts_or_delta(d: dict, keys: List[str]) -> float:
    """Like _pick_ts but also handles MEXC-style remaining-time deltas.

    MEXC bulk ticker and funding_rate endpoints return ``nextSettleTime``
    as milliseconds *remaining* until settlement (a delta), not an absolute
    Unix timestamp.  Values below 86 400 000 ms (1 day) cannot be a valid
    absolute timestamp in seconds, so they are interpreted as a delta:
        abs_ts_sec = now + delta_ms / 1000
    """
    ts = _pick_ts(d, keys)
    if math.isfinite(ts):
        return ts
    # Try delta-ms interpretation (MEXC nextSettleTime)
    one_day_ms = 86_400_000.0
    for key in keys:
        val = to_float(d.get(key))
        if math.isfinite(val) and 0 < val < one_day_ms:
            return time.time() + val / 1000.0
    return math.nan


def _norm_interval_h(raw_val) -> int:
    """Normalise a raw funding interval value to hours (int).

    Handles milliseconds (>=3_600_000), seconds (3600..86400), minutes (60..1440),
    and direct hours (1..24).  Returns 0 for invalid/zero values.
    """
    val = to_float(raw_val)
    if not (math.isfinite(val) and val > 0):
        return 0
    if val >= 3_600_000:          # ms  → seconds
        val = val / 1000.0
    # Round to nearest integer before modulo to avoid float precision issues
    # (e.g. 28800.0000001 % 60 is not exactly 0.0 in some float representations)
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


async def fetch_json(session: aiohttp.ClientSession, url: str, params: Optional[dict] = None) -> Any:
    async with session.get(url, params=params, timeout=HTTP_TIMEOUT) as response:
        return await response.json(content_type=None)


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
    # Delay startup: wait until after the first compute_once() cycle finishes
    # (fires at t+5s and takes 5-15s on a slow VPS).  Firing immediately caused
    # 200 concurrent HTTP requests to MEXC to compete with the first data cycle
    # and all early login/logout requests, making auth feel frozen for 20-30s.
    await asyncio.sleep(25)

    # Step 0: try to warm _MEXC_INTERVALS from Redis on first run
    # (survives server restarts — TTL 24h means intervals are always available immediately)
    if _REDIS is not None and not _MEXC_INTERVALS:
        try:
            cached_json = await _REDIS.get(_REDIS_KEY_MEXC_INT)
            if cached_json:
                loaded = json.loads(cached_json)
                if isinstance(loaded, dict):
                    _MEXC_INTERVALS.update({k: iv for k, v in loaded.items() if (iv := int(v)) > 0})
                    logger.info("[MEXC] %d intervals loaded from Redis arb:mexc:intervals", len(_MEXC_INTERVALS))
        except Exception as exc:
            logger.debug("[MEXC] Redis interval load failed: %s", exc)

    while True:
        try:
            # Reuse shared persistent session when available (avoids competing
            # TCP connection pool at startup); fall back to own session if not ready.
            timeout = aiohttp.ClientTimeout(sock_connect=5, sock_read=15)
            _shared = _HTTP_SESSION
            _own_session = None
            if _shared is None or _shared.closed:
                _own_session = aiohttp.ClientSession(timeout=timeout)
            bg_session = _shared if _own_session is None else _own_session
            try:
                # Step 1: fast bulk init from contract/detail.
                # MEXC contract detail has 'settlePeriod' (hours) for each symbol.
                # Also try all known field name variants for forward-compatibility.
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

                # Step 2: per-symbol fallback for any symbols not found in detail
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
                        # Semaphore=5 (was 10) — gentler during hourly background refresh
                        await _refresh_mexc_intervals(bg_session, missing, semaphore=5)
                        logger.info("[MEXC] per-symbol fallback filled %d missing intervals", len(missing))
                    # Count symbols still missing after per-symbol pass (e.g. timeouts/failures)
                    still_missing_count = sum(1 for s in all_syms if s not in _MEXC_INTERVALS)
                    logger.info("[MEXC] interval refresh done (%d total cached, %d still missing)",
                                len(_MEXC_INTERVALS), still_missing_count)
                    # Persist to Redis (TTL 24h) so next server restart doesn't need to refetch
                    if _REDIS is not None and _MEXC_INTERVALS:
                        try:
                            await _REDIS.set(_REDIS_KEY_MEXC_INT, json.dumps(_MEXC_INTERVALS), ex=86400)
                            logger.debug("[MEXC] %d intervals saved to Redis arb:mexc:intervals", len(_MEXC_INTERVALS))
                        except Exception as exc:
                            logger.debug("[MEXC] Redis interval save failed: %s", exc)
            finally:
                if _own_session is not None:
                    await _own_session.close()
        except Exception as exc:
            logger.warning("[MEXC] _mexc_intervals_refresher error: %s", exc)
            still_missing_count = 1  # treat error as "incomplete" → retry sooner
        # Adaptive retry: if some symbols failed (e.g. rate-limit / timeout), retry in 2
        # minutes so they are filled quickly.  Once all cached, respect the full TTL.
        sleep_time = 120 if still_missing_count > 0 else MEXC_INTERVALS_TTL
        if still_missing_count:
            logger.info("[MEXC] %d symbols missing; retrying in %ds", still_missing_count, sleep_time)
        await asyncio.sleep(sleep_time)


async def load_mexc(session: aiohttp.ClientSession) -> Dict[str, MarketRow]:
    out: Dict[str, MarketRow] = {}
    try:
        # Fetch ticker only on the critical path.
        # Intervals come from _MEXC_INTERVALS (background task: contract/detail + per-symbol funding_rate).
        ticker_data = await fetch_json(session, MEXC_TICKERS)
        data = ticker_data
        items = data.get("data") if isinstance(data, dict) else data
        if not isinstance(items, list):
            return out

        # _MEXC_INTERVALS is populated by the _mexc_intervals_refresher() background task.
        # load_mexc() does NOT fetch intervals inline — that would block compute_once().

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
            # MEXC bulk ticker returns nextSettleTime as a delta in ms (not absolute ts)
            next_ts = _pick_ts_or_delta(it, ["nextFundingTime", "nextSettleTime", "fundingTime"])
            # Priority: _MEXC_INTERVALS (from funding_rate/{sym}, updated hourly)
            # → collectCycle in ticker row (available in some future API update)
            # → hardcoded 8h default
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


async def load_bybit(session: aiohttp.ClientSession) -> Dict[str, MarketRow]:
    global _BYBIT_INST_AT
    out: Dict[str, MarketRow] = {}
    try:
        # Fetch tickers always (live prices/rates).
        # instruments-info (1000+ items, changes ≈ monthly) only re-fetched when TTL expired.
        need_inst = (time.time() - _BYBIT_INST_AT) > BYBIT_INST_TTL
        ticker_fut = fetch_json(session, BYBIT_TICKERS, params={"category": "linear"})
        if need_inst:
            inst_fut = fetch_json(session, BYBIT_INSTRUMENTS, params={"category": "linear"})
            ticker_data, inst_data = await asyncio.gather(ticker_fut, inst_fut, return_exceptions=True)
        else:
            ticker_data = await ticker_fut
            inst_data = None  # use cached _BYBIT_INTERVALS

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
            # Use per-symbol interval from instruments-info; ticker has no interval field.
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


async def load_bingx(session: aiohttp.ClientSession, candidate_norm: List[str], on_symbol=None) -> Dict[str, MarketRow]:
    global _BINGX_CONTRACTS_CACHE, _BINGX_CONTRACTS_AT
    out: Dict[str, MarketRow] = {}
    try:
        dbg = {"selected": 0, "from_bulk": 0, "from_fallback": 0, "rejected_no_quote": 0}
        # Contracts change rarely (new listings ≈ daily at most). Cache for 1 hour.
        if (time.time() - _BINGX_CONTRACTS_AT) > BINGX_CONTRACTS_TTL or not _BINGX_CONTRACTS_CACHE:
            contracts_resp = await fetch_json(session, BINGX_CONTRACTS)
            fetched = _as_list(contracts_resp)
            if fetched:  # only overwrite cache if the fetch succeeded
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

        # Pre-populate _BINGX_INTERVALS from contracts (already fetched, zero extra cost).
        # BINGX_CONTRACTS has 'fundingIntervalHours' for each symbol.
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
            fetch_json(session, BINGX_FUNDING_RATE),  # fundingRate+nextFundingTime+fundingInterval
            return_exceptions=True,
        )
        bulk_book: Dict[str, dict] = {}
        bulk_tick: Dict[str, dict] = {}
        bulk_prem: Dict[str, dict] = {}
        # bulk_fund: key fields fundingRate, nextFundingTime (ms), fundingInterval (seconds)
        bulk_fund: Dict[str, dict] = {}
        if not isinstance(bulk_book_resp, Exception):
            bulk_book = {normalize_symbol_key(str(x.get("symbol") or "")): x for x in _as_list(bulk_book_resp)}
        if not isinstance(bulk_tick_resp, Exception):
            bulk_tick = {normalize_symbol_key(str(x.get("symbol") or "")): x for x in _as_list(bulk_tick_resp)}
        if not isinstance(bulk_prem_resp, Exception):
            bulk_prem = {normalize_symbol_key(str(x.get("symbol") or "")): x for x in _as_list(bulk_prem_resp)}
        if not isinstance(bulk_fund_resp, Exception):
            bulk_fund = {normalize_symbol_key(str(x.get("symbol") or "")): x for x in _as_list(bulk_fund_resp)}
            # Pre-populate _BINGX_INTERVALS from fundingInterval (seconds) in bulk_fund
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
                # bulk_fund: fundingRate + nextFundingTime (ms) + fundingInterval (seconds)
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

                # When bid/ask missing (no book data for low-volume symbols), fall back to lastPrice.
                # Same logic as ws_collector._on_bingx — lastPrice is better than rejecting entirely.
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

                # fundingRate: bulk_fund (most accurate) → prem → tick
                fund = _pick_float(fnd, ["fundingRate", "lastFundingRate"])
                if not math.isfinite(fund):
                    fund = _pick_float(prem, ["fundingRate", "lastFundingRate", "funding"])
                if not math.isfinite(fund):
                    fund = _pick_float(tick, ["fundingRate", "lastFundingRate", "funding"])
                # nextFundingTime: bulk_fund has it reliably; prem bulk does NOT
                next_ts = _pick_ts(fnd, ["nextFundingTime", "nextFundingTimestamp", "fundingTime"])
                if not math.isfinite(next_ts):
                    next_ts = _pick_ts(prem, ["nextFundingTime", "nextFundingTimestamp", "nextSettleTime"])
                if not math.isfinite(next_ts):
                    next_ts = _pick_ts(contract, ["nextFundingTime", "nextFundingTimestamp", "nextSettleTime"])

                if not (is_pos(bid) and is_pos(ask)):
                    dbg["rejected_no_quote"] += 1
                    return None

                # Sanity: reject obviously wrong bid/ask (e.g. volume picked as price).
                # abs(log10(price)) > 6.5 rejects prices above ~$3M or below ~3e-7 —
                # unrealistic for USDT perpetual futures; catches volume-as-price bugs.
                if (math.isfinite(bid) and bid > 0 and abs(math.log10(bid)) > 6.5) or \
                   (math.isfinite(ask) and ask > 0 and abs(math.log10(ask)) > 6.5):
                    logger.debug("[BingX REST] Suspicious bid=%.6g ask=%.6g for %s — discarding",
                                 bid, ask, norm_sym)
                    return None

                if used_fallback:
                    dbg["from_fallback"] += 1
                else:
                    dbg["from_bulk"] += 1

                # Compute interval BEFORE MarketRow so fund24_est uses the correct value.
                # Priority 1: fundingInterval from bulk_fund (seconds, e.g. 28800=8h) — most reliable
                # Priority 2: _BINGX_INTERVALS cache (from contracts pre-population or previous cycle)
                # Priority 3: contract dict (fundingIntervalHours, direct hours value)
                # Priority 4: infer from nextFundingTime UTC alignment (now valid since fnd has it)
                # Priority 5: default 8h (safe fallback for most BingX coins)
                fnd_interval_h = _norm_interval_h(_pick_float(fnd, ["fundingInterval"]))
                bingx_interval_h = (
                    fnd_interval_h
                    or _BINGX_INTERVALS.get(norm_sym, 0)
                    or _pick_int(contract, ["fundingIntervalHours", "fundingInterval", "fundingTime", "settleCycle"], default=0)
                )
                if not bingx_interval_h:
                    bingx_interval_h = _infer_bingx_interval_h(next_ts)
                if bingx_interval_h and norm_sym not in _BINGX_INTERVALS:
                    _BINGX_INTERVALS[norm_sym] = bingx_interval_h  # write-through cache

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


def exec_spread(buy: MarketRow, sell: MarketRow) -> float:
    if not (is_pos(buy.ask) and is_pos(sell.bid)):
        return math.nan
    return (sell.bid - buy.ask) / buy.ask


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


def best_pairs(rows: List[MarketRow], min_vol: float) -> List[Dict[str, Any]]:
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
            fund_spread = adj_sell - adj_buy if math.isfinite(adj_buy) and math.isfinite(adj_sell) else math.nan
            out.append({
                "spread": spread,
                "pair_key": "",
                "buy_ex": buy.exchange,
                "sell_ex": sell.exchange,
                "buy_ask": buy.ask,
                "sell_bid": sell.bid,
                # Use _safe_float for all exchange-sourced floats that may be nan.
                # Starlette JSONResponse uses allow_nan=False → nan causes HTTP 500.
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


async def _push_pairs_to_live_rows(
    mexc: Dict[str, "MarketRow"],
    bybit: Dict[str, "MarketRow"],
    bingx: Dict[str, "MarketRow"],
    min_vol: float,
    min_spread: float,
    symbols: Optional[set] = None,
) -> None:
    """Compute best pairs and write to in-memory LIVE_ROWS immediately.

    Redis sync is done once at the end of compute_once via _rlive_set_batch
    (a single pipeline call) rather than N individual hset calls here.
    Yields to the event loop every 20 symbols so login/logout/SSE requests
    are never blocked for more than ~20ms at a time.
    """
    if symbols is None:
        symbols = set(mexc.keys()) | set(bybit.keys()) | set(bingx.keys())
    for i, symbol in enumerate(symbols):
        if i % 20 == 0:
            await asyncio.sleep(0)  # yield to event loop every 20 symbols
        market_rows = [r for r in (mexc.get(symbol), bybit.get(symbol), bingx.get(symbol)) if r is not None]
        if len(market_rows) < 2:
            continue
        pairs = best_pairs(market_rows, min_vol=min_vol)
        for pair in pairs:
            if min_spread > 0 and pair["spread"] < min_spread:
                continue
            pair["symbol"] = symbol
            key = f"{symbol}|{pair['buy_ex']}|{pair['sell_ex']}"
            pair["pair_key"] = key
            LIVE_ROWS[key] = pair  # in-memory only; Redis batch at end of cycle


def load_config() -> Dict[str, Any]:
    defaults = {
        "refresh_sec": REFRESH_SEC,
        "min_vol": DEFAULT_MIN_VOL_USD,
        "min_spread": DEFAULT_MIN_SPREAD,
        "enabled": dict(DEFAULT_EXCH_ENABLED),
    }
    if not os.path.exists(CONFIG_PATH):
        return defaults
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as fh:
            loaded = json.load(fh)
        if not isinstance(loaded, dict):
            return defaults
        defaults.update(loaded)
        enabled = defaults.get("enabled", {})
        defaults["enabled"] = {
            "MEXC": bool(enabled.get("MEXC", True)),
            "Bybit": bool(enabled.get("Bybit", True)),
            "BingX": bool(enabled.get("BingX", True)),
        }
        return defaults
    except Exception:
        return defaults


def save_config(cfg: Dict[str, Any]) -> None:
    with open(CONFIG_PATH, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, ensure_ascii=False, indent=2)


AUTH_CIPHER = Fernet(_get_or_create_auth_key())
RSA_PRIVATE_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
RSA_PUBLIC_PEM = RSA_PRIVATE_KEY.public_key().public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo,
).decode("utf-8")


def _save_users(users: Dict[str, Any]) -> None:
    raw = json.dumps(users, ensure_ascii=False).encode("utf-8")
    token = AUTH_CIPHER.encrypt(raw)
    with open(USERS_DB_PATH, "wb") as fh:
        fh.write(token)


def _seed_admin(users: Dict[str, Any]) -> None:
    if "admin" not in users:
        salt, pwh = _make_password_record("salimonenkodima")
        users["admin"] = {
            "username": "admin",
            "salt": salt,
            "password_hash": pwh,
            "is_admin": True,
            "subscription_approved": True,
            "created_at": int(time.time()),
        }
    if "adminegor" not in users:
        salt2, pwh2 = _make_password_record("egorkorotkov96!")
        users["adminegor"] = {
            "username": "adminegor",
            "salt": salt2,
            "password_hash": pwh2,
            "is_admin": True,
            "subscription_approved": True,
            "created_at": int(time.time()),
        }


def _load_users() -> Dict[str, Any]:
    users: Dict[str, Any] = {}
    if os.path.exists(USERS_DB_PATH):
        try:
            with open(USERS_DB_PATH, "rb") as fh:
                users = json.loads(AUTH_CIPHER.decrypt(fh.read()).decode("utf-8"))
        except Exception:
            users = {}
    _seed_admin(users)
    _save_users(users)
    return users


def _decrypt_client_field(value: str) -> str:
    if not isinstance(value, str) or not value:
        return ""
    decoded = base64.b64decode(value.encode("utf-8"))
    plain = RSA_PRIVATE_KEY.decrypt(
        decoded,
        padding.OAEP(mgf=padding.MGF1(algorithm=hashes.SHA256()), algorithm=hashes.SHA256(), label=None),
    )
    return plain.decode("utf-8")


def _extract_auth_credentials(payload: Dict[str, Any]) -> Tuple[str, str, str]:
    plain_username = _normalize_username(str(payload.get("username") or ""))
    plain_password = str(payload.get("password") or "")
    tg_username = _normalize_tg_username(str(payload.get("tg_username") or ""))

    dec_username = ""
    dec_password = ""

    enc_u = str(payload.get("username_enc") or "")
    enc_p = str(payload.get("password_enc") or "")
    if enc_u and enc_p:
        try:
            dec_username = _normalize_username(_decrypt_client_field(enc_u))
            dec_password = _decrypt_client_field(enc_p)
        except Exception:
            dec_username = ""
            dec_password = ""

    username = dec_username or plain_username
    password = dec_password or plain_password
    return username, password, tg_username


USERS = _load_users()
USERS_LOCK = asyncio.Lock()
SESSIONS: Dict[str, Dict[str, Any]] = {}
_BOT_CHECK_RL: Dict[str, int] = {}  # IP-based rate limit counters for /api/bot/check-subscription
_TG_LINK_CODES: Dict[str, Dict[str, Any]] = {}  # {token: {username, expires_at}}
# Brute-force login protection: {ip_minute_key: fail_count}
_LOGIN_FAIL: Dict[str, int] = {}
_AUTH_RATE: Dict[str, int] = {}  # register+login rate limit: {ip_minute: count}


def _make_session(username: str) -> str:
    token = secrets.token_urlsafe(32)
    SESSIONS[token] = {"username": username, "expires": time.time() + SESSION_TTL_SEC}
    # Also store in Redis when available (survives server restarts)
    if _REDIS is not None:
        try:
            asyncio.get_running_loop().create_task(
                _REDIS.setex(f"arb:sess:{token}", SESSION_TTL_SEC, username)
            )
        except RuntimeError:
            pass  # no running loop (e.g. called from sync context in tests)
    return token


def _get_client_ip(request: Request) -> str:
    """Extract real client IP, respecting X-Forwarded-For set by nginx."""
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _rl_check(store: Dict[str, int], key: str, limit: int) -> bool:
    """Rate-limit helper: returns True if request is allowed, False if over limit.
    Evicts stale minute-keys automatically to prevent unbounded memory growth.
    """
    store[key] = store.get(key, 0) + 1
    # Evict entries from other minutes
    for k in list(store):
        if k != key:
            store.pop(k, None)
    return store[key] <= limit


class GZipExcludeMiddleware(GZipMiddleware):
    """GZipMiddleware that skips compression for SSE endpoints.

    GZipMiddleware buffers the entire streaming response body to compress it.
    For Server-Sent Events (Content-Type: text/event-stream), this means events
    are never flushed until the client disconnects — the SSE stream appears frozen.
    This subclass bypasses gzip for /events so SSE works correctly.
    All other paths are compressed normally.
    """
    _NO_GZIP: tuple = ("/events",)

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope.get("type") == "http" and scope.get("path", "") in self._NO_GZIP:
            await self.app(scope, receive, send)
        else:
            await super().__call__(scope, receive, send)


class SecurityHeadersMiddleware:
    """Pure ASGI security headers middleware — zero body-buffering overhead.
    BaseHTTPMiddleware buffers the response body twice; this implementation
    intercepts only the http.response.start ASGI message (no body reads)."""

    def __init__(self, app: Any) -> None:
        self._app = app

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self._app(scope, receive, send)
            return

        is_static = scope.get("path", "").startswith("/static/")

        async def send_with_headers(message: Any) -> None:
            if message["type"] == "http.response.start":
                hdrs = MutableHeaders(scope=message)
                hdrs.setdefault("X-Content-Type-Options", "nosniff")
                hdrs.setdefault("X-Frame-Options", "SAMEORIGIN")
                hdrs.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
                hdrs.setdefault("X-XSS-Protection", "1; mode=block")
                if is_static:
                    hdrs["Cache-Control"] = "public, max-age=31536000, immutable"
            await send(message)

        await self._app(scope, receive, send_with_headers)


def _session_user(request: Request) -> Optional[Dict[str, Any]]:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return None
    token = auth[7:]
    rec = SESSIONS.get(token)
    if not rec:
        return None
    if rec["expires"] < time.time():
        SESSIONS.pop(token, None)
        return None
    user = USERS.get(rec["username"])
    if not user:
        return None
    return user


async def _session_user_async(request: Request) -> Optional[Dict[str, Any]]:
    """Async variant: checks in-memory SESSIONS first, then Redis on miss."""
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return None
    token = auth[7:]
    rec = SESSIONS.get(token)
    if rec:
        if rec["expires"] < time.time():
            SESSIONS.pop(token, None)
        else:
            return USERS.get(rec["username"])
    # Fallback: check Redis (handles tokens issued before a server restart)
    if _REDIS is not None:
        try:
            username = await _REDIS.get(f"arb:sess:{token}")
            if username:
                # Restore into in-memory cache so next call is instant
                SESSIONS[token] = {"username": username, "expires": time.time() + SESSION_TTL_SEC}
                return USERS.get(username)
        except Exception:
            pass
    return None


def _is_subscription_active(user: Dict[str, Any]) -> bool:
    """Return True when subscription_approved=True AND not past expiry date (if set)."""
    if not user.get("subscription_approved"):
        return False
    expires_at = user.get("subscription_expires")
    if expires_at is not None:
        try:
            if time.time() > float(expires_at):
                # Lazy expiry: clear flag so future calls are fast
                user["subscription_approved"] = False
                user["subscription_expires"] = None
                return False
        except (TypeError, ValueError):
            pass
    return True


def _limit_rows_for_access(rows: List[dict], user: Optional[Dict[str, Any]]) -> Tuple[List[dict], Optional[float], bool, bool]:
    is_admin = bool(user and user.get("is_admin"))
    is_logged = bool(user)
    is_paid = bool(user and _is_subscription_active(user))
    spread_limit: Optional[float] = None
    if not is_logged:
        spread_limit = MAX_FREE_SPREAD
        rows = [r for r in rows if float(r.get("spread") or 0.0) <= spread_limit]
    return rows, spread_limit, is_admin, is_paid


@asynccontextmanager
async def lifespan(_: FastAPI):
    global _HTTP_SESSION
    # Size thread pool for the server CPU count. Default Python pool is
    # min(32, cpu+4) which can be excessive. 2 vCPU → 4 threads is optimal:
    # enough parallelism for PBKDF2 auth + _save_users without context-switch
    # overhead of many threads competing on 2 cores.
    cpu_count = os.cpu_count() or 2
    asyncio.get_running_loop().set_default_executor(
        ThreadPoolExecutor(max_workers=cpu_count * 2)
    )
    await _redis_connect()
    # Persistent HTTP session — reuses TCP connections across all compute cycles.
    connector = aiohttp.TCPConnector(limit=60, ttl_dns_cache=300)
    _HTTP_SESSION = aiohttp.ClientSession(connector=connector)
    # When COLLECTOR_ONLY=1 (API-only mode): exchange fetching runs in a
    # separate collector.py process. This process only serves HTTP and reads
    # pre-built snapshots from Redis (written by collector).
    if not COLLECTOR_ONLY:
        logger.warning("Running in FULL mode (with updater) — set COLLECTOR_ONLY=1 for production")
        asyncio.create_task(updater_loop())
        asyncio.create_task(_mexc_intervals_refresher())   # non-blocking MEXC interval refresh
    else:
        logger.warning("Running in COLLECTOR_ONLY mode — no updater tasks started (reads from Redis)")
    # BingX intervals are inferred from nextFundingTime alignment in load_bingx() — no background task needed
    # _redis_sse_subscriber only needed in COLLECTOR_ONLY mode: in full mode, _broadcast_sse puts
    # messages directly into _SSE_QUEUES (no Redis round-trip needed; starting it in full mode
    # would cause every SSE client to receive each update TWICE — once direct, once via Redis).
    if _REDIS is not None and COLLECTOR_ONLY:
        asyncio.create_task(_redis_sse_subscriber())
    yield
    await _HTTP_SESSION.close()
    await _redis_disconnect()


ensure_assets()
app = FastAPI(lifespan=lifespan)
# CORS: set ALLOWED_ORIGINS env var to restrict to your domain in production.
# Example: ALLOWED_ORIGINS=https://yourdomain.com
# Multiple: ALLOWED_ORIGINS=https://a.com,https://b.com
# Empty/unset (default): allows all origins (ok for a private/internal server)
_cors_origins = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)
app.add_middleware(GZipExcludeMiddleware, minimum_size=500)  # gzip for all paths except /events
app.add_middleware(SecurityHeadersMiddleware)  # nosniff, no-framing, XSS protection
app.mount("/assets", StaticFiles(directory=ASSETS_DIR), name="assets")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
templates = Jinja2Templates(directory=TEMPLATES_DIR)
_START_TIME = time.time()  # used by /health endpoint
CFG = load_config()
CACHE = {"updated_at": None, "rows": [], "dbg": {"mexc": 0, "bybit": 0, "bingx": 0, "kept": 0, "took_ms": 0}}
CACHE_LOCK = asyncio.Lock()
PAIR_HISTORY: Dict[str, List[Dict[str, Any]]] = {}
PAIR_HISTORY_MAX = 300
LIVE_ROWS: Dict[str, dict] = {}
_SSE_QUEUES: List[asyncio.Queue] = []
# Pre-built /api/data response bodies per access tier (guest / paid / admin).
# Built once at the end of each compute_once() cycle; served as raw bytes in
# api_data() without any Redis I/O, JSON parsing, sorting, or serialisation.
# With N concurrent users, /api/data cost drops from O(N * rows) to O(1).
_DATA_CACHE: Dict[str, bytes] = {}       # keys: "guest", "paid", "admin"
_DATA_ETAG:  Dict[str, str]  = {}       # keys: "guest", "paid", "admin" → short ETag
# Persistent aiohttp session shared across all compute_once() cycles.
# Created in lifespan() and closed on shutdown to reuse TCP connections.
_HTTP_SESSION: Optional[aiohttp.ClientSession] = None

# Cache-busting version tag based on startup time.
# Every server restart (= every deploy) produces a new tag, so browsers
# re-download CSS/JS.  Between restarts they serve from cache for free.
_STATIC_VER = hex(int(_START_TIME))[2:]
# ---------------------------------------------------------------------------
# Optional Redis layer
# Set REDIS_URL env var (e.g. redis://localhost:6379/0) to enable Redis.
# When Redis is available:
#   - LIVE_ROWS are stored in Redis Hash "arb:live"  → survives restarts,
#     shared across multiple uvicorn workers.
#   - PAIR_HISTORY entries stored in Redis Lists "arb:hist:{pair_key}".
#   - CACHE metadata stored as "arb:cache_meta".
#   - SSE broadcast published on "arb:sse" pub/sub channel so all workers
#     can push to their own connected clients.
# When Redis is NOT available: falls back to in-process dicts (current
# behaviour, unchanged).
# ---------------------------------------------------------------------------

_REDIS_KEY_LIVE = "arb:live"
_REDIS_KEY_CACHE_META = "arb:cache_meta"
_REDIS_CHANNEL_SSE = "arb:sse"
_REDIS_KEY_SNAP = "arb:snap"        # arb:snap:{tier} → pre-built JSON bytes
_REDIS_KEY_MEXC_INT = "arb:mexc:intervals"  # MEXC per-symbol funding intervals (TTL 24h)
_REDIS: Optional[Any] = None  # redis.asyncio.Redis instance, or None


async def _redis_connect() -> None:
    global _REDIS
    url = os.environ.get("REDIS_URL", "").strip()
    if not url or not _REDIS_AVAILABLE:
        return
    try:
        client = aioredis.from_url(url, encoding="utf-8", decode_responses=True)
        await client.ping()
        _REDIS = client
        logger.info("[Redis] Connected: %s", url)
    except Exception as exc:
        logger.warning("[Redis] Cannot connect to %r: %s — using in-memory fallback", url, exc)
        _REDIS = None


async def _redis_disconnect() -> None:
    global _REDIS
    if _REDIS is not None:
        try:
            await _REDIS.aclose()
        except Exception:
            pass
        _REDIS = None


# LIVE_ROWS helpers -----------------------------------------------------------

async def _rlive_set(pair_key: str, row: dict) -> None:
    """Write one row to in-memory dict only (use _rlive_set_batch for Redis)."""
    LIVE_ROWS[pair_key] = row


async def _rlive_set_batch(rows: Dict[str, dict]) -> None:
    """Write multiple rows to in-memory LIVE_ROWS and Redis in a single pipeline.

    This replaces calling _rlive_set() in a loop which produced N individual
    Redis round-trips per cycle.  One pipeline call handles any number of rows.
    """
    LIVE_ROWS.update(rows)
    if _REDIS is not None and rows:
        try:
            pipe = _REDIS.pipeline()
            for k, v in rows.items():
                pipe.hset(_REDIS_KEY_LIVE, k, json.dumps(v))
            await pipe.execute()
        except Exception:
            pass


async def _rlive_del(pair_key: str) -> None:
    """Delete one row from Redis hash and in-memory dict."""
    LIVE_ROWS.pop(pair_key, None)
    if _REDIS is not None:
        try:
            await _REDIS.hdel(_REDIS_KEY_LIVE, pair_key)
        except Exception:
            pass


async def _rlive_all() -> Dict[str, dict]:
    """Return all live rows from Redis (or in-memory fallback)."""
    if _REDIS is not None:
        try:
            raw = await _REDIS.hgetall(_REDIS_KEY_LIVE)
            if raw:
                return {k: json.loads(v) for k, v in raw.items()}
        except Exception:
            pass
    return dict(LIVE_ROWS)


# PAIR_HISTORY helpers --------------------------------------------------------

async def _rhist_append(pair_key: str, entry: dict) -> None:
    """Append a history entry; also update in-memory PAIR_HISTORY."""
    h = PAIR_HISTORY.setdefault(pair_key, [])
    h.append(entry)
    if len(h) > PAIR_HISTORY_MAX:
        del h[:-PAIR_HISTORY_MAX]
    if _REDIS is not None:
        rkey = f"arb:hist:{pair_key}"
        try:
            pipe = _REDIS.pipeline()
            pipe.rpush(rkey, json.dumps(entry))
            pipe.ltrim(rkey, -PAIR_HISTORY_MAX, -1)
            await pipe.execute()
        except Exception:
            pass


async def _rhist_get(pair_key: str) -> List[dict]:
    """Fetch history from Redis if available, else in-memory."""
    if _REDIS is not None:
        rkey = f"arb:hist:{pair_key}"
        try:
            raw = await _REDIS.lrange(rkey, 0, -1)
            if raw:
                return [json.loads(x) for x in raw]
        except Exception:
            pass
    return list(PAIR_HISTORY.get(pair_key, []))


# CACHE metadata helpers -------------------------------------------------------

async def _rcache_set(meta: dict) -> None:
    if _REDIS is not None:
        try:
            await _REDIS.set(_REDIS_KEY_CACHE_META, json.dumps(meta))
        except Exception:
            pass


async def _rcache_get() -> dict:
    if _REDIS is not None:
        try:
            raw = await _REDIS.get(_REDIS_KEY_CACHE_META)
            if raw:
                return json.loads(raw)
        except Exception:
            pass
    return {}


# SSE broadcast (Redis pub/sub for multi-worker) ------------------------------

async def _redis_sse_subscriber() -> None:
    """Subscribe to the Redis pub/sub SSE channel and forward messages
    to all in-process SSE clients.  Runs as a background task when Redis
    is available.  If the connection drops it retries after 5 seconds."""
    if _REDIS is None:
        return
    url = os.environ.get("REDIS_URL", "").strip()
    if not url:
        return
    while True:
        try:
            sub_client = aioredis.from_url(url, encoding="utf-8", decode_responses=True)
            pubsub = sub_client.pubsub()
            await pubsub.subscribe(_REDIS_CHANNEL_SSE)
            async for message in pubsub.listen():
                if message and message.get("type") == "message":
                    data = message.get("data", "")
                    # Forward to in-process queues (clients on THIS worker)
                    for q in list(_SSE_QUEUES):
                        try:
                            q.put_nowait(data)
                        except asyncio.QueueFull:
                            pass
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning("[Redis] SSE subscriber error: %s — retrying in 5s", exc)
            await asyncio.sleep(5)


def _broadcast_sse(payload: str) -> None:
    """Push a message to every connected SSE client (fire-and-forget).

    When Redis is configured the message is also published to the
    ``arb:sse`` pub/sub channel so workers that have no local SSE
    subscriber still deliver the update to their clients via
    ``_redis_sse_subscriber``.
    """
    for q in list(_SSE_QUEUES):
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            pass  # slow client – skip this tick
    if _REDIS is not None:
        # Schedule publish on the running event loop without blocking the caller.
        try:
            asyncio.get_running_loop().create_task(
                _REDIS.publish(_REDIS_CHANNEL_SSE, payload)
            )
        except RuntimeError:
            pass  # no running loop (shouldn't happen in normal async context)


def _rebuild_data_cache(rows_out: List[dict], cache_meta: dict) -> None:
    """Pre-build /api/data JSON response bytes for all 3 access tiers.

    Called once at the end of each compute_once() cycle.  api_data() then
    returns the appropriate pre-built bytes directly — zero Redis I/O, zero
    JSON parsing, zero sorting, zero serialisation per user request.

    3 tiers:
      guest — free users and unauthenticated visitors (spread <= MAX_FREE_SPREAD)
      paid  — users with active subscription (all rows)
      admin — admin users (all rows + is_admin=True)
    """
    sorted_rows = sorted(rows_out, key=_spread_sort_key, reverse=True)
    updated_at = cache_meta.get("updated_at", time.strftime("%H:%M:%S"))
    dbg_base = dict(cache_meta.get("dbg", {}))

    for tier in ("guest", "paid", "admin"):
        if tier == "guest":
            spread_limit: Optional[float] = MAX_FREE_SPREAD
            rows: List[dict] = [r for r in sorted_rows if float(r.get("spread") or 0.0) <= spread_limit]
            is_admin_tier = False
            is_paid_tier = False
        elif tier == "paid":
            spread_limit = None
            rows = sorted_rows
            is_admin_tier = False
            is_paid_tier = True
        else:  # admin
            spread_limit = None
            rows = sorted_rows
            is_admin_tier = True
            is_paid_tier = True

        data = {
            "updated_at": updated_at,
            "dbg": {**dbg_base, "kept": len(rows)},
            "rows": rows,
            "access": {
                "username": None,           # frontend uses STATE.user.username instead
                "is_admin": is_admin_tier,
                "subscription_approved": is_paid_tier,
                "spread_limit": spread_limit,
            },
        }
        try:
            raw_bytes = json.dumps(data, ensure_ascii=False, allow_nan=False).encode()
        except (ValueError, TypeError):
            # Fallback: sanitize NaN/inf → null so JSON is always valid.
            # Data structure is at most 3 levels deep (data→rows→field),
            # so recursion depth is bounded. max_depth=10 adds a safety cap.
            def _sanitize(obj: Any, depth: int = 0) -> Any:
                if depth > 10:
                    return None
                if isinstance(obj, float):
                    return None if not math.isfinite(obj) else obj
                if isinstance(obj, dict):
                    return {k: _sanitize(v, depth + 1) for k, v in obj.items()}
                if isinstance(obj, list):
                    return [_sanitize(v, depth + 1) for v in obj]
                return obj
            logger.warning("[cache] NaN detected in %s rows — sanitizing", tier)
            raw_bytes = json.dumps(_sanitize(data), ensure_ascii=False).encode()
        _DATA_CACHE[tier] = raw_bytes
        _DATA_ETAG[tier]  = '"' + hashlib.sha256(_DATA_CACHE[tier]).hexdigest()[:16] + '"'


async def _rsnapshot_write() -> None:
    """Write pre-built snapshot bytes to Redis so other processes (API workers
    without a local updater_loop) can serve /api/data without recomputing.

    Called as a fire-and-forget task from compute_once() after _rebuild_data_cache().
    Each tier key has TTL=120s — API falls back to live HGETALL if collector stops.
    """
    if _REDIS is None:
        return
    try:
        pipe = _REDIS.pipeline(transaction=False)
        for t in ("guest", "paid", "admin"):
            if t in _DATA_CACHE:
                pipe.set(f"{_REDIS_KEY_SNAP}:{t}", _DATA_CACHE[t], ex=120)
                pipe.set(f"{_REDIS_KEY_SNAP}:etag:{t}", _DATA_ETAG.get(t, ""), ex=120)
        await pipe.execute()
    except Exception as exc:
        logger.debug("[Redis] _rsnapshot_write error: %s", exc)


async def compute_once() -> Dict[str, Any]:
    started = time.time()
    # Use the persistent session (created in lifespan) — avoids new TCP connections every cycle.
    # Fall back to a temporary session if somehow called before lifespan (e.g. tests).
    session = _HTTP_SESSION
    _owned = False
    if session is None or session.closed:
        session = aiohttp.ClientSession()
        _owned = True
    try:
        enabled = CFG.get("enabled", DEFAULT_EXCH_ENABLED)
        mexc_task = asyncio.create_task(load_mexc(session)) if enabled.get("MEXC", True) else None
        bybit_task = asyncio.create_task(load_bybit(session)) if enabled.get("Bybit", True) else None
        mexc = await mexc_task if mexc_task else {}
        bybit = await bybit_task if bybit_task else {}

        min_vol = float(CFG.get("min_vol", DEFAULT_MIN_VOL_USD))
        min_spread = float(CFG.get("min_spread", DEFAULT_MIN_SPREAD))

        # Phase 1: immediately push MEXC+Bybit pairs so clients see updates fast.
        # Pre-build _DATA_CACHE from Phase 1 rows BEFORE broadcasting SSE so
        # that when clients call /api/data they get MEXC+Bybit data immediately
        # instead of the loading placeholder (which happened when the broadcast
        # fired before _rebuild_data_cache was called).
        await _push_pairs_to_live_rows(mexc, bybit, {}, min_vol, min_spread)
        _phase1_at = time.strftime("%H:%M:%S")
        _rebuild_data_cache(list(LIVE_ROWS.values()), {
            "updated_at": _phase1_at,
            "dbg": {"mexc": len(mexc), "bybit": len(bybit), "bingx": 0, "kept": len(LIVE_ROWS), "took_ms": 0},
        })
        _broadcast_sse(json.dumps({"t": "upd", "at": _phase1_at}))

        candidates: Dict[str, float] = {}
        for source in (mexc, bybit):
            for symbol, row in source.items():
                vol = row.vol24_usd if math.isfinite(row.vol24_usd) else 0.0
                candidates[symbol] = max(candidates.get(symbol, 0.0), vol)

        sorted_candidates = [x[0] for x in sorted(candidates.items(), key=lambda item: item[1], reverse=True)]

        # Phase 2: BingX – load all symbols concurrently.
        # on_symbol callback removed: it called best_pairs() 260 times (~1300ms CPU)
        # with no SSE broadcast between calls, so clients never saw intermediate updates.
        # LIVE_ROWS is updated once in the final _rlive_set_batch call below.
        bingx = await load_bingx(session, sorted_candidates) if enabled.get("BingX", True) else {}
    finally:
        if _owned:
            await session.close()

    rows_out: List[dict] = []
    all_symbols = set(mexc.keys()) | set(bybit.keys()) | set(bingx.keys())

    for i, symbol in enumerate(all_symbols):
        if i % 20 == 0:
            await asyncio.sleep(0)  # yield to event loop every 20 symbols
        rows = [r for r in (mexc.get(symbol), bybit.get(symbol), bingx.get(symbol)) if r]
        if len(rows) < 2:
            continue
        pairs = best_pairs(rows, min_vol=min_vol)
        if not pairs:
            continue
        for best in pairs:
            if min_spread > 0 and best["spread"] < min_spread:
                continue
            best["symbol"] = symbol
            best["pair_key"] = f"{symbol}|{best['buy_ex']}|{best['sell_ex']}"
            rows_out.append(best)

    rows_out.sort(key=lambda row: row["spread"], reverse=True)
    now_ts = int(time.time())
    for r in rows_out:
        k = r.get("pair_key")
        if not k:
            continue
        entry = {
            "ts": now_ts,
            "spread": float(r.get("spread") or 0.0),
            "buy_price": float(r.get("buy_ask") or math.nan),
            "sell_price": float(r.get("sell_bid") or math.nan),
            "buy_ex": r.get("buy_ex"),
            "sell_ex": r.get("sell_ex"),
            "symbol": r.get("symbol"),
        }
        await _rhist_append(k, entry)

    # Sync LIVE_ROWS: apply final authoritative data and remove stale pairs.
    # Use batch write to Redis (one pipeline call) instead of N individual hset calls.
    final_valid_keys = {r["pair_key"] for r in rows_out}
    stale_keys = [k for k in list(LIVE_ROWS) if k not in final_valid_keys]
    for k in stale_keys:
        await _rlive_del(k)
    await _rlive_set_batch({r["pair_key"]: r for r in rows_out})

    # Persist cache metadata to Redis
    cache_meta = {
        "updated_at": time.strftime("%H:%M:%S"),
        "dbg": {
            "mexc": len(mexc),
            "bybit": len(bybit),
            "bingx": len(bingx),
            "kept": len(rows_out),
            "took_ms": int((time.time() - started) * 1000),
        },
    }
    await _rcache_set(cache_meta)
    # Pre-build /api/data response bytes for all 3 tiers — O(1) serving per user
    _rebuild_data_cache(rows_out, cache_meta)
    asyncio.create_task(_rsnapshot_write())  # write snapshots to Redis for cross-process API workers

    # Cycle metrics — log every cycle; warn if slow (>2000ms)
    took_ms = int((time.time() - started) * 1000)
    logger.info(
        "Cycle: %d ms | MEXC: %d | Bybit: %d | BingX: %d | pairs: %d",
        took_ms, len(mexc), len(bybit), len(bingx), len(rows_out),
    )
    if took_ms > CYCLE_WARN_MS:
        logger.warning("Cycle > %d ms: %d ms — consider increasing REFRESH_SEC", CYCLE_WARN_MS, took_ms)

    return {
        "started_ts": started,
        "updated_at": cache_meta["updated_at"],
        "rows": rows_out,
        "dbg": cache_meta["dbg"],
    }


async def updater_loop():
    while True:
        cycle_started = time.time()
        try:
            data = await compute_once()
            async with CACHE_LOCK:
                CACHE.update(data)
            cycle_started = float(data.get("started_ts", cycle_started)) if isinstance(data, dict) else cycle_started
            _broadcast_sse(json.dumps({"t": "upd", "at": data.get("updated_at", "")}))
        except Exception:
            logger.exception("updater_loop: compute_once raised an error")
        elapsed = max(0.0, time.time() - cycle_started)
        wait_for = max(0.05, float(CFG.get("refresh_sec", REFRESH_SEC)) - elapsed)
        await asyncio.sleep(wait_for)


def _spread_sort_key(r: dict) -> float:
    """Safe sort key for rows — converts 'spread' to float, returns 0.0 on error."""
    try:
        return float(r.get("spread") or 0.0)
    except (TypeError, ValueError):
        return 0.0


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Serve the dashboard with server-injected initial snapshot.

    Uses pre-built guest snapshot from _DATA_CACHE or Redis arb:snap:guest —
    O(1) per page load regardless of number of users or rows.
    Auth token lives in localStorage (not a cookie), so this page request is
    always guest-level; the JS calls /api/me + /api/data in parallel on load
    to upgrade to the authenticated view within one SSE cycle.
    """
    # Use the pre-built guest cache (built once per compute cycle, not per request).
    # This avoids Redis HGETALL + sort + json.dumps on every page load.
    snap_bytes = _DATA_CACHE.get("guest")
    if not snap_bytes and _REDIS is not None:
        try:
            snap_bytes = await _REDIS.get(f"{_REDIS_KEY_SNAP}:guest")
        except Exception as exc:
            logger.debug("[index] Redis arb:snap:guest unavailable: %s", exc)
    if snap_bytes:
        snap_str = snap_bytes.decode() if isinstance(snap_bytes, bytes) else snap_bytes
        initial_data = snap_str
    else:
        # Nothing ready yet (first startup, <5s after launch): serve empty loading state.
        initial_data = json.dumps({
            "updated_at": "", "dbg": {}, "rows": [],
            "access": {"username": None, "is_admin": False,
                       "subscription_approved": False, "spread_limit": MAX_FREE_SPREAD},
        }, ensure_ascii=False)
    initial_config = json.dumps(CFG, ensure_ascii=False)
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "initial_data": initial_data,
        "initial_config": initial_config,
        "sv": _STATIC_VER,
    })


@app.get("/health")
async def health():
    """Health-check endpoint for nginx/systemd/uptime monitors.

    O(1) — never calls Redis HGETALL.  Reports whether the guest snapshot
    cache is populated (updated each compute cycle by ws_collector/collector).
    """
    snap = _DATA_CACHE.get("guest")
    return JSONResponse({
        "ok": True,
        "uptime_s": int(time.time() - _START_TIME),
        "snapshot_ready": snap is not None,
    })


@app.get("/api/config")
async def api_config():
    return JSONResponse(CFG)


@app.post("/api/config")
async def api_config_set(payload: Dict[str, Any]):
    changed = False
    for key, caster in (("min_vol", float), ("min_spread", float), ("refresh_sec", int)):
        if key in payload:
            try:
                val = caster(payload[key])
                if key == "refresh_sec":
                    val = max(1, val)
                CFG[key] = val
                changed = True
            except Exception:
                pass

    if "enabled" in payload and isinstance(payload["enabled"], dict):
        enabled = {**CFG.get("enabled", DEFAULT_EXCH_ENABLED)}
        for key, value in payload["enabled"].items():
            if key in DEFAULT_EXCH_ENABLED:
                enabled[key] = bool(value)
        CFG["enabled"] = enabled
        changed = True

    if changed:
        save_config(CFG)
    return JSONResponse(CFG)


@app.get("/api/assets")
async def api_assets():
    logos = {ex: find_logo(ex) for ex in ("MEXC", "Bybit", "BingX")}
    return JSONResponse({"logos": logos, "sounds": list_sounds()})


@app.get("/api/funding-next")
async def api_funding_next(exchange: str = "", symbol: str = ""):
    """Return the nearest next-funding timestamp (ms UTC) for the given exchange.
    When `symbol` is provided for MEXC (e.g. BTC_USDT), fetches per-symbol
    nextSettleTime from MEXC contract/funding_rate endpoint (cached per symbol).
    Falls back to exchange-level minimum across live rows, then BTC_USDT fallback.
    """
    live = await _rlive_all()
    ex = exchange.strip().lower()
    nearest_funding_ms: int = 0
    now_ms = int(time.time() * 1000)

    # Per-symbol MEXC lookup — most precise (each contract has its own cycle)
    sym_upper = symbol.strip().upper()
    if ex == "mexc" and sym_upper:
        cached = _MEXC_SYM_FUND_CACHE.get(sym_upper, {})
        if (time.time() - cached.get("at", 0.0)) < MEXC_FUNDING_CACHE_TTL_SEC and cached.get("ts_ms", 0) > now_ms:
            return JSONResponse({"nextFundingTime": cached["ts_ms"], "exchange": exchange, "symbol": symbol})
        try:
            async with aiohttp.ClientSession() as _s:
                raw = await fetch_json(_s, f"https://contract.mexc.com/api/v1/contract/funding_rate/{sym_upper}")
            d = raw.get("data") if isinstance(raw, dict) else None
            if isinstance(d, dict):
                ts_raw = d.get("nextSettleTime") or d.get("nextFundingTime")
                ts_ms = _mexc_ts_raw_to_ms(ts_raw, now_ms)
                if ts_ms > now_ms:
                    _MEXC_SYM_FUND_CACHE[sym_upper] = {"ts_ms": ts_ms, "at": time.time()}
                    logger.info("[MEXC] %s: next %s", sym_upper, datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc).isoformat())
                    return JSONResponse({"nextFundingTime": ts_ms, "exchange": exchange, "symbol": symbol})
        except Exception as _e:
            logger.warning("[MEXC] per-symbol funding-next error for %s: %s", sym_upper, _e)
        # Fall through to exchange-level lookup below

    for row in live.values():
        if row.get("buy_ex", "").lower() == ex:
            ts = int(row.get("buy_next_ts_ms") or 0)
            if ts > now_ms and (nearest_funding_ms == 0 or ts < nearest_funding_ms):
                nearest_funding_ms = ts
        if row.get("sell_ex", "").lower() == ex:
            ts = int(row.get("sell_next_ts_ms") or 0)
            if ts > now_ms and (nearest_funding_ms == 0 or ts < nearest_funding_ms):
                nearest_funding_ms = ts

    # MEXC bulk ticker often omits nextSettleTime — fall back to BTC_USDT direct call
    if ex == "mexc" and nearest_funding_ms == 0:
        cached_ts = _MEXC_FUND_CACHE["ts_ms"]
        cached_at = _MEXC_FUND_CACHE["at"]
        if cached_ts > now_ms and (time.time() - cached_at) < MEXC_FUNDING_CACHE_TTL_SEC:
            nearest_funding_ms = cached_ts
        else:
            try:
                async with aiohttp.ClientSession() as _s:
                    raw = await fetch_json(_s, MEXC_FUNDING_RATE_BTC)
                d = raw.get("data") if isinstance(raw, dict) else None
                if isinstance(d, dict):
                    ts_raw = d.get("nextSettleTime") or d.get("nextFundingTime")
                    ts_ms = _mexc_ts_raw_to_ms(ts_raw, now_ms)
                    if ts_ms > now_ms:
                        _MEXC_FUND_CACHE["ts_ms"] = ts_ms
                        _MEXC_FUND_CACHE["at"] = time.time()
                        nearest_funding_ms = ts_ms
                        logger.info("[MEXC] next funding: %s", datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc).isoformat())
            except Exception as _e:
                logger.warning("[MEXC] funding-next fallback error: %s", _e)

    return JSONResponse({"nextFundingTime": nearest_funding_ms, "exchange": exchange})


def _mexc_ts_raw_to_ms(ts_raw: Any, now_ms: int) -> int:
    """Convert MEXC nextSettleTime/nextFundingTime to an absolute UTC millisecond timestamp.

    MEXC may return either:
    - An absolute timestamp in milliseconds (> 1e12)
    - An absolute timestamp in seconds (> 1e9)
    - A **remaining-time delta in milliseconds** (< 86 400 000 ms = 1 day) ← common case
    Returns 0 if value is invalid or in the past.
    """
    ts_val = to_float(ts_raw)
    if not math.isfinite(ts_val) or ts_val <= 0:
        return 0
    if ts_val > TIMESTAMP_MS_THRESHOLD:
        ts_ms = int(ts_val)            # already ms timestamp
    elif ts_val > 1e9:
        ts_ms = int(ts_val * 1000)     # seconds timestamp → ms
    else:
        ts_ms = now_ms + int(ts_val)   # delta in ms → absolute timestamp
    return ts_ms if ts_ms > now_ms else 0


@app.get("/api/data")
async def api_data(request: Request):
    # Use async variant to check Redis for sessions not yet in local memory
    # (critical in COLLECTOR_ONLY mode where API restarts with empty SESSIONS dict).
    user = await _session_user_async(request)
    # Determine access tier for this user.
    # Tier is one of "guest" / "paid" / "admin" — maps to a pre-built response
    # built once per compute cycle in _rebuild_data_cache().
    # Serving raw bytes is O(1) per request regardless of how many users are connected.
    is_admin = bool(user and user.get("is_admin"))
    is_paid  = bool(user and _is_subscription_active(user))
    tier = "admin" if is_admin else ("paid" if is_paid else "guest")

    cached = _DATA_CACHE.get(tier)
    # Cross-process mode: when COLLECTOR_ONLY=1 env var is set, the API has no local
    # compute loop — collector writes arb:snap:{tier} to Redis, API reads it here.
    if not cached and _REDIS is not None:
        try:
            snap = await _REDIS.get(f"{_REDIS_KEY_SNAP}:{tier}")
            if snap:
                etag = await _REDIS.get(f"{_REDIS_KEY_SNAP}:etag:{tier}") or ""
                snap_bytes = snap if isinstance(snap, bytes) else snap.encode()
                if etag and request.headers.get("If-None-Match") == etag:
                    return Response(status_code=304, headers={"ETag": etag, "Cache-Control": "no-cache"})
                return Response(content=snap_bytes, media_type="application/json",
                                headers={"ETag": etag, "Cache-Control": "no-cache"} if etag else {})
        except Exception:
            pass  # fall through to live fallback
    if cached:
        etag = _DATA_ETAG.get(tier, "")
        if etag and request.headers.get("If-None-Match") == etag:
            return Response(status_code=304, headers={"ETag": etag, "Cache-Control": "no-cache"})
        return Response(content=cached, media_type="application/json",
                        headers={"ETag": etag, "Cache-Control": "no-cache"} if etag else {"Cache-Control": "no-cache"})

    # Fallback: snapshot not ready yet (first ~5s after startup before first compute cycle).
    # HTTP 202 Accepted: client should retry. JS checks resp.status===202 and returns
    # early — does NOT overwrite STATE.data with an empty placeholder.
    # Never call _rlive_all() here (would trigger Redis HGETALL on 600+ keys per request).
    return JSONResponse(
        {
            "updated_at": "",
            "dbg": {"loading": True},
            "rows": [],
            "access": {
                "username": user.get("username") if user else None,
                "is_admin": is_admin,
                "subscription_approved": is_paid,
                "spread_limit": MAX_FREE_SPREAD,
            },
        },
        status_code=202,
        headers={"Retry-After": "5"},
    )


@app.get("/api/pair")
async def api_pair(request: Request, pair_key: str):
    user = _session_user(request)
    live = await _rlive_all()
    row = live.get(pair_key)
    if not row:
        return JSONResponse({"ok": False, "error": "pair_not_found"}, status_code=404)
    filtered_rows, spread_limit, _is_admin, _is_paid = _limit_rows_for_access([row], user)
    if not filtered_rows:
        return JSONResponse({"ok": False, "error": "forbidden_by_tier", "spread_limit": spread_limit}, status_code=403)
    hist = await _rhist_get(pair_key)
    return JSONResponse({"ok": True, "row": filtered_rows[0], "history": hist[-PAIR_HISTORY_MAX:]})




@app.get("/graph", response_class=HTMLResponse)
async def graph_page(request: Request):
    return templates.TemplateResponse("graph.html", {"request": request, "sv": _STATIC_VER})


@app.post("/api/refresh")
async def api_refresh():
    if COLLECTOR_ONLY:
        return JSONResponse({"ok": False, "error": "collector_only_mode",
                             "detail": "Data is managed by the collector process. Use ws_collector.py."}, status_code=503)
    data = await compute_once()
    async with CACHE_LOCK:
        CACHE.update(data)
    _broadcast_sse(json.dumps({"t": "upd", "at": data.get("updated_at", "")}))
    return JSONResponse({"ok": True})


@app.get("/events")
async def sse_stream(request: Request):
    """Server-Sent Events endpoint.

    Each connected client holds one open TCP connection.
    The updater_loop broadcasts a lightweight 'update available' message
    to all queues; clients then fetch /api/data once.
    This decouples user count from exchange API call frequency.
    """
    q: asyncio.Queue = asyncio.Queue(maxsize=5)
    _SSE_QUEUES.append(q)

    async def generate():
        try:
            yield ": connected\n\n"  # immediate first byte — confirms stream is live
            while True:
                try:
                    payload = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield f"data: {payload}\n\n"
                except asyncio.TimeoutError:
                    yield ": ping\n\n"  # keepalive comment (every 15s)
        finally:
            try:
                _SSE_QUEUES.remove(q)
            except ValueError:
                pass

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/auth/pubkey")
async def api_auth_pubkey():
    return JSONResponse({"public_key": RSA_PUBLIC_PEM})


@app.post("/api/auth/register")
async def api_auth_register(request: Request, payload: Dict[str, Any]):
    # Rate-limit: max 10 registrations per minute per IP
    ip = _get_client_ip(request)
    if not _rl_check(_AUTH_RATE, f"{ip}:{int(time.time() // 60)}:reg", 10):
        return JSONResponse({"ok": False, "error": "too_many_requests"}, status_code=429)

    # RSA decrypt + PBKDF2 hash are CPU-bound (≥200ms). Run in thread pool
    # so the async event loop is never blocked — table updates keep flowing.
    username, password, tg_username = await asyncio.to_thread(_extract_auth_credentials, payload)

    if len(username) < 3 or len(password) < 6:
        return JSONResponse({"ok": False, "error": "invalid_credentials"}, status_code=400)

    async with USERS_LOCK:
        if username in USERS:
            return JSONResponse({"ok": False, "error": "user_exists"}, status_code=400)
        salt, pwh = await asyncio.to_thread(_make_password_record, password)
        USERS[username] = {
            "username": username,
            "salt": salt,
            "password_hash": pwh,
            "pbkdf2_iters": PBKDF2_ITERS,
            "is_admin": False,
            "subscription_approved": False,
            "tg_username": tg_username,
            "tg_chat_id": None,
            "created_at": int(time.time()),
        }
        await asyncio.to_thread(_save_users, USERS)

    # Resolve Telegram chat_id in background (non-blocking)
    if tg_username:
        asyncio.create_task(_resolve_and_store_tg_chat_id(username, tg_username))

    return JSONResponse({"ok": True})


async def _resolve_and_store_tg_chat_id(username: str, tg_username: str) -> None:
    """Background task: resolve @username → chat_id and store in USERS."""
    chat_id = await _tg_resolve_chat_id(tg_username)
    if chat_id is not None:
        async with USERS_LOCK:
            if username in USERS:
                USERS[username]["tg_chat_id"] = chat_id
                await asyncio.to_thread(_save_users, USERS)
        logger.info("Resolved Telegram chat_id=%s for user=%s (@%s)", chat_id, username, tg_username)


@app.post("/api/auth/login")
async def api_auth_login(request: Request, payload: Dict[str, Any]):
    ip = _get_client_ip(request)
    minute_key = f"{ip}:{int(time.time() // 60)}"

    # Rate-limit: max 20 login attempts per minute per IP
    if not _rl_check(_AUTH_RATE, f"{minute_key}:login", 20):
        return JSONResponse({"ok": False, "error": "too_many_requests"}, status_code=429)

    # Brute-force: after 5 failures in the current minute, block for 60s
    fail_key = f"fail:{ip}:{int(time.time() // 60)}"
    if _LOGIN_FAIL.get(fail_key, 0) >= 5:
        return JSONResponse({"ok": False, "error": "too_many_failures"}, status_code=429)

    # RSA decrypt + PBKDF2 verify combined in ONE thread call.
    # Combining avoids two separate asyncio.to_thread round-trips (thread pool
    # scheduling overhead + potential queue wait on VPS with 1 vCPU).
    # _do_login_verify also reads USERS inside the thread (snapshot is safe to read
    # without the lock since we only need a consistent in-memory snapshot).
    result = await asyncio.to_thread(_do_login_verify, payload, dict(USERS))
    if result is None:
        _LOGIN_FAIL[fail_key] = _LOGIN_FAIL.get(fail_key, 0) + 1
        # Evict other-minute fail keys to prevent unbounded growth
        for k in list(_LOGIN_FAIL):
            if k != fail_key:
                _LOGIN_FAIL.pop(k, None)
        return JSONResponse({"ok": False, "error": "bad_login"}, status_code=401)

    username, password, _tg, needs_upgrade = result

    # Clear fail counter on successful login
    _LOGIN_FAIL.pop(fail_key, None)

    # Transparently upgrade legacy 250k-iteration hashes to PBKDF2_ITERS (100k).
    # This runs in background so it never delays the login response.
    if needs_upgrade:
        async def _upgrade_hash() -> None:
            new_salt, new_hash = await asyncio.to_thread(_make_password_record, password)
            async with USERS_LOCK:
                if username in USERS:
                    USERS[username]["salt"] = new_salt
                    USERS[username]["password_hash"] = new_hash
                    USERS[username]["pbkdf2_iters"] = PBKDF2_ITERS
                    await asyncio.to_thread(_save_users, USERS)
        asyncio.create_task(_upgrade_hash())

    user = USERS.get(username, {})
    token = _make_session(username)
    return JSONResponse(
        {
            "ok": True,
            "token": token,
            "user": {
                "username": user["username"],
                "is_admin": bool(user.get("is_admin")),
                "subscription_approved": _is_subscription_active(user),
                "subscription_expires": user.get("subscription_expires"),
                "tg_username": user.get("tg_username") or "",
                "tg_chat_id": user.get("tg_chat_id"),
            },
        }
    )


@app.get("/api/auth/me")
async def api_auth_me(request: Request):
    user = _session_user(request)
    if not user:
        return JSONResponse({"ok": False, "user": None}, status_code=401)
    return JSONResponse(
        {
            "ok": True,
            "user": {
                "username": user["username"],
                "is_admin": bool(user.get("is_admin")),
                "subscription_approved": _is_subscription_active(user),
                "subscription_expires": user.get("subscription_expires"),
                "tg_username": user.get("tg_username") or "",
                "tg_chat_id": user.get("tg_chat_id"),
            },
        }
    )


@app.post("/api/auth/logout")
async def api_auth_logout(request: Request):
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        token = auth[7:]
        SESSIONS.pop(token, None)
        if _REDIS is not None:
            try:
                await _REDIS.delete(f"arb:sess:{token}")
            except Exception:
                pass
    return JSONResponse({"ok": True})


@app.get("/api/admin/users")
async def api_admin_users(request: Request):
    user = _session_user(request)
    if not user or not user.get("is_admin"):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    items = []
    for u in USERS.values():
        items.append({
            "username": u.get("username"),
            "is_admin": bool(u.get("is_admin")),
            "subscription_approved": _is_subscription_active(u),
            "subscription_expires": u.get("subscription_expires"),
            "tg_username": u.get("tg_username") or "",
            "tg_chat_id": u.get("tg_chat_id"),
            "created_at": u.get("created_at"),
        })
    items.sort(key=lambda x: (not x["is_admin"], x["username"]))
    return JSONResponse({"ok": True, "users": items})


@app.post("/api/admin/subscription")
async def api_admin_subscription(request: Request, payload: Dict[str, Any]):
    admin = _session_user(request)
    if not admin or not admin.get("is_admin"):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    username = _normalize_username(str(payload.get("username") or ""))
    approved = bool(payload.get("approved"))
    # days: how long the subscription is valid (30/60/90/180/365); 0 = indefinite
    VALID_DAYS = {0, 30, 60, 90, 180, 365}
    try:
        days = int(payload.get("days") or 0)
    except (TypeError, ValueError):
        days = 0
    if days not in VALID_DAYS:
        days = 0
    if not username or username not in USERS:
        return JSONResponse({"ok": False, "error": "user_not_found"}, status_code=404)
    if USERS[username].get("is_admin"):
        return JSONResponse({"ok": False, "error": "cant_change_admin"}, status_code=400)
    expires_at: Optional[float] = (time.time() + days * 86400) if (approved and days > 0) else None
    async with USERS_LOCK:
        USERS[username]["subscription_approved"] = approved
        USERS[username]["subscription_expires"] = expires_at
        _save_users(USERS)  # inside USERS_LOCK — sync write is acceptable here; wrapping
        # in asyncio.to_thread would release the lock mid-write. This write is fast (small JSON).
        # Read notification targets inside lock to avoid race
        chat_id = USERS[username].get("tg_chat_id")
        tg_user = USERS[username].get("tg_username") or ""

    # Notify user via Telegram bot (non-blocking background task)
    if chat_id or tg_user:
        safe_bot = _tg_escape(TELEGRAM_BOT_USERNAME)
        if approved:
            period_str = f" на {days} дней" if days else " (бессрочно)"
            msg = (
                f"✅ <b>Подписка активирована{period_str}!</b>\n"
                f"Теперь вы можете видеть все спреды на сайте.\n"
                f"🤖 Бот @{safe_bot} активен для вашего аккаунта."
            )
        else:
            msg = (
                f"❌ <b>Подписка отключена.</b>\n"
                f"Доступ ограничен до спредов ≤2%.\n"
                f"🤖 Бот @{safe_bot} приостановлен."
            )
        target = chat_id or f"@{tg_user}"
        asyncio.create_task(_tg_send(target, msg))

    return JSONResponse({"ok": True, "expires_at": expires_at})


@app.post("/api/admin/delete-user")
async def api_admin_delete_user(request: Request, payload: Dict[str, Any]):
    """Admin: permanently delete a user account. Cannot delete admin accounts or self."""
    admin = _session_user(request)
    if not admin or not admin.get("is_admin"):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    username = _normalize_username(str(payload.get("username") or ""))
    if not username or username not in USERS:
        return JSONResponse({"ok": False, "error": "user_not_found"}, status_code=404)
    if USERS[username].get("is_admin"):
        return JSONResponse({"ok": False, "error": "cant_delete_admin"}, status_code=400)
    if username == admin.get("username"):
        return JSONResponse({"ok": False, "error": "cant_delete_self"}, status_code=400)
    # Notify user via Telegram before deleting (non-blocking)
    user_rec = USERS[username]
    chat_id = user_rec.get("tg_chat_id")
    tg_user = user_rec.get("tg_username") or ""
    if chat_id or tg_user:
        msg = "⚠️ <b>Ваш аккаунт был удалён администратором.</b>"
        target = chat_id or f"@{tg_user}"
        asyncio.create_task(_tg_send(target, msg))
    async with USERS_LOCK:
        USERS.pop(username, None)
        await asyncio.to_thread(_save_users, USERS)
    logger.info("Admin %s deleted user %s", admin.get("username"), username)
    return JSONResponse({"ok": True})


@app.get("/api/bot/check-subscription")
async def api_bot_check_subscription(request: Request):
    """
    Bot integration endpoint — lets the Telegram bot check if a user has an active subscription.
    Query: ?tg_username=johndoe  OR  ?chat_id=123456789
    Returns: {"ok": true, "approved": true/false, "username": "site_login"}
    Rate-limited: max 120 requests per minute per IP to prevent abuse.
    """
    # Simple IP-based rate limit: max 120 calls/minute per IP
    client_ip = request.client.host if request.client else "unknown"
    now_min = int(time.time() // 60)
    rl_key = f"botcheck:{client_ip}:{now_min}"
    _BOT_CHECK_RL[rl_key] = _BOT_CHECK_RL.get(rl_key, 0) + 1
    # Evict old keys (keep only current + previous minute)
    for k in list(_BOT_CHECK_RL):
        if k.split(":")[-1] != str(now_min) and k.split(":")[-1] != str(now_min - 1):
            _BOT_CHECK_RL.pop(k, None)
    if _BOT_CHECK_RL[rl_key] > 120:
        return JSONResponse({"ok": False, "error": "rate_limited"}, status_code=429)

    tg_username = _normalize_tg_username(request.query_params.get("tg_username", ""))
    chat_id_raw = request.query_params.get("chat_id", "")
    chat_id_int: Optional[int] = None
    if chat_id_raw:
        try:
            chat_id_int = int(chat_id_raw)
        except (ValueError, TypeError):
            pass

    matched_user = None
    for u in USERS.values():
        # Priority 1: match by resolved tg_chat_id (most reliable)
        if chat_id_int is not None and u.get("tg_chat_id") == chat_id_int:
            matched_user = u
            break
        # Priority 2: match by tg_username stored at registration (fallback)
        if tg_username and _normalize_tg_username(u.get("tg_username", "")) == tg_username:
            matched_user = u
            break

    if not matched_user and chat_id_int is not None:
        # Priority 3: try both in one pass (chat_id may have arrived before username match)
        for u in USERS.values():
            if _normalize_tg_username(u.get("tg_username", "")) == tg_username and tg_username:
                matched_user = u
                break

    if not matched_user:
        return JSONResponse({"ok": True, "approved": False, "username": None})

    return JSONResponse({
        "ok": True,
        "approved": _is_subscription_active(matched_user),
        "username": matched_user.get("username"),
        "tg_linked": matched_user.get("tg_chat_id") is not None,
    })


@app.get("/api/user/link-code")
async def api_user_link_code(request: Request):
    """
    Returns a one-time deep-link for the logged-in user to connect their Telegram account.
    The user opens the link in Telegram (t.me/BOT?start=link_TOKEN); the bot sends
    POST /api/bot/link-telegram with {code, chat_id} to complete the binding.
    Token is valid for 15 minutes.
    """
    user = _session_user(request)
    if not user:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)

    username = user["username"]
    now = time.time()

    # Prune expired codes
    for k in list(_TG_LINK_CODES):
        if _TG_LINK_CODES[k]["expires_at"] < now:
            _TG_LINK_CODES.pop(k, None)

    # Generate a new code even if already linked (re-linking allowed)
    code = secrets.token_hex(16)  # 32 hex chars
    _TG_LINK_CODES[code] = {"username": username, "expires_at": now + 900}  # 15 min

    bot = TELEGRAM_BOT_USERNAME.lstrip("@")
    link = f"https://t.me/{bot}?start=link_{code}"
    return JSONResponse({"ok": True, "code": code, "link": link})


@app.post("/api/bot/link-telegram")
async def api_bot_link_telegram(request: Request):
    """
    Called by the Telegram bot when a user sends /start link_<code>.
    Body: {code: str, chat_id: int}
    Validates the code, stores tg_chat_id in the user record, sends confirmation.
    """
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid json"}, status_code=400)

    code = str(payload.get("code", "")).strip()
    try:
        chat_id = int(payload.get("chat_id", 0))
    except (TypeError, ValueError):
        return JSONResponse({"ok": False, "error": "invalid chat_id"}, status_code=400)

    if not code or not chat_id:
        return JSONResponse({"ok": False, "error": "code and chat_id required"}, status_code=400)

    now = time.time()
    entry = _TG_LINK_CODES.get(code)
    if not entry or entry["expires_at"] < now:
        return JSONResponse({"ok": False, "error": "invalid or expired code"}, status_code=400)

    username = entry["username"]
    _TG_LINK_CODES.pop(code, None)  # one-time use

    async with USERS_LOCK:
        if username not in USERS:
            return JSONResponse({"ok": False, "error": "user not found"}, status_code=404)
        USERS[username]["tg_chat_id"] = chat_id
        await asyncio.to_thread(_save_users, USERS)

    logger.info("Telegram chat_id=%s linked to user=%s via deep-link code", chat_id, username)

    # Confirm to the user
    asyncio.create_task(_tg_send(chat_id, "✅ Ваш Telegram успешно привязан к аккаунту на сайте!"))

    return JSONResponse({"ok": True, "username": username})


def run():
    env = os.getenv("ENV", "production").strip().lower()
    if env not in {"development", "production"}:
        env = "production"

    is_dev = env == "development"
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))

    # Use uvloop on Linux/macOS (2-3× faster I/O than default asyncio event loop).
    # uvicorn[standard] already installs uvloop as a dependency.
    # timeout_keep_alive=30: keep TCP connections open for 30s per user (default
    # is 5s; with 20+ concurrent users this reduces connection-setup overhead).
    try:
        import uvloop  # noqa: F401
        loop_policy = "uvloop"
    except ImportError:
        loop_policy = "asyncio"

    uvicorn.run(
        app,
        host=host,
        port=port,
        reload=is_dev,
        log_config=None,
        access_log=is_dev,
        loop=loop_policy,
        timeout_keep_alive=30,
    )


if __name__ == "__main__":
    run()
