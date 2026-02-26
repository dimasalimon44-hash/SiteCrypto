"""app package — backward-compatible re-exports for collector.py and ws_collector.py.

Both external scripts do ``import app as _a`` and access symbols like
``_a._REDIS``, ``_a.normalize_usdt``, etc.  This __init__.py re-exports every
symbol they need so that ``import app`` continues to work after the refactor.

Mutable module-level globals that get *re-assigned* at runtime (_REDIS,
_HTTP_SESSION) are defined here directly so that attribute assignments via
``_a._REDIS = client`` land in this module's namespace and are immediately
visible to all other modules that read them through ``import app; app._REDIS``.
"""
from __future__ import annotations

import sys
import logging
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Logging — keep the root logger configured when app is imported standalone
# ---------------------------------------------------------------------------
_LOG_LEVEL = __import__("os").getenv("LOG_LEVEL", "INFO").upper()
_log_fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(name)s: %(message)s", datefmt="%H:%M:%S")
_root = logging.getLogger()
if not _root.handlers:
    _root.setLevel(getattr(logging, _LOG_LEVEL, logging.INFO))
    _stdout_h = logging.StreamHandler(sys.stdout)
    _stdout_h.setFormatter(_log_fmt)
    _root.addHandler(_stdout_h)
    _log_file = __import__("os").getenv("LOG_FILE", "")
    if _log_file:
        from logging.handlers import RotatingFileHandler as _RFH
        _file_h = _RFH(_log_file, maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8")
        _file_h.setFormatter(_log_fmt)
        _root.addHandler(_file_h)
for _uv_log in ("uvicorn", "uvicorn.error", "uvicorn.access"):
    logging.getLogger(_uv_log).propagate = False
logger = logging.getLogger("arb_dashboard")
logger.propagate = False

# ---------------------------------------------------------------------------
# Frozen-executable stdout guard
# ---------------------------------------------------------------------------
if getattr(sys, "frozen", False):
    if sys.stdout is None:
        sys.stdout = open(__import__("os").devnull, "w")
    if sys.stderr is None:
        sys.stderr = open(__import__("os").devnull, "w")

# ---------------------------------------------------------------------------
# Mutable globals that get *reassigned* at runtime — defined here so that
#   _a._REDIS = client
# in ws_collector/collector lands in *this* namespace and is readable by all
# submodules that do ``import app; app._REDIS``.
# ---------------------------------------------------------------------------
_REDIS: Optional[Any] = None
_HTTP_SESSION: Optional[Any] = None

# ---------------------------------------------------------------------------
# Re-exports from submodules
# ---------------------------------------------------------------------------

# config
from app.config import (  # noqa: E402
    CFG,
    DEFAULT_MIN_VOL_USD,
    DEFAULT_MIN_SPREAD,
    REFRESH_SEC,
    COLLECTOR_ONLY,
    RUN_UPDATER,
    MAX_FREE_SPREAD,
    BINGX_CONTRACTS,
    BINGX_FUNDING_RATE,
    BYBIT_INSTRUMENTS,
    MEXC_TICKERS,
    MEXC_CONTRACT_DETAIL,
    MEXC_FUNDING_RATE_BTC,
    BINGX_BOOK_TICKER,
    BINGX_TICKER_24H,
    BINGX_PREMIUM_INDEX,
    SESSION_TTL_SEC,
    PBKDF2_ITERS,
    load_config,
    save_config,
)

# store — Redis helpers and in-memory stores
from app.store import (  # noqa: E402
    LIVE_ROWS,
    PAIR_HISTORY,
    PAIR_HISTORY_MAX,
    CACHE,
    CACHE_LOCK,
    _DATA_CACHE,
    _DATA_ETAG,
    _REDIS_KEY_LIVE,
    _REDIS_KEY_CACHE_META,
    _REDIS_CHANNEL_SSE,
    _REDIS_KEY_SNAP,
    _REDIS_KEY_MEXC_INT,
    _redis_connect,
    _redis_disconnect,
    _rlive_set_batch,
    _rlive_all,
    _rlive_del,
    _rhist_append,
    _rhist_get,
    _rcache_set,
    _rcache_get,
    _rsnapshot_write,
    _rebuild_data_cache,
)

# exchange utilities
from app.exchanges import (  # noqa: E402
    to_float,
    is_pos,
    normalize_usdt,
    normalize_symbol_key,
    _as_list,
    _pick_float,
    _match_symbol_entry,
    _pick_ts,
    _pick_ts_or_delta,
    fetch_json,
    mexc_trade_url,
    bybit_trade_url,
    bingx_trade_url,
)

# funding
from app.funding import (  # noqa: E402
    _norm_interval_h,
    _pick_int,
    _infer_bingx_interval_h,
    funding_24h_estimate,
    funding_eta_str,
    _adjusted_fund,
    _safe_float,
    exec_spread,
    best_pairs,
    _spread_sort_key,
)

# exchange modules — mutable dict caches (re-exported by reference → mutations visible)
from app.exchanges.mexc import (  # noqa: E402
    _MEXC_INTERVALS,
    _MEXC_INTERVALS_AT,
    _MEXC_FUND_CACHE,
    _MEXC_SYM_FUND_CACHE,
    _mexc_intervals_refresher,
    load_mexc,
)
from app.exchanges.bybit import (  # noqa: E402
    _BYBIT_INTERVALS,
    load_bybit,
)
from app.exchanges.bingx import (  # noqa: E402
    _BINGX_INTERVALS,
    _BINGX_CONTRACTS_CACHE,
    load_bingx,
)

# sse
from app.sse import _broadcast_sse, _redis_sse_subscriber  # noqa: E402

# auth helpers that auth routes expose (not needed by collector but harmless)
from app.auth import (  # noqa: E402
    RSA_PUBLIC_PEM,
    AUTH_CIPHER,
    USERS,
    USERS_LOCK,
    SESSIONS,
)


# ---------------------------------------------------------------------------
# Lazy re-exports that would cause circular imports if loaded eagerly
# ---------------------------------------------------------------------------

def __getattr__(name: str) -> Any:
    """Lazily proxy compute_once and updater_loop from app.main.

    These are defined in app.main which imports from routes which import from
    auth/store — all of which are already loaded by the time this function is
    called (only at runtime, never at module load time).
    """
    if name in ("updater_loop", "compute_once"):
        from app import main as _main
        return getattr(_main, name)
    raise AttributeError(f"module 'app' has no attribute {name!r}")
