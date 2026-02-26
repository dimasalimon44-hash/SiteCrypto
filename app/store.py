"""Redis helpers, in-memory stores, and snapshot/cache management."""
import asyncio
import hashlib
import json
import logging
import math
import time
from threading import Lock as _ThreadLock
from typing import Any, Dict, List, Optional

from app.config import MAX_FREE_SPREAD
from app.funding import _spread_sort_key

logger = logging.getLogger("arb_dashboard")

# Redis key constants
_REDIS_KEY_LIVE = "arb:live"
_REDIS_KEY_CACHE_META = "arb:cache_meta"
_REDIS_CHANNEL_SSE = "arb:sse"
_REDIS_KEY_SNAP = "arb:snap"
_REDIS_KEY_MEXC_INT = "arb:mexc:intervals"

# In-memory state
LIVE_ROWS: Dict[str, dict] = {}
PAIR_HISTORY: Dict[str, List[Dict[str, Any]]] = {}
PAIR_HISTORY_MAX = 300
CACHE: Dict[str, Any] = {
    "updated_at": None,
    "rows": [],
    "dbg": {"mexc": 0, "bybit": 0, "bingx": 0, "kept": 0, "took_ms": 0},
}
CACHE_LOCK = asyncio.Lock()

# Pre-built /api/data response bodies per access tier
_DATA_CACHE: Dict[str, bytes] = {}
_DATA_ETAG: Dict[str, str] = {}

# Central pre-computed data store (populated by collectors / compute_once)
DATA_STORE: List[Dict] = []
DATA_LOCK = _ThreadLock()
LAST_UPDATE_TS: float = 0.0

# Per-exchange nearest next-funding timestamps (ms UTC), keyed by exchange name
# lower-cased (e.g. "mexc", "bybit", "bingx").  Updated by _next_funding_task()
# every 15 minutes — not on every aggregation cycle.
FUNDING_STORE: Dict[str, int] = {}
FUNDING_LOCK = _ThreadLock()


def _get_redis() -> Optional[Any]:
    """Return current Redis client from the package-level _REDIS variable."""
    import app as _app_pkg
    return _app_pkg._REDIS


# ---------------------------------------------------------------------------
# Redis connect / disconnect
# ---------------------------------------------------------------------------

async def _redis_connect() -> None:
    import app as _app_pkg
    url = __import__("os").environ.get("REDIS_URL", "").strip()
    if not url:
        return
    try:
        import redis.asyncio as aioredis  # type: ignore[import]
    except ImportError:
        return
    try:
        client = aioredis.from_url(url, encoding="utf-8", decode_responses=True)
        await client.ping()
        _app_pkg._REDIS = client
        logger.info("[Redis] Connected: %s", url)
    except Exception as exc:
        logger.warning("[Redis] Cannot connect to %r: %s — using in-memory fallback", url, exc)
        _app_pkg._REDIS = None


async def _redis_disconnect() -> None:
    import app as _app_pkg
    redis = _app_pkg._REDIS
    if redis is not None:
        try:
            await redis.aclose()
        except Exception:
            pass
        _app_pkg._REDIS = None


# ---------------------------------------------------------------------------
# LIVE_ROWS helpers
# ---------------------------------------------------------------------------

async def _rlive_set_batch(rows: Dict[str, dict]) -> None:
    LIVE_ROWS.update(rows)
    redis = _get_redis()
    if redis is not None and rows:
        try:
            pipe = redis.pipeline()
            for k, v in rows.items():
                pipe.hset(_REDIS_KEY_LIVE, k, json.dumps(v))
            await pipe.execute()
        except Exception:
            pass


async def _rlive_del(pair_key: str) -> None:
    LIVE_ROWS.pop(pair_key, None)
    redis = _get_redis()
    if redis is not None:
        try:
            await redis.hdel(_REDIS_KEY_LIVE, pair_key)
        except Exception:
            pass


async def _rlive_all() -> Dict[str, dict]:
    redis = _get_redis()
    if redis is not None:
        try:
            raw = await redis.hgetall(_REDIS_KEY_LIVE)
            if raw:
                return {k: json.loads(v) for k, v in raw.items()}
        except Exception:
            pass
    return dict(LIVE_ROWS)


# ---------------------------------------------------------------------------
# PAIR_HISTORY helpers
# ---------------------------------------------------------------------------

async def _rhist_append(pair_key: str, entry: dict) -> None:
    h = PAIR_HISTORY.setdefault(pair_key, [])
    h.append(entry)
    if len(h) > PAIR_HISTORY_MAX:
        del h[:-PAIR_HISTORY_MAX]
    redis = _get_redis()
    if redis is not None:
        rkey = f"arb:hist:{pair_key}"
        try:
            pipe = redis.pipeline()
            pipe.rpush(rkey, json.dumps(entry))
            pipe.ltrim(rkey, -PAIR_HISTORY_MAX, -1)
            await pipe.execute()
        except Exception:
            pass


async def _rhist_get(pair_key: str) -> List[dict]:
    redis = _get_redis()
    if redis is not None:
        rkey = f"arb:hist:{pair_key}"
        try:
            raw = await redis.lrange(rkey, 0, -1)
            if raw:
                return [json.loads(x) for x in raw]
        except Exception:
            pass
    return list(PAIR_HISTORY.get(pair_key, []))


# ---------------------------------------------------------------------------
# CACHE metadata helpers
# ---------------------------------------------------------------------------

async def _rcache_set(meta: dict) -> None:
    redis = _get_redis()
    if redis is not None:
        try:
            await redis.set(_REDIS_KEY_CACHE_META, json.dumps(meta))
        except Exception:
            pass


async def _rcache_get() -> dict:
    redis = _get_redis()
    if redis is not None:
        try:
            raw = await redis.get(_REDIS_KEY_CACHE_META)
            if raw:
                return json.loads(raw)
        except Exception:
            pass
    return {}


# ---------------------------------------------------------------------------
# Snapshot write
# ---------------------------------------------------------------------------

async def _rsnapshot_write() -> None:
    """Write pre-built snapshot bytes to Redis for cross-process API workers."""
    redis = _get_redis()
    if redis is None:
        return
    try:
        pipe = redis.pipeline(transaction=False)
        for t in ("guest", "paid", "admin"):
            if t in _DATA_CACHE:
                pipe.set(f"{_REDIS_KEY_SNAP}:{t}", _DATA_CACHE[t], ex=120)
                pipe.set(f"{_REDIS_KEY_SNAP}:etag:{t}", _DATA_ETAG.get(t, ""), ex=120)
        await pipe.execute()
    except Exception as exc:
        logger.debug("[Redis] _rsnapshot_write error: %s", exc)


# ---------------------------------------------------------------------------
# Data cache builder
# ---------------------------------------------------------------------------

def _rebuild_data_cache(rows_out: List[dict], cache_meta: dict) -> None:
    """Pre-build /api/data JSON response bytes for all 3 access tiers.

    Called once at the end of each compute_once() cycle.  api_data() then
    returns the appropriate pre-built bytes directly — zero Redis I/O, zero
    JSON parsing, zero sorting, zero serialisation per user request.
    """
    global LAST_UPDATE_TS
    sorted_rows = sorted(rows_out, key=_spread_sort_key, reverse=True)
    updated_at = cache_meta.get("updated_at", time.strftime("%H:%M:%S"))
    dbg_base = dict(cache_meta.get("dbg", {}))

    # Update central data store (used by API as precomputed read-only source)
    with DATA_LOCK:
        DATA_STORE.clear()
        DATA_STORE.extend(sorted_rows)
        LAST_UPDATE_TS = time.time()

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

        data: Dict[str, Any] = {
            "updated_at": updated_at,
            "dbg": {**dbg_base, "kept": len(rows)},
            "rows": rows,
            "access": {
                "username": None,
                "is_admin": is_admin_tier,
                "subscription_approved": is_paid_tier,
                "spread_limit": spread_limit,
            },
        }
        try:
            raw_bytes = json.dumps(data, ensure_ascii=False, allow_nan=False).encode()
        except (ValueError, TypeError):
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
        _DATA_ETAG[tier] = '"' + hashlib.sha256(_DATA_CACHE[tier]).hexdigest()[:16] + '"'
