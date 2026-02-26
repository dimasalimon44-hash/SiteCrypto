"""API routes: /api/data, /api/config, /api/pair, /api/assets, /api/refresh, /events."""
import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from app.auth import (
    _is_subscription_active,
    _limit_rows_for_access,
    _session_user,
    _session_user_async,
)
from app.config import (
    CFG,
    COLLECTOR_ONLY,
    RUN_UPDATER,
    DEFAULT_EXCH_ENABLED,
    LOGOS_DIR,
    MAX_FREE_SPREAD,
    SOUNDS_DIR,
)
from app.sse import _SSE_QUEUES, _broadcast_sse
from app.store import (
    _DATA_CACHE,
    _DATA_ETAG,
    _REDIS_KEY_SNAP,
    _rlive_all,
    _rhist_get,
    CACHE,
    CACHE_LOCK,
    PAIR_HISTORY_MAX,
    _rebuild_data_cache,
    _rcache_set,
)

logger = logging.getLogger("arb_dashboard")
router = APIRouter()


# ---------------------------------------------------------------------------
# Asset helpers
# ---------------------------------------------------------------------------

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




# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/api/config")
async def api_config():
    return JSONResponse(CFG)


@router.post("/api/config")
async def api_config_set(payload: Dict[str, Any]):
    from app.config import save_config
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


@router.get("/api/assets")
async def api_assets():
    logos = {ex: find_logo(ex) for ex in ("MEXC", "Bybit", "BingX")}
    return JSONResponse({"logos": logos, "sounds": list_sounds()})


@router.get("/api/data")
async def api_data(request: Request):
    user = await _session_user_async(request)
    is_admin = bool(user and user.get("is_admin"))
    is_paid = bool(user and _is_subscription_active(user))
    tier = "admin" if is_admin else ("paid" if is_paid else "guest")

    cached = _DATA_CACHE.get(tier)
    import app as _app_pkg
    redis = _app_pkg._REDIS
    if not cached and redis is not None:
        try:
            snap = await redis.get(f"{_REDIS_KEY_SNAP}:{tier}")
            if snap:
                etag = await redis.get(f"{_REDIS_KEY_SNAP}:etag:{tier}") or ""
                snap_bytes = snap if isinstance(snap, bytes) else snap.encode()
                if etag and request.headers.get("If-None-Match") == etag:
                    return Response(status_code=304, headers={"ETag": etag, "Cache-Control": "no-cache"})
                return Response(content=snap_bytes, media_type="application/json",
                                headers={"ETag": etag, "Cache-Control": "no-cache"} if etag else {})
        except Exception:
            pass
    if cached:
        etag = _DATA_ETAG.get(tier, "")
        if etag and request.headers.get("If-None-Match") == etag:
            return Response(status_code=304, headers={"ETag": etag, "Cache-Control": "no-cache"})
        return Response(content=cached, media_type="application/json",
                        headers={"ETag": etag, "Cache-Control": "no-cache"} if etag else {"Cache-Control": "no-cache"})

    # Fallback: build cache directly from arb:live hash when no snapshot is available.
    # The collector writes HSET arb:live but may not publish arb:snap:* snapshots yet
    # (e.g. first startup, older collector version, or snapshot TTL expired).
    try:
        live = await _rlive_all()
        if live:
            _rebuild_data_cache(list(live.values()), {"updated_at": time.strftime("%H:%M:%S")})
            cached = _DATA_CACHE.get(tier)
            if cached:
                etag = _DATA_ETAG.get(tier, "")
                if etag and request.headers.get("If-None-Match") == etag:
                    return Response(status_code=304, headers={"ETag": etag, "Cache-Control": "no-cache"})
                return Response(content=cached, media_type="application/json",
                                headers={"ETag": etag, "Cache-Control": "no-cache"} if etag else {"Cache-Control": "no-cache"})
    except Exception:
        logger.debug("[api_data] arb:live fallback failed", exc_info=True)

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


@router.get("/api/pair")
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


@router.post("/api/refresh")
async def api_refresh():
    if not RUN_UPDATER:
        return JSONResponse({"ok": False, "error": "api_only_mode",
                             "detail": "Data is managed by the collector process. Use ws_collector.py."}, status_code=503)
    from app.main import compute_once
    data = await compute_once()
    async with CACHE_LOCK:
        CACHE.update(data)
    _broadcast_sse(json.dumps({"t": "upd", "at": data.get("updated_at", "")}))
    return JSONResponse({"ok": True})


@router.get("/events")
async def sse_stream(request: Request):
    """Server-Sent Events endpoint."""
    q: asyncio.Queue = asyncio.Queue(maxsize=5)
    _SSE_QUEUES.append(q)

    async def generate():
        try:
            yield ": connected\n\n"
            while True:
                try:
                    payload = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield f"data: {payload}\n\n"
                except asyncio.TimeoutError:
                    yield ": ping\n\n"
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
