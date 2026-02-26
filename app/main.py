"""FastAPI application: middleware, lifespan, page routes, compute loop, runner."""
import asyncio
import json
import logging
import math
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import aiohttp
import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.datastructures import MutableHeaders
from starlette.middleware.gzip import GZipMiddleware

from app.config import (
    ASSETS_DIR,
    CFG,
    COLLECTOR_ONLY,
    CYCLE_WARN_MS,
    DEFAULT_EXCH_ENABLED,
    DEFAULT_MIN_SPREAD,
    DEFAULT_MIN_VOL_USD,
    MAX_FREE_SPREAD,
    REFRESH_SEC,
    STATIC_DIR,
    TEMPLATES_DIR,
)
from app.exchanges import fetch_json
from app.exchanges.bingx import load_bingx
from app.exchanges.bybit import load_bybit
from app.exchanges.mexc import load_mexc, _mexc_intervals_refresher
from app.funding import best_pairs, _spread_sort_key
from app.sse import _broadcast_sse, _redis_sse_subscriber
from app.store import (
    CACHE,
    CACHE_LOCK,
    LIVE_ROWS,
    PAIR_HISTORY_MAX,
    _DATA_CACHE,
    _REDIS_KEY_SNAP,
    _rcache_set,
    _rebuild_data_cache,
    _redis_connect,
    _redis_disconnect,
    _rhist_append,
    _rlive_all,
    _rlive_del,
    _rlive_set_batch,
    _rsnapshot_write,
)

logger = logging.getLogger("arb_dashboard")

# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

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
    """Pure ASGI security headers middleware — zero body-buffering overhead."""

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


# ---------------------------------------------------------------------------
# Compute engine
# ---------------------------------------------------------------------------

async def _push_pairs_to_live_rows(
    mexc: Dict[str, Any],
    bybit: Dict[str, Any],
    bingx: Dict[str, Any],
    min_vol: float,
    min_spread: float,
    symbols: Optional[set] = None,
) -> None:
    if symbols is None:
        symbols = set(mexc.keys()) | set(bybit.keys()) | set(bingx.keys())
    for i, symbol in enumerate(symbols):
        if i % 20 == 0:
            await asyncio.sleep(0)
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
            LIVE_ROWS[key] = pair


async def compute_once() -> Dict[str, Any]:
    import app as _app_pkg
    started = time.time()
    session = _app_pkg._HTTP_SESSION
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

        bingx = await load_bingx(session, sorted_candidates) if enabled.get("BingX", True) else {}
    finally:
        if _owned:
            await session.close()

    rows_out: List[dict] = []
    all_symbols = set(mexc.keys()) | set(bybit.keys()) | set(bingx.keys())

    for i, symbol in enumerate(all_symbols):
        if i % 20 == 0:
            await asyncio.sleep(0)
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

    final_valid_keys = {r["pair_key"] for r in rows_out}
    stale_keys = [k for k in list(LIVE_ROWS) if k not in final_valid_keys]
    for k in stale_keys:
        await _rlive_del(k)
    await _rlive_set_batch({r["pair_key"]: r for r in rows_out})

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
    _rebuild_data_cache(rows_out, cache_meta)
    asyncio.create_task(_rsnapshot_write())

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


async def updater_loop() -> None:
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


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

def ensure_assets() -> None:
    from app.config import LOGOS_DIR, SOUNDS_DIR, STATIC_DIR
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


@asynccontextmanager
async def lifespan(_: FastAPI):
    import app as _app_pkg
    cpu_count = os.cpu_count() or 2
    asyncio.get_running_loop().set_default_executor(
        ThreadPoolExecutor(max_workers=cpu_count * 2)
    )
    await _redis_connect()
    connector = aiohttp.TCPConnector(limit=60, ttl_dns_cache=300)
    _app_pkg._HTTP_SESSION = aiohttp.ClientSession(connector=connector)
    if not COLLECTOR_ONLY:
        logger.warning("Running in FULL mode (with updater) — set COLLECTOR_ONLY=1 for production")
        asyncio.create_task(updater_loop())
        asyncio.create_task(_mexc_intervals_refresher())
    else:
        logger.warning("Running in COLLECTOR_ONLY mode — no updater tasks started (reads from Redis)")
    if _app_pkg._REDIS is not None and COLLECTOR_ONLY:
        asyncio.create_task(_redis_sse_subscriber())
    yield
    await _app_pkg._HTTP_SESSION.close()
    await _redis_disconnect()


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

ensure_assets()
app = FastAPI(lifespan=lifespan)

_cors_origins = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)
app.add_middleware(GZipExcludeMiddleware, minimum_size=500)
app.add_middleware(SecurityHeadersMiddleware)
app.mount("/assets", StaticFiles(directory=ASSETS_DIR), name="assets")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

_START_TIME = time.time()
_STATIC_VER = hex(int(_START_TIME))[2:]

# Include routers
from app.routes import api as _api_mod, auth as _auth_mod
from app.routes.health import router as health_router

app.include_router(health_router)
app.include_router(_api_mod.router)
app.include_router(_auth_mod.router)


# ---------------------------------------------------------------------------
# Page routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Serve the dashboard with server-injected initial snapshot."""
    import app as _app_pkg
    snap_bytes = _DATA_CACHE.get("guest")
    if not snap_bytes and _app_pkg._REDIS is not None:
        try:
            snap_bytes = await _app_pkg._REDIS.get(f"{_REDIS_KEY_SNAP}:guest")
        except Exception as exc:
            logger.debug("[index] Redis arb:snap:guest unavailable: %s", exc)
    if snap_bytes:
        snap_str = snap_bytes.decode() if isinstance(snap_bytes, bytes) else snap_bytes
        initial_data = snap_str
    else:
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


@app.get("/graph", response_class=HTMLResponse)
async def graph_page(request: Request):
    return templates.TemplateResponse("graph.html", {"request": request, "sv": _STATIC_VER})


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run() -> None:
    env = os.getenv("ENV", "production").strip().lower()
    if env not in {"development", "production"}:
        env = "production"

    is_dev = env == "development"
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))

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
