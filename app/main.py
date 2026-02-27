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
from typing import Any, Dict, List, Optional, Tuple

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
    RUN_UPDATER,
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
    FUNDING_LOCK,
    FUNDING_STORE,
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
# Per-exchange data caches — updated by independent background tasks
# ---------------------------------------------------------------------------

_MEXC_DATA: Dict[str, Any] = {}
_BYBIT_DATA: Dict[str, Any] = {}
_BINGX_DATA: Dict[str, Any] = {}
# Timestamp of most recent exchange cache update; watched by _aggregator_task
_LAST_EXCHANGE_UPDATE_TS: float = 0.0

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

def _build_bingx_candidates(mexc: Dict, bybit: Dict) -> List[str]:
    """Build BingX candidate list sorted by 24h volume from MEXC+Bybit caches."""
    candidates: Dict[str, float] = {}
    for source in (mexc, bybit):
        for symbol, row in source.items():
            vol = row.vol24_usd if math.isfinite(row.vol24_usd) else 0.0
            candidates[symbol] = max(candidates.get(symbol, 0.0), vol)
    return [x[0] for x in sorted(candidates.items(), key=lambda item: item[1], reverse=True)]


async def _run_aggregation(
    mexc: Dict, bybit: Dict, bingx: Dict,
    started: Optional[float] = None,
) -> Tuple[List[dict], Dict]:
    """Compute spread pairs from exchange data snapshots and publish results.

    Returns (rows_out, cache_meta).  Callers receive timing data and can log
    exchange-specific metrics.  Broadcasts an SSE update event on completion.
    """
    if started is None:
        started = time.time()
    min_vol = float(CFG.get("min_vol", DEFAULT_MIN_VOL_USD))
    min_spread = float(CFG.get("min_spread", DEFAULT_MIN_SPREAD))

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

    took_ms = int((time.time() - started) * 1000)
    cache_meta: Dict[str, Any] = {
        "updated_at": time.strftime("%H:%M:%S"),
        "dbg": {
            "mexc": len(mexc),
            "bybit": len(bybit),
            "bingx": len(bingx),
            "kept": len(rows_out),
            "took_ms": took_ms,
        },
    }
    await _rcache_set(cache_meta)
    _rebuild_data_cache(rows_out, cache_meta)
    asyncio.create_task(_rsnapshot_write())
    _broadcast_sse(json.dumps({"t": "upd", "at": cache_meta["updated_at"]}))
    try:
        from services.telegram_alerts import send_spread_alerts  # noqa: PLC0415
        send_spread_alerts(rows_out)
    except Exception:
        pass
    return rows_out, cache_meta


async def compute_once() -> Dict[str, Any]:
    """Fetch all exchanges concurrently and compute spread pairs.

    Used by /api/refresh for an on-demand forced refresh.  Also updates the
    per-exchange in-memory caches so background tasks stay in sync.
    """
    import app as _app_pkg
    global _MEXC_DATA, _BYBIT_DATA, _BINGX_DATA
    started = time.time()
    session = _app_pkg._HTTP_SESSION
    _owned = False
    if session is None or session.closed:
        session = aiohttp.ClientSession()
        _owned = True
    try:
        enabled = CFG.get("enabled", DEFAULT_EXCH_ENABLED)
        mexc_t = asyncio.create_task(load_mexc(session)) if enabled.get("MEXC", True) else None
        bybit_t = asyncio.create_task(load_bybit(session)) if enabled.get("Bybit", True) else None

        _t0 = time.perf_counter()
        mexc = await mexc_t if mexc_t else {}
        bybit = await bybit_t if bybit_t else {}
        logger.info(
            "[MEXC+Bybit] loaded in %d ms | MEXC: %d | Bybit: %d",
            int((time.perf_counter() - _t0) * 1000), len(mexc), len(bybit),
        )
        if mexc:
            _MEXC_DATA = mexc
        if bybit:
            _BYBIT_DATA = bybit

        _t_bingx = time.perf_counter()
        bingx = (
            await load_bingx(session, _build_bingx_candidates(mexc, bybit))
            if enabled.get("BingX", True) else {}
        )
        logger.info(
            "[BingX] loaded %d symbols in %d ms",
            len(bingx), int((time.perf_counter() - _t_bingx) * 1000),
        )
        if bingx:
            _BINGX_DATA = bingx
    finally:
        if _owned:
            await session.close()

    rows_out, cache_meta = await _run_aggregation(mexc, bybit, bingx, started)

    took_ms = int((time.time() - started) * 1000)
    logger.info(
        "Cycle: %d ms | MEXC: %d | Bybit: %d | BingX: %d | pairs: %d",
        took_ms, len(mexc), len(bybit), len(bingx), len(rows_out),
    )

    return {
        "started_ts": started,
        "updated_at": cache_meta["updated_at"],
        "rows": rows_out,
        "dbg": cache_meta["dbg"],
    }


# ---------------------------------------------------------------------------
# Per-exchange independent background tasks
# ---------------------------------------------------------------------------

async def _mexc_task() -> None:
    """Independent MEXC data fetcher — runs as a separate background asyncio task."""
    import app as _app_pkg
    global _MEXC_DATA, _LAST_EXCHANGE_UPDATE_TS
    while True:
        enabled = CFG.get("enabled", DEFAULT_EXCH_ENABLED)
        if enabled.get("MEXC", True):
            start = time.perf_counter()
            try:
                session = _app_pkg._HTTP_SESSION
                if session is not None and not session.closed:
                    data = await asyncio.wait_for(load_mexc(session), timeout=20.0)
                    if data:
                        _MEXC_DATA = data
                        _LAST_EXCHANGE_UPDATE_TS = time.time()
                    logger.info("[MEXC] update took %d ms, %d symbols",
                                int((time.perf_counter() - start) * 1000), len(_MEXC_DATA))
            except asyncio.TimeoutError:
                logger.warning("[MEXC] fetch timed out after 20 s")
            except Exception:
                logger.exception("[MEXC] task error")
        await asyncio.sleep(float(CFG.get("refresh_sec", REFRESH_SEC)))


async def _bybit_task() -> None:
    """Independent Bybit data fetcher — runs as a separate background asyncio task."""
    import app as _app_pkg
    global _BYBIT_DATA, _LAST_EXCHANGE_UPDATE_TS
    while True:
        enabled = CFG.get("enabled", DEFAULT_EXCH_ENABLED)
        if enabled.get("Bybit", True):
            start = time.perf_counter()
            try:
                session = _app_pkg._HTTP_SESSION
                if session is not None and not session.closed:
                    data = await asyncio.wait_for(load_bybit(session), timeout=20.0)
                    if data:
                        _BYBIT_DATA = data
                        _LAST_EXCHANGE_UPDATE_TS = time.time()
                    logger.info("[Bybit] update took %d ms, %d symbols",
                                int((time.perf_counter() - start) * 1000), len(_BYBIT_DATA))
            except asyncio.TimeoutError:
                logger.warning("[Bybit] fetch timed out after 20 s")
            except Exception:
                logger.exception("[Bybit] task error")
        await asyncio.sleep(float(CFG.get("refresh_sec", REFRESH_SEC)))


async def _bingx_task() -> None:
    """Independent BingX data fetcher — runs as a separate background asyncio task.

    Starts after one REFRESH_SEC delay so MEXC and Bybit have data to build
    the candidate symbol list before the first BingX fetch.
    """
    import app as _app_pkg
    global _BINGX_DATA, _LAST_EXCHANGE_UPDATE_TS
    await asyncio.sleep(float(CFG.get("refresh_sec", REFRESH_SEC)))
    while True:
        enabled = CFG.get("enabled", DEFAULT_EXCH_ENABLED)
        if enabled.get("BingX", True):
            start = time.perf_counter()
            try:
                session = _app_pkg._HTTP_SESSION
                if session is not None and not session.closed:
                    candidates = _build_bingx_candidates(_MEXC_DATA, _BYBIT_DATA)
                    data = await asyncio.wait_for(
                        load_bingx(session, candidates), timeout=30.0
                    )
                    if data:
                        _BINGX_DATA = data
                        _LAST_EXCHANGE_UPDATE_TS = time.time()
                    logger.info("[BingX] update took %d ms, %d symbols",
                                int((time.perf_counter() - start) * 1000), len(_BINGX_DATA))
            except asyncio.TimeoutError:
                logger.warning("[BingX] fetch timed out after 30 s")
            except Exception:
                logger.exception("[BingX] task error")
        await asyncio.sleep(float(CFG.get("refresh_sec", REFRESH_SEC)))


async def _aggregator_task() -> None:
    """Reads per-exchange caches whenever any exchange data is refreshed,
    computes spread pairs, and publishes results to Redis/SSE.

    Checks every second but only recomputes when exchange data has changed
    since the last aggregation run.

    asyncio is single-threaded (cooperative), so all three cache reads below
    happen without any await point in between — the exchange tasks cannot
    interleave here.  Each cache variable is replaced atomically (whole-dict
    assignment), so _run_aggregation receives consistent snapshots via its
    parameter bindings even across the internal await asyncio.sleep(0) yields.
    """
    last_agg_ts: float = 0.0
    while True:
        current_update = _LAST_EXCHANGE_UPDATE_TS
        if current_update > last_agg_ts and (_MEXC_DATA or _BYBIT_DATA or _BINGX_DATA):
            last_agg_ts = time.time()
            start = time.perf_counter()
            # Capture snapshots before the first await; parameter binding in
            # _run_aggregation protects against subsequent dict replacement.
            try:
                await _run_aggregation(_MEXC_DATA, _BYBIT_DATA, _BINGX_DATA)
                took = int((time.perf_counter() - start) * 1000)
                logger.info("[aggregator] spreads computed in %d ms", took)
            except Exception:
                logger.exception("[aggregator] task error")
        await asyncio.sleep(1.0)


async def _next_funding_task() -> None:
    """Refresh FUNDING_STORE from cached live rows every 10 minutes.

    Next funding timestamps change at most once per funding interval (typically
    every 8 hours), so there is no need to recompute them on every aggregation
    cycle.  This task runs independently at a much lower cadence to reduce load.

    An initial delay of 2×REFRESH_SEC lets the exchange tasks populate live rows
    before the first funding store rebuild.
    """
    await asyncio.sleep(float(CFG.get("refresh_sec", REFRESH_SEC)) * 2)
    while True:
        try:
            live = await _rlive_all()
            now_ms = int(time.time() * 1000)
            funding: Dict[str, int] = {}
            for r in live.values():
                for ex_key, ts_key in (("buy_ex", "buy_next_ts_ms"), ("sell_ex", "sell_next_ts_ms")):
                    ex = str(r.get(ex_key) or "").lower()
                    ts = int(r.get(ts_key) or 0)
                    if ex and ts > now_ms:
                        if ex not in funding or ts < funding[ex]:
                            funding[ex] = ts
            # Build the new dict fully before acquiring the lock so that the
            # store is never empty from a reader's perspective.
            with FUNDING_LOCK:
                FUNDING_STORE.clear()
                FUNDING_STORE.update(funding)
            logger.info("[next-funding] store updated: %d exchanges", len(funding))
        except Exception:
            logger.exception("[next-funding] task error")
        await asyncio.sleep(600)  # 10 minutes


async def updater_loop() -> None:
    """Start per-exchange fetchers and the aggregator as concurrent independent tasks."""
    await asyncio.gather(
        _mexc_task(),
        _bybit_task(),
        _bingx_task(),
        _aggregator_task(),
    )


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
    if RUN_UPDATER:
        logger.info("Collector mode (updater running)")
        asyncio.create_task(updater_loop())
        asyncio.create_task(_mexc_intervals_refresher())
    else:
        logger.info("API mode (no updater)")
    if _app_pkg._REDIS is not None and not RUN_UPDATER:
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
