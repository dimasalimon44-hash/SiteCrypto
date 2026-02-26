#!/usr/bin/env python3
"""
Arbitrage Insights — Collector process
=======================================
Fetches exchange data (MEXC / Bybit / BingX) and writes pre-built
JSON snapshots to Redis.  The API process reads from Redis and never
calls exchange APIs directly.

Architecture:
  ┌──────────────────────────┐       Redis       ┌──────────────────────────┐
  │  collector.py            │ ─── arb:snap:* ──▶│  app.py (API)            │
  │  (per-exchange tasks +   │ ─── arb:live   ──▶│  RUN_UPDATER=0           │
  │   aggregator task)       │ ─── arb:sse    ──▶│  _redis_sse_subscriber() │
  └──────────────────────────┘                   └──────────────────────────┘

Setup (2 systemd services or 2 terminal windows):

  # Service 1 — Collector (exchange data, no HTTP server):
  REDIS_URL=redis://localhost:6379/0 python collector.py

  # Service 2 — API (HTTP server, reads from Redis):
  REDIS_URL=redis://localhost:6379/0 python app.py

Single-process mode (original behaviour, no change needed):
  RUN_UPDATER=1 python app.py    # starts per-exchange tasks AND HTTP server

Benefits of split mode:
  - Collector CPU usage (exchange fetch + spread compute) is completely
    isolated from HTTP request handling — login/logout are always instant
  - API workers can be scaled horizontally (all read from same Redis)
  - Collector can be restarted without disconnecting SSE clients
  - API can be restarted without interrupting data collection
"""

import asyncio
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# Make sure app.py module can be imported from the same directory
sys.path.insert(0, str(Path(__file__).parent))

# Ensure the API lifespan does NOT start the per-exchange tasks (RUN_UPDATER defaults to 0)
# The collector starts those tasks itself below via asyncio.create_task.
os.environ.setdefault("RUN_UPDATER", "0")

logger = logging.getLogger("collector")


async def main() -> None:
    import aiohttp  # noqa: PLC0415

    # Lazy import after env var is set so lifespan() is safe
    import app as _a  # noqa: PLC0415

    # ── Thread pool ──────────────────────────────────────────────────────
    cpu_count = os.cpu_count() or 2
    asyncio.get_running_loop().set_default_executor(
        ThreadPoolExecutor(max_workers=cpu_count * 2)
    )

    # ── Redis ────────────────────────────────────────────────────────────
    await _a._redis_connect()
    if _a._REDIS is None:
        logger.error(
            "Collector requires Redis. Set REDIS_URL env var, e.g.:\n"
            "  export REDIS_URL=redis://localhost:6379/0"
        )
        sys.exit(1)
    logger.info("Redis connected — collector starting")

    # ── Shared HTTP session ──────────────────────────────────────────────
    connector = aiohttp.TCPConnector(limit=60, ttl_dns_cache=300)
    _a._HTTP_SESSION = aiohttp.ClientSession(connector=connector)

    # ── Background tasks ─────────────────────────────────────────────────
    tasks = [
        asyncio.create_task(_a._mexc_task(), name="mexc"),
        asyncio.create_task(_a._bybit_task(), name="bybit"),
        asyncio.create_task(_a._bingx_task(), name="bingx"),
        asyncio.create_task(_a._aggregator_task(), name="aggregator"),
        asyncio.create_task(_a._mexc_intervals_refresher(), name="mexc-intervals"),
    ]
    logger.info("Collector tasks started: %s", [t.get_name() for t in tasks])

    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Collector shutting down…")
    finally:
        for t in tasks:
            t.cancel()
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception:
            pass
        if _a._HTTP_SESSION and not _a._HTTP_SESSION.closed:
            await _a._HTTP_SESSION.close()
        await _a._redis_disconnect()
        logger.info("Collector stopped.")


if __name__ == "__main__":
    _log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, _log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Suppress duplicate log lines from uvicorn (it has its own handlers)
    for _uv in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(_uv).propagate = False
    try:
        import uvloop  # noqa: F401
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        logger.info("Using uvloop event loop")
    except ImportError:
        pass
    asyncio.run(main())
