"""Health-check route."""
import time

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.store import _DATA_CACHE

router = APIRouter()

_START_TIME = time.time()


@router.get("/health")
async def health():
    """Health-check endpoint for nginx/systemd/uptime monitors."""
    snap = _DATA_CACHE.get("guest")
    return JSONResponse({
        "ok": True,
        "uptime_s": int(time.time() - _START_TIME),
        "snapshot_ready": snap is not None,
    })
