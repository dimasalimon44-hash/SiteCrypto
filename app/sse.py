"""SSE (Server-Sent Events): queue management, broadcast, and Redis subscriber."""
import asyncio
import json
import logging
from typing import List

logger = logging.getLogger("arb_dashboard")

# List of active SSE client queues — one queue per connected browser tab
_SSE_QUEUES: List[asyncio.Queue] = []


def _get_redis():
    import app as _app_pkg
    return _app_pkg._REDIS


def _broadcast_sse(payload: str) -> None:
    """Push a message to every connected SSE client (fire-and-forget).

    When Redis is configured the message is also published to the
    ``arb:sse`` pub/sub channel so workers that have no local SSE
    subscriber still deliver the update to their clients via
    ``_redis_sse_subscriber``.
    """
    from app.store import _REDIS_CHANNEL_SSE
    for q in list(_SSE_QUEUES):
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            pass
    redis = _get_redis()
    if redis is not None:
        try:
            asyncio.get_running_loop().create_task(
                redis.publish(_REDIS_CHANNEL_SSE, payload)
            )
        except RuntimeError:
            pass


async def _redis_sse_subscriber() -> None:
    """Subscribe to the Redis pub/sub SSE channel and forward messages
    to all in-process SSE clients.  Runs as a background task when Redis
    is available.  If the connection drops it retries after 5 seconds."""
    import os
    from app.store import _REDIS_CHANNEL_SSE
    redis = _get_redis()
    if redis is None:
        return
    url = os.environ.get("REDIS_URL", "").strip()
    if not url:
        return
    while True:
        try:
            import redis.asyncio as aioredis  # type: ignore[import]
            sub_client = aioredis.from_url(url, encoding="utf-8", decode_responses=True)
            pubsub = sub_client.pubsub()
            await pubsub.subscribe(_REDIS_CHANNEL_SSE)
            async for message in pubsub.listen():
                if message and message.get("type") == "message":
                    data = message.get("data", "")
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
