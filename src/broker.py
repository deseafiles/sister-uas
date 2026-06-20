
from __future__ import annotations

import json
import logging
import os
from typing import Optional

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
QUEUE_KEY = "aggregator:event_queue"
RETRY_KEY = "aggregator:retry_queue"
DEAD_LETTER_KEY = "aggregator:dead_letter"
MAX_RETRY = 3


class RedisBroker:
    """
    Redis-backed async message broker.

    Menggunakan Redis LIST sebagai queue:
    - Publisher: LPUSH (push ke head)
    - Consumer:  BRPOP (blocking pop dari tail, FIFO order)

    At-least-once delivery: jika consumer crash sebelum commit ke Postgres,
    event bisa di-retry dari retry_queue.
    """

    def __init__(self, url: str = REDIS_URL):
        self._url = url
        self._client: Optional[aioredis.Redis] = None

    async def init(self) -> None:
        self._client = aioredis.from_url(self._url, decode_responses=True)
        await self._client.ping()
        logger.info("RedisBroker connected to %s", self._url)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            logger.info("RedisBroker disconnected.")

    async def publish(self, events: list[dict]) -> int:
        """
        Push events ke Redis queue. Return jumlah event yang di-push.
        Menggunakan pipeline untuk efisiensi batch.
        """
        if not events:
            return 0

        pipe = self._client.pipeline()
        for event in events:
            pipe.lpush(QUEUE_KEY, json.dumps(event))
        results = await pipe.execute()
        count = len(events)
        logger.debug("RedisBroker: pushed %d events to queue (queue_len=%s)", count, results[-1])
        return count

    async def consume(self, timeout: float = 1.0) -> Optional[dict]:
        """
        Blocking pop satu event dari queue.
        Return None jika timeout (tidak ada event).
        """
        result = await self._client.brpop(QUEUE_KEY, timeout=int(timeout))
        if result is None:
            return None
        _, raw = result
        return json.loads(raw)

    async def consume_batch(self, max_size: int = 10) -> list[dict]:
        """Non-blocking pop hingga max_size event."""
        pipe = self._client.pipeline()
        for _ in range(max_size):
            pipe.rpop(QUEUE_KEY)
        results = await pipe.execute()
        return [json.loads(r) for r in results if r is not None]

    async def push_retry(self, event: dict, retry_count: int = 0) -> None:
        """Push event ke retry queue dengan metadata retry."""
        event["_retry_count"] = retry_count + 1
        await self._client.lpush(RETRY_KEY, json.dumps(event))
        logger.warning(
            "Event pushed to retry queue: event_id=%s retry=%d",
            event.get("event_id"), retry_count + 1,
        )

    async def pop_retry(self) -> Optional[dict]:
        """Pop satu event dari retry queue."""
        result = await self._client.rpop(RETRY_KEY)
        if result is None:
            return None
        return json.loads(result)

    async def push_dead_letter(self, event: dict) -> None:
        """Event yang gagal melebihi MAX_RETRY → dead letter queue."""
        await self._client.lpush(DEAD_LETTER_KEY, json.dumps(event))
        logger.error(
            "Event moved to dead letter: event_id=%s", event.get("event_id")
        )

    async def queue_size(self) -> int:
        return await self._client.llen(QUEUE_KEY)

    async def retry_queue_size(self) -> int:
        return await self._client.llen(RETRY_KEY)

    async def dead_letter_size(self) -> int:
        return await self._client.llen(DEAD_LETTER_KEY)

    async def ping(self) -> bool:
        try:
            return await self._client.ping()
        except Exception:
            return False
