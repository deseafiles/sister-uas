"""
consumer.py — Multi-Worker Idempotent Async Consumer.

Mendukung multiple worker coroutine yang berkompetisi mengonsumsi event
dari Redis broker dan memproses ke PostgreSQL dengan idempotency guarantee.

Race condition prevention:
    Dua worker yang memproses event_id yang sama secara concurrent
    → keduanya mencoba INSERT ... ON CONFLICT DO NOTHING ke PostgreSQL
    → hanya satu INSERT berhasil (unique constraint enforcement)
    → worker kedua mendapat ON CONFLICT (bukan error), return False
    → exactly-once processing terjamin

Ini adalah contoh konkret dari "idempotent receiver" pattern dari
Tanenbaum & Van Steen, Distributed Systems, Bab 8 (Consistency & Replication).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, UTC

from .broker import RedisBroker, MAX_RETRY
from .dedup_store import DedupStore
from .models import Event

logger = logging.getLogger(__name__)

# Metrik throughput global (diakses oleh /stats)
_processed_count = 0
_start_ts = time.time()


def get_throughput() -> float:
    elapsed = time.time() - _start_ts
    return round(_processed_count / elapsed, 2) if elapsed > 0 else 0.0


class EventConsumer:
    """
    Idempotent consumer dengan dukungan multi-worker.

    Setiap worker adalah asyncio Task yang berjalan concurrently.
    Semua worker berkompetisi membaca dari Redis queue yang sama
    (fan-out pattern — setiap event dikonsumsi tepat satu worker).
    """

    def __init__(
        self,
        broker: RedisBroker,
        store: DedupStore,
        worker_count: int = 3,
    ):
        self._broker = broker
        self._store = store
        self._worker_count = worker_count
        self._running = False
        self._tasks: list[asyncio.Task] = []
        self._processed = 0
        self._dropped = 0
        self._errors = 0

    @property
    def worker_count(self) -> int:
        return self._worker_count

    async def start(self) -> None:
        self._running = True
        global _start_ts
        _start_ts = time.time()

        for i in range(self._worker_count):
            task = asyncio.create_task(
                self._worker_loop(worker_id=i),
                name=f"consumer-worker-{i}",
            )
            self._tasks.append(task)

        # Juga jalankan retry worker
        retry_task = asyncio.create_task(
            self._retry_loop(), name="consumer-retry"
        )
        self._tasks.append(retry_task)

        logger.info(
            "EventConsumer started with %d workers + 1 retry worker.",
            self._worker_count,
        )

    async def stop(self) -> None:
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        logger.info("EventConsumer stopped. processed=%d dropped=%d errors=%d",
                    self._processed, self._dropped, self._errors)

    async def _worker_loop(self, worker_id: int) -> None:
        """Main consume loop untuk satu worker."""
        logger.info("Worker-%d started.", worker_id)
        while self._running:
            try:
                event_data = await self._broker.consume(timeout=1.0)
                if event_data is None:
                    continue
                await self._process(event_data, worker_id)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._errors += 1
                logger.error("Worker-%d error: %s", worker_id, exc, exc_info=True)
        logger.info("Worker-%d stopped.", worker_id)

    async def _retry_loop(self) -> None:
        """Retry worker — memproses ulang event dari retry queue."""
        while self._running:
            try:
                event_data = await self._broker.pop_retry()
                if event_data is None:
                    await asyncio.sleep(5.0)
                    continue
                retry_count = event_data.pop("_retry_count", 1)
                logger.info("Retrying event_id=%s (attempt %d)", event_data.get("event_id"), retry_count)
                await self._process(event_data, worker_id=-1, retry_count=retry_count)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Retry worker error: %s", exc, exc_info=True)

    async def _process(self, event_data: dict, worker_id: int, retry_count: int = 0) -> None:
        """
        Proses satu event: validasi → dedup store → log.

        Idempotency dijamin oleh DedupStore.mark_processed() yang menggunakan
        INSERT ... ON CONFLICT DO NOTHING (atomik di PostgreSQL).
        """
        global _processed_count

        try:
            # Parse & validate via Pydantic
            event = Event(**event_data)

            topic = event.topic
            event_id = event.event_id
            timestamp = (
                event.timestamp.isoformat()
                if isinstance(event.timestamp, datetime)
                else str(event.timestamp)
            )

            is_new = await self._store.mark_processed(
                topic=topic,
                event_id=event_id,
                source=event.source,
                payload=event.payload,
                timestamp=timestamp,
            )

            if is_new:
                self._processed += 1
                _processed_count += 1
                logger.info(
                    "[Worker-%d] PROCESSED: topic=%s event_id=%s",
                    worker_id, topic, event_id,
                )
            else:
                self._dropped += 1
                logger.warning(
                    "[Worker-%d] DUPLICATE DROPPED: topic=%s event_id=%s",
                    worker_id, topic, event_id,
                )

        except Exception as exc:
            self._errors += 1
            logger.error(
                "[Worker-%d] Failed to process event: %s — %s",
                worker_id, event_data.get("event_id", "?"), exc,
            )
            # At-least-once: push ke retry queue jika belum melebihi batas
            if retry_count < MAX_RETRY:
                await self._broker.push_retry(event_data, retry_count)
            else:
                await self._broker.push_dead_letter(event_data)

    def get_stats(self) -> dict:
        return {
            "processed": self._processed,
            "dropped": self._dropped,
            "errors": self._errors,
            "throughput_eps": get_throughput(),
        }
