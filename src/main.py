"""
main.py — FastAPI Application untuk Pub-Sub Log Aggregator.

Endpoint:
  POST /publish  — Publish batch/single event ke Redis broker
  GET  /events   — Daftar event unik yang sudah diproses (dari PostgreSQL)
  GET  /stats    — Statistik sistem lengkap
  GET  /health   — Health check (Postgres + Redis connectivity)

Arsitektur:
  Publisher → POST /publish → Redis Queue → Consumer Workers → PostgreSQL
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from .broker import RedisBroker
from .consumer import EventConsumer
from .dedup_store import DedupStore
from .models import (
    EventResponse,
    HealthResponse,
    PublishRequest,
    PublishResponse,
    StatsResponse,
)

# ── Structured Logging ────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s %(funcName)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

WORKER_COUNT = int(os.environ.get("WORKER_COUNT", "3"))

# ── Global singletons ─────────────────────────────────────────────────────────
_store: DedupStore | None = None
_broker: RedisBroker | None = None
_consumer: EventConsumer | None = None
_start_time: float = 0.0


# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _store, _broker, _consumer, _start_time

    _start_time = time.time()

    # Init PostgreSQL store
    _store = DedupStore()
    await _store.init()

    # Init Redis broker
    _broker = RedisBroker()
    await _broker.init()

    # Init & start multi-worker consumer
    _consumer = EventConsumer(
        broker=_broker,
        store=_store,
        worker_count=WORKER_COUNT,
    )
    await _consumer.start()

    logger.info(
        "Aggregator service started. Workers=%d Postgres=ready Redis=ready",
        WORKER_COUNT,
    )
    yield

    # Graceful shutdown
    await _consumer.stop()
    await _store.close()
    await _broker.close()
    logger.info("Aggregator service shut down cleanly.")


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Pub-Sub Log Aggregator",
    description=(
        "Layanan aggregator log berbasis Pub-Sub dengan idempotent consumer, "
        "Redis broker, PostgreSQL persistent storage, dan multi-worker consumer. "
        "Menjamin exactly-once processing meskipun publisher mengirim duplikat "
        "(at-least-once delivery semantics)."
    ),
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _get_components() -> tuple[RedisBroker, DedupStore]:
    if _broker is None or _store is None:
        raise HTTPException(status_code=503, detail="Service not ready. Try again shortly.")
    return _broker, _store


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.post(
    "/publish",
    response_model=PublishResponse,
    status_code=202,
    summary="Publish event(s) ke aggregator",
    description=(
        "Terima satu atau banyak event sekaligus. "
        "Event langsung di-enqueue ke Redis dan diproses secara async. "
        "Duplikat akan dideteksi dan di-drop oleh consumer."
    ),
)
async def publish(body: PublishRequest) -> PublishResponse:
    broker, store = _get_components()

    events_data = []
    for event in body.events:
        events_data.append({
            "topic": event.topic,
            "event_id": event.event_id,
            "timestamp": event.timestamp.isoformat(),
            "source": event.source,
            "payload": event.payload,
        })

    count = len(events_data)
    await broker.publish(events_data)
    await store.increment_received(count)

    logger.info("RECEIVED & ENQUEUED: %d event(s)", count)
    return PublishResponse(
        status="accepted",
        enqueued=count,
        message=f"{count} event(s) diterima dan sedang diproses oleh {WORKER_COUNT} worker.",
    )


@app.get(
    "/events",
    response_model=list[EventResponse],
    summary="Ambil daftar event unik yang sudah diproses",
    description="Hanya event yang berhasil disimpan ke PostgreSQL (unique) yang ditampilkan.",
)
async def get_events(
    topic: str | None = Query(None, description="Filter berdasarkan nama topic"),
) -> list[EventResponse]:
    _, store = _get_components()
    # Sedikit delay agar consumer sempat memproses event yang baru di-publish
    await asyncio.sleep(0.05)
    events = await store.get_events(topic=topic)
    return [EventResponse(**e) for e in events]


@app.get(
    "/stats",
    response_model=StatsResponse,
    summary="Statistik sistem aggregator",
    description=(
        "Menampilkan: received, unique_processed, duplicate_dropped, "
        "topics, uptime, queue_size, throughput (events/s), avg latency (ms), worker_count."
    ),
)
async def get_stats() -> StatsResponse:
    broker, store = _get_components()

    raw = await store.get_stats()
    topics = await store.get_topics()
    uptime = time.time() - _start_time
    queue_size = await broker.queue_size()
    avg_latency = await store.get_avg_latency_ms()
    consumer_stats = _consumer.get_stats() if _consumer else {}

    return StatsResponse(
        received=raw.get("received", 0),
        unique_processed=raw.get("unique_processed", 0),
        duplicate_dropped=raw.get("duplicate_dropped", 0),
        topics=topics,
        uptime_seconds=round(uptime, 2),
        queue_size=queue_size,
        events_per_second=consumer_stats.get("throughput_eps", 0.0),
        avg_latency_ms=avg_latency,
        worker_count=WORKER_COUNT,
    )


@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check — Postgres + Redis connectivity",
    include_in_schema=False,
)
async def health() -> HealthResponse:
    broker, store = _get_components()

    # Check Postgres
    pg_status = "ok"
    try:
        await store.get_stats()
    except Exception as e:
        pg_status = f"error: {e}"

    # Check Redis
    redis_status = "ok" if await broker.ping() else "error"

    queue_size = await broker.queue_size()

    return HealthResponse(
        status="ok" if pg_status == "ok" and redis_status == "ok" else "degraded",
        uptime_seconds=round(time.time() - _start_time, 2),
        postgres=pg_status,
        redis=redis_status,
        queue_size=queue_size,
    )
