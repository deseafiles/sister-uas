from __future__ import annotations

import asyncio
import json
import os
import tempfile
import uuid
from datetime import datetime, UTC
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

# ── SQLite-backed DedupStore untuk testing ────────────────────────────────────
# Override env sebelum import src
_tmp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DEDUP_DB_PATH"] = _tmp_db.name
os.environ["POSTGRES_DSN"] = "sqlite+aiosqlite:///:memory:"
os.environ["REDIS_URL"] = "redis://localhost:6379/0"
os.environ["WORKER_COUNT"] = "2"


# ── Import setelah env override ───────────────────────────────────────────────
from src.models import Event


# ── In-memory DedupStore untuk unit tests ────────────────────────────────────

class InMemoryDedupStore:
    """
    SQLite-based in-memory dedup store untuk testing.
    Menggantikan PostgreSQL store dengan perilaku identik.
    """

    def __init__(self):
        import aiosqlite
        self._aiosqlite = aiosqlite
        self._db_path = ":memory:"
        self._db = None
        self._lock = asyncio.Lock()
        self._latency_samples = []

    async def init(self):
        import aiosqlite
        self._db = await aiosqlite.connect(self._db_path)
        await self._db.execute("PRAGMA journal_mode=WAL;")
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS processed_events (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                topic        TEXT NOT NULL,
                event_id     TEXT NOT NULL,
                source       TEXT NOT NULL,
                payload      TEXT NOT NULL DEFAULT '{}',
                timestamp    TEXT NOT NULL,
                processed_at TEXT NOT NULL,
                CONSTRAINT uq_topic_event_id UNIQUE (topic, event_id)
            )
        """)
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                key   TEXT PRIMARY KEY,
                value INTEGER NOT NULL DEFAULT 0
            )
        """)
        for key in ("received", "unique_processed", "duplicate_dropped"):
            await self._db.execute(
                "INSERT OR IGNORE INTO stats (key, value) VALUES (?, 0)", (key,)
            )
        await self._db.commit()

    async def close(self):
        if self._db:
            await self._db.close()

    async def mark_processed(self, topic, event_id, source, payload, timestamp) -> bool:
        import aiosqlite
        async with self._lock:
            try:
                payload_str = json.dumps(payload) if isinstance(payload, dict) else payload
                await self._db.execute(
                    """
                    INSERT INTO processed_events
                        (topic, event_id, source, payload, timestamp, processed_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (topic, event_id, source, payload_str, timestamp,
                     datetime.now(UTC).isoformat()),
                )
                await self._db.execute(
                    "UPDATE stats SET value = value + 1 WHERE key = 'unique_processed'"
                )
                await self._db.commit()
                return True
            except aiosqlite.IntegrityError:
                await self._db.execute(
                    "UPDATE stats SET value = value + 1 WHERE key = 'duplicate_dropped'"
                )
                await self._db.commit()
                return False

    async def increment_received(self, count: int = 1):
        async with self._lock:
            await self._db.execute(
                "UPDATE stats SET value = value + ? WHERE key = 'received'", (count,)
            )
            await self._db.commit()

    async def is_duplicate(self, topic: str, event_id: str) -> bool:
        async with self._lock:
            async with self._db.execute(
                "SELECT 1 FROM processed_events WHERE topic=? AND event_id=?",
                (topic, event_id),
            ) as cur:
                row = await cur.fetchone()
        return row is not None

    async def get_stats(self) -> dict:
        async with self._lock:
            async with self._db.execute("SELECT key, value FROM stats") as cur:
                rows = await cur.fetchall()
        return {r[0]: r[1] for r in rows}

    async def get_events(self, topic=None) -> list[dict]:
        query = "SELECT topic, event_id, source, payload, timestamp, processed_at FROM processed_events"
        params = ()
        if topic:
            query += " WHERE topic = ?"
            params = (topic,)
        query += " ORDER BY processed_at DESC"
        async with self._lock:
            async with self._db.execute(query, params) as cur:
                rows = await cur.fetchall()
        return [
            {
                "topic": r[0], "event_id": r[1], "source": r[2],
                "payload": json.loads(r[3]) if r[3] else {},
                "timestamp": r[4], "processed_at": r[5],
            }
            for r in rows
        ]

    async def get_topics(self) -> list[str]:
        async with self._lock:
            async with self._db.execute(
                "SELECT DISTINCT topic FROM processed_events ORDER BY topic"
            ) as cur:
                rows = await cur.fetchall()
        return [r[0] for r in rows]

    async def get_avg_latency_ms(self) -> float:
        return 1.5  # mock value

    async def process_with_explicit_transaction(self, events: list[dict]) -> tuple[int, int]:
        import aiosqlite
        inserted = 0
        duplicates = 0
        async with self._lock:
            for ev in events:
                try:
                    payload_str = json.dumps(ev.get("payload", {}))
                    await self._db.execute(
                        """
                        INSERT INTO processed_events
                            (topic, event_id, source, payload, timestamp, processed_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (ev["topic"], ev["event_id"], ev["source"],
                         payload_str, ev.get("timestamp", datetime.now(UTC).isoformat()),
                         datetime.now(UTC).isoformat()),
                    )
                    inserted += 1
                except aiosqlite.IntegrityError:
                    duplicates += 1
            if inserted > 0:
                await self._db.execute(
                    "UPDATE stats SET value = value + ? WHERE key = 'unique_processed'", (inserted,)
                )
            if duplicates > 0:
                await self._db.execute(
                    "UPDATE stats SET value = value + ? WHERE key = 'duplicate_dropped'", (duplicates,)
                )
            await self._db.commit()
        return inserted, duplicates


# ── In-memory Redis Broker Mock ───────────────────────────────────────────────

class InMemoryBroker:
    """Mock broker menggunakan asyncio.Queue sebagai pengganti Redis."""

    def __init__(self):
        self._queue = asyncio.Queue()
        self._retry_queue = asyncio.Queue()
        self._dead_letter = []

    async def init(self): pass
    async def close(self): pass

    async def publish(self, events: list[dict]) -> int:
        for ev in events:
            await self._queue.put(ev)
        return len(events)

    async def consume(self, timeout: float = 1.0):
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    async def consume_batch(self, max_size: int = 10) -> list[dict]:
        items = []
        for _ in range(max_size):
            try:
                items.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return items

    async def push_retry(self, event: dict, retry_count: int = 0):
        event["_retry_count"] = retry_count + 1
        await self._retry_queue.put(event)

    async def pop_retry(self):
        try:
            return self._retry_queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    async def push_dead_letter(self, event: dict):
        self._dead_letter.append(event)

    async def queue_size(self) -> int:
        return self._queue.qsize()

    async def retry_queue_size(self) -> int:
        return self._retry_queue.qsize()

    async def dead_letter_size(self) -> int:
        return len(self._dead_letter)

    async def ping(self) -> bool:
        return True


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def store() -> AsyncGenerator[InMemoryDedupStore, None]:
    """Fresh in-memory store for each test."""
    s = InMemoryDedupStore()
    await s.init()
    yield s
    await s.close()


@pytest_asyncio.fixture
async def broker() -> AsyncGenerator[InMemoryBroker, None]:
    """Fresh in-memory broker for each test."""
    b = InMemoryBroker()
    await b.init()
    yield b


@pytest_asyncio.fixture
async def persistent_store(tmp_path) -> AsyncGenerator[InMemoryDedupStore, None]:
    """Store dengan path file nyata untuk persistence tests."""
    import aiosqlite
    db_path = str(tmp_path / "persist_test.db")

    class FileDedupStore(InMemoryDedupStore):
        def __init__(self, path):
            super().__init__()
            self._db_path = path

    s = FileDedupStore(db_path)
    await s.init()
    yield s
    await s.close()


def make_event(
    topic: str = "test.topic",
    event_id: str | None = None,
    source: str = "test-publisher",
) -> dict:
    """Helper untuk membuat event dict."""
    return {
        "topic": topic,
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": datetime.now(UTC).isoformat(),
        "source": source,
        "payload": {"msg": "test event", "val": 42},
    }
