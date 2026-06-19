"""
dedup_store.py — PostgreSQL-based Idempotent Deduplication Store.

=== ISOLATION LEVEL: READ COMMITTED ===

Kami memilih READ COMMITTED (default PostgreSQL) dengan alasan:

1. ATOMICITY via unique constraint + INSERT ... ON CONFLICT DO NOTHING:
   Bahkan pada READ COMMITTED, operasi INSERT tunggal bersifat atomik di
   PostgreSQL. Jika dua worker concurrently mencoba INSERT event yang sama,
   hanya satu yang berhasil — yang lain mendapat ON CONFLICT (bukan error).
   Ini cukup untuk menjamin exactly-once processing.

2. Tidak butuh SERIALIZABLE:
   SERIALIZABLE mencegah phantom reads dan write skew, namun overhead-nya
   signifikan (SSI bookkeeping, potential serialization failures + retry).
   Untuk deduplication sederhana berbasis PK unik, READ COMMITTED sudah aman
   karena constraint enforcement terjadi di storage layer, bukan di MVCC.

3. Trade-off:
   - READ COMMITTED: throughput tinggi, latensi rendah, occasional re-read
     (nilai yang dibaca bisa berubah dalam transaksi panjang).
   - REPEATABLE READ: aman dari non-repeatable reads, overhead moderat.
   - SERIALIZABLE: aman penuh, overhead tinggi, cocok untuk financial txns.

Untuk log aggregator dengan volume tinggi, READ COMMITTED adalah pilihan tepat.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, UTC
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)

# ── DSN dari environment ──────────────────────────────────────────────────────
PG_DSN = os.environ.get(
    "POSTGRES_DSN",
    "postgresql://aggregator:secret@localhost:5432/aggregator_db",
)
WORKER_COUNT = int(os.environ.get("WORKER_COUNT", "3"))


class DedupStore:
    """
    Persistent deduplication store menggunakan PostgreSQL.

    Strategi dedup:
        INSERT INTO processed_events ... ON CONFLICT (topic, event_id) DO NOTHING
        → Jika INSERT berhasil  → event baru (unique_processed += 1)
        → Jika INSERT di-skip   → duplikat  (duplicate_dropped += 1)

    Isolation level READ COMMITTED dipilih secara eksplisit di setiap transaksi.
    """

    def __init__(self, dsn: str = PG_DSN):
        self._dsn = dsn
        self._pool: Optional[asyncpg.Pool] = None
        self._start_time = time.time()
        self._latency_samples: list[float] = []
        self._lock = asyncio.Lock()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def init(self) -> None:
        """Buat connection pool dan tabel jika belum ada."""
        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=2,
            max_size=10,
            command_timeout=30,
        )
        await self._create_schema()
        logger.info("DedupStore (PostgreSQL) initialized. DSN=%s", self._dsn)

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            logger.info("DedupStore pool closed.")

    async def _create_schema(self) -> None:
        """Buat tabel schema via asyncpg (DDL idempotent dengan IF NOT EXISTS)."""
        async with self._pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_events (
                    id            BIGSERIAL PRIMARY KEY,
                    topic         TEXT        NOT NULL,
                    event_id      TEXT        NOT NULL,
                    source        TEXT        NOT NULL,
                    payload       JSONB       NOT NULL DEFAULT '{}',
                    timestamp     TIMESTAMPTZ NOT NULL,
                    processed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT uq_topic_event_id UNIQUE (topic, event_id)
                );

                CREATE INDEX IF NOT EXISTS idx_events_topic
                    ON processed_events (topic, processed_at DESC);

                CREATE TABLE IF NOT EXISTS stats (
                    key    TEXT    PRIMARY KEY,
                    value  BIGINT  NOT NULL DEFAULT 0
                );

                INSERT INTO stats (key, value) VALUES
                    ('received',          0),
                    ('unique_processed',  0),
                    ('duplicate_dropped', 0)
                ON CONFLICT (key) DO NOTHING;

                CREATE TABLE IF NOT EXISTS throughput_log (
                    logged_at          TIMESTAMPTZ PRIMARY KEY DEFAULT NOW(),
                    events_per_second  NUMERIC(10,2),
                    batch_size         INT,
                    latency_ms         NUMERIC(10,2)
                );
            """)

    # ── Core Dedup Operations ─────────────────────────────────────────────────

    async def mark_processed(
        self,
        topic: str,
        event_id: str,
        source: str,
        payload: dict,
        timestamp: str,
    ) -> bool:
        """
        Coba INSERT event. Kembalikan True jika baru, False jika duplikat.

        Menggunakan INSERT ... ON CONFLICT DO NOTHING yang atomik di level
        storage engine. Dua worker yang mengirim event sama secara concurrent
        hanya akan menghasilkan satu INSERT berhasil.

        Isolation level: READ COMMITTED (eksplisit).
        """
        t0 = time.perf_counter()
        async with self._pool.acquire() as conn:
            # Eksplisit set isolation level READ COMMITTED
            async with conn.transaction(isolation="read_committed"):
                result = await conn.fetchval(
                    """
                    WITH ins AS (
                        INSERT INTO processed_events
                            (topic, event_id, source, payload, timestamp, processed_at)
                        VALUES ($1, $2, $3, $4::jsonb, $5::timestamptz, NOW())
                        ON CONFLICT (topic, event_id) DO NOTHING
                        RETURNING 1
                    )
                    SELECT COUNT(*) FROM ins
                    """,
                    topic,
                    event_id,
                    source,
                    json.dumps(payload),
                    timestamp,
                )

                is_new = (result == 1)

                if is_new:
                    await conn.execute(
                        "UPDATE stats SET value = value + 1 WHERE key = 'unique_processed'"
                    )
                    logger.info(
                        "PROCESSED: topic=%s event_id=%s source=%s",
                        topic, event_id, source,
                    )
                else:
                    await conn.execute(
                        "UPDATE stats SET value = value + 1 WHERE key = 'duplicate_dropped'"
                    )
                    logger.warning(
                        "DUPLICATE DETECTED & DROPPED: topic=%s event_id=%s",
                        topic, event_id,
                    )

        # Record latency sample
        latency_ms = (time.perf_counter() - t0) * 1000
        async with self._lock:
            self._latency_samples.append(latency_ms)
            if len(self._latency_samples) > 1000:
                self._latency_samples = self._latency_samples[-1000:]

        return is_new

    async def increment_received(self, count: int = 1) -> None:
        """Increment counter 'received' secara atomik."""
        async with self._pool.acquire() as conn:
            async with conn.transaction(isolation="read_committed"):
                await conn.execute(
                    "UPDATE stats SET value = value + $1 WHERE key = 'received'",
                    count,
                )

    async def is_duplicate(self, topic: str, event_id: str) -> bool:
        """Cek apakah event sudah diproses (read-only, tanpa transaksi eksplisit)."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchval(
                "SELECT 1 FROM processed_events WHERE topic=$1 AND event_id=$2",
                topic, event_id,
            )
        return row is not None

    # ── Query Operations ──────────────────────────────────────────────────────

    async def get_stats(self) -> dict[str, int]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT key, value FROM stats")
        return {r["key"]: r["value"] for r in rows}

    async def get_events(self, topic: str | None = None) -> list[dict]:
        query = """
            SELECT topic, event_id, source, payload, timestamp, processed_at
            FROM processed_events
        """
        params: tuple = ()
        if topic:
            query += " WHERE topic = $1"
            params = (topic,)
        query += " ORDER BY processed_at DESC LIMIT 500"

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        return [
            {
                "topic": r["topic"],
                "event_id": r["event_id"],
                "source": r["source"],
                "payload": dict(r["payload"]) if r["payload"] else {},
                "timestamp": r["timestamp"].isoformat() if r["timestamp"] else "",
                "processed_at": r["processed_at"].isoformat() if r["processed_at"] else "",
            }
            for r in rows
        ]

    async def get_topics(self) -> list[str]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT DISTINCT topic FROM processed_events ORDER BY topic"
            )
        return [r["topic"] for r in rows]

    async def get_avg_latency_ms(self) -> float:
        async with self._lock:
            if not self._latency_samples:
                return 0.0
            return round(sum(self._latency_samples) / len(self._latency_samples), 2)

    async def log_throughput(
        self, events_per_second: float, batch_size: int, latency_ms: float
    ) -> None:
        """Simpan snapshot throughput untuk observability."""
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO throughput_log (events_per_second, batch_size, latency_ms)
                    VALUES ($1, $2, $3)
                    """,
                    events_per_second, batch_size, latency_ms,
                )
        except Exception:
            pass  # non-critical

    # ── Transaction Demonstration ─────────────────────────────────────────────

    async def process_with_explicit_transaction(
        self, events: list[dict]
    ) -> tuple[int, int]:
        """
        Proses batch event dalam satu transaksi eksplisit.

        Digunakan untuk demonstrasi transaction commit/rollback dalam tests.
        Return: (inserted_count, duplicate_count)
        """
        inserted = 0
        duplicates = 0

        async with self._pool.acquire() as conn:
            async with conn.transaction(isolation="read_committed"):
                for ev in events:
                    result = await conn.fetchval(
                        """
                        WITH ins AS (
                            INSERT INTO processed_events
                                (topic, event_id, source, payload, timestamp, processed_at)
                            VALUES ($1, $2, $3, $4::jsonb, $5::timestamptz, NOW())
                            ON CONFLICT (topic, event_id) DO NOTHING
                            RETURNING 1
                        )
                        SELECT COUNT(*) FROM ins
                        """,
                        ev["topic"],
                        ev["event_id"],
                        ev["source"],
                        json.dumps(ev.get("payload", {})),
                        ev.get("timestamp", datetime.now(UTC).isoformat()),
                    )
                    if result == 1:
                        inserted += 1
                    else:
                        duplicates += 1

                # Update stats dalam transaksi yang sama
                if inserted > 0:
                    await conn.execute(
                        "UPDATE stats SET value = value + $1 WHERE key = 'unique_processed'",
                        inserted,
                    )
                if duplicates > 0:
                    await conn.execute(
                        "UPDATE stats SET value = value + $1 WHERE key = 'duplicate_dropped'",
                        duplicates,
                    )

        return inserted, duplicates
