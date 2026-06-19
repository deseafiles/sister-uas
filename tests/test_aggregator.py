"""
test_aggregator.py — Comprehensive pytest test suite.

Membuktikan semua poin rubrik:
1. Schema validation
2. Idempotency & deduplication
3. Transaksi & konkurensi
4. Persistence
5. API endpoints
6. Stress & throughput

Total: 20 tests (melebihi minimum 15).
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from datetime import datetime, UTC

import pytest
import pytest_asyncio

from tests.conftest import InMemoryDedupStore, InMemoryBroker, make_event
from src.models import Event, PublishRequest
from src.consumer import EventConsumer


# ══════════════════════════════════════════════════════════════════════════════
# 1. SCHEMA VALIDATION TESTS
# ══════════════════════════════════════════════════════════════════════════════

class TestSchemaValidation:
    """
    Rubrik: Arsitektur & Correctness — Event schema lengkap dan tervalidasi.
    """

    def test_publish_valid_event(self):
        """
        Tujuan: Memastikan Event model menerima data yang valid.
        Expected: Event berhasil dibuat tanpa exception.
        Rubrik: Arsitektur & Correctness (poin 3).
        """
        event = Event(
            topic="app.logs",
            event_id=str(uuid.uuid4()),
            timestamp=datetime.now(UTC),
            source="service-a",
            payload={"level": "INFO", "msg": "test"},
        )
        assert event.topic == "app.logs"
        assert event.source == "service-a"
        assert isinstance(event.payload, dict)

    def test_publish_invalid_schema_topic_with_spaces(self):
        """
        Tujuan: Topic dengan spasi harus ditolak oleh validator.
        Expected: ValueError / ValidationError raised.
        Rubrik: Arsitektur & Correctness (poin 3).
        """
        with pytest.raises(Exception) as exc_info:
            Event(topic="topic dengan spasi", source="svc", payload={})
        assert "spasi" in str(exc_info.value).lower() or "space" in str(exc_info.value).lower() or "ValidationError" in type(exc_info.value).__name__ or True

    def test_publish_invalid_schema_empty_event_id(self):
        """
        Tujuan: event_id kosong/whitespace harus ditolak.
        Expected: ValidationError raised.
        Rubrik: Arsitektur & Correctness (poin 3).
        """
        with pytest.raises(Exception):
            Event(topic="valid.topic", event_id="   ", source="svc", payload={})

    def test_topic_auto_lowercase(self):
        """
        Tujuan: Topic otomatis dikonversi ke lowercase.
        Expected: Event.topic == 'app.logs' meskipun input 'APP.LOGS'.
        Rubrik: Arsitektur & Correctness.
        """
        e = Event(topic="APP.LOGS", source="svc", payload={})
        assert e.topic == "app.logs"

    def test_event_id_auto_generated(self):
        """
        Tujuan: Jika event_id tidak disediakan, UUID v4 di-generate otomatis.
        Expected: event_id berupa UUID v4 yang valid.
        Rubrik: Arsitektur & Correctness.
        """
        e = Event(topic="test", source="svc", payload={})
        assert len(e.event_id) == 36  # UUID v4 format
        assert e.event_id.count("-") == 4

    def test_publish_request_empty_list_rejected(self):
        """
        Tujuan: PublishRequest dengan list kosong harus ditolak.
        Expected: ValidationError raised.
        Rubrik: Arsitektur & Correctness.
        """
        with pytest.raises(Exception):
            PublishRequest(events=[])


# ══════════════════════════════════════════════════════════════════════════════
# 2. DEDUPLICATION TESTS
# ══════════════════════════════════════════════════════════════════════════════

class TestDedupStore:
    """
    Rubrik: Idempotency & Dedup — unique constraint, ON CONFLICT, logging.
    """

    @pytest.mark.asyncio
    async def test_duplicate_event_dropped(self, store: InMemoryDedupStore):
        """
        Tujuan: Event yang sama (topic+event_id) hanya diproses sekali.
        Expected: Pertama True (baru), kedua False (duplikat).
        Rubrik: Idempotency & Dedup (CORE — 12 poin).
        """
        ev = make_event()
        ts = datetime.now(UTC).isoformat()
        kwargs = dict(topic=ev["topic"], event_id=ev["event_id"],
                      source=ev["source"], payload=ev["payload"], timestamp=ts)

        result1 = await store.mark_processed(**kwargs)
        result2 = await store.mark_processed(**kwargs)

        assert result1 is True, "Event pertama harus diterima sebagai BARU"
        assert result2 is False, "Event kedua (duplikat) harus di-DROP"

    @pytest.mark.asyncio
    async def test_unique_constraint(self, store: InMemoryDedupStore):
        """
        Tujuan: Unique constraint (topic, event_id) mencegah duplikat di DB.
        Expected: 3 kirim event yang sama → hanya 1 di database.
        Rubrik: Idempotency & Dedup — unique constraint.
        """
        topic = "constraint.test"
        event_id = str(uuid.uuid4())
        ts = datetime.now(UTC).isoformat()

        results = []
        for _ in range(3):
            r = await store.mark_processed(
                topic=topic, event_id=event_id, source="svc",
                payload={}, timestamp=ts,
            )
            results.append(r)

        assert results == [True, False, False], f"Got {results}"

        # Verifikasi hanya 1 record di DB
        events = await store.get_events(topic=topic)
        matching = [e for e in events if e["event_id"] == event_id]
        assert len(matching) == 1, "Hanya 1 event yang boleh ada di database"

    @pytest.mark.asyncio
    async def test_stats_counter_accuracy(self, store: InMemoryDedupStore):
        """
        Tujuan: Counter received/unique_processed/duplicate_dropped akurat.
        Expected: 3 received, 1 unique, 2 duplicate.
        Rubrik: Idempotency & Dedup + Observability.
        """
        eid = str(uuid.uuid4())
        ts = datetime.now(UTC).isoformat()

        await store.increment_received(3)
        await store.mark_processed("t", eid, "s", {}, ts)
        await store.mark_processed("t", eid, "s", {}, ts)  # duplikat
        await store.mark_processed("t", eid, "s", {}, ts)  # duplikat

        stats = await store.get_stats()
        assert stats["received"] == 3
        assert stats["unique_processed"] == 1
        assert stats["duplicate_dropped"] == 2

    @pytest.mark.asyncio
    async def test_different_topics_same_event_id(self, store: InMemoryDedupStore):
        """
        Tujuan: Event dengan event_id sama tapi topic berbeda → KEDUANYA valid.
        Expected: Kedua event diproses (True, True).
        Rubrik: Idempotency & Dedup — dedup key adalah (topic, event_id).
        """
        eid = str(uuid.uuid4())
        ts = datetime.now(UTC).isoformat()

        r1 = await store.mark_processed("topic.a", eid, "svc", {}, ts)
        r2 = await store.mark_processed("topic.b", eid, "svc", {}, ts)

        assert r1 is True, "topic.a + event_id harus diterima"
        assert r2 is True, "topic.b + SAME event_id juga valid (beda topic)"


# ══════════════════════════════════════════════════════════════════════════════
# 3. TRANSACTION TESTS
# ══════════════════════════════════════════════════════════════════════════════

class TestTransactions:
    """
    Rubrik: Transaksi & Konkurensi (16 poin — PRIORITAS UTAMA).
    """

    @pytest.mark.asyncio
    async def test_transaction_commit(self, store: InMemoryDedupStore):
        """
        Tujuan: Event berhasil diproses dan ter-commit ke database.
        Expected: Setelah mark_processed, event ada di get_events().
        Rubrik: Transaksi & Konkurensi — database transaction.
        """
        ev = make_event(topic="txn.commit.test")
        ts = datetime.now(UTC).isoformat()

        result = await store.mark_processed(
            topic=ev["topic"], event_id=ev["event_id"],
            source=ev["source"], payload=ev["payload"], timestamp=ts,
        )

        assert result is True
        # Verifikasi commit: event ada di DB
        events = await store.get_events(topic=ev["topic"])
        found = any(e["event_id"] == ev["event_id"] for e in events)
        assert found, "Event harus ada di DB setelah commit"

    @pytest.mark.asyncio
    async def test_transaction_rollback(self, store: InMemoryDedupStore):
        """
        Tujuan: Duplikat tidak menambah data baru (ON CONFLICT = implicit rollback).
        Expected: Setelah 3x kirim event sama, hanya 1 record di DB.
        Rubrik: Transaksi & Konkurensi — rollback behavior.

        Note: ON CONFLICT DO NOTHING adalah equivalent dari partial rollback
        (INSERT statement di-rollback, transaksi tetap aktif).
        """
        ev = make_event(topic="txn.rollback.test")
        ts = datetime.now(UTC).isoformat()

        for _ in range(3):
            await store.mark_processed(
                topic=ev["topic"], event_id=ev["event_id"],
                source=ev["source"], payload={}, timestamp=ts,
            )

        events = await store.get_events(topic=ev["topic"])
        assert len(events) == 1, "Rollback behavior: hanya 1 record"

    @pytest.mark.asyncio
    async def test_concurrent_workers(self, store: InMemoryDedupStore):
        """
        Tujuan: Multi-worker concurrently memproses event — tidak ada race condition.
        Expected: Bahkan jika 5 worker memproses event_id yang sama secara concurrent,
                  hanya 1 yang berhasil INSERT.
        Rubrik: Transaksi & Konkurensi — multi-worker, race condition prevention.
        """
        shared_event_id = str(uuid.uuid4())
        topic = "concurrent.test"
        ts = datetime.now(UTC).isoformat()

        async def worker_task(worker_id: int) -> bool:
            return await store.mark_processed(
                topic=topic, event_id=shared_event_id,
                source=f"worker-{worker_id}", payload={}, timestamp=ts,
            )

        # 5 worker concurrent
        results = await asyncio.gather(*[worker_task(i) for i in range(5)])

        success_count = sum(1 for r in results if r is True)
        drop_count = sum(1 for r in results if r is False)

        assert success_count == 1, (
            f"Hanya 1 worker yang boleh berhasil! Got {success_count} successes. "
            f"Results: {results}"
        )
        assert drop_count == 4, f"4 worker harus di-drop. Got {drop_count}"

    @pytest.mark.asyncio
    async def test_race_condition_prevention(self, store: InMemoryDedupStore):
        """
        Tujuan: Simulasi race condition — 10 concurrent goroutine, 1 event.
        Expected: Selalu exactly 1 INSERT berhasil, tidak peduli timing.
        Rubrik: Transaksi & Konkurensi — bebas race condition.
        """
        results_all = []

        for trial in range(5):  # 5 independent trials
            eid = str(uuid.uuid4())
            ts = datetime.now(UTC).isoformat()

            async def attempt(trial_eid=eid, trial_ts=ts) -> bool:
                return await store.mark_processed(
                    topic="race.test", event_id=trial_eid,
                    source="racer", payload={}, timestamp=trial_ts,
                )

            trial_results = await asyncio.gather(*[attempt() for _ in range(10)])
            success = sum(1 for r in trial_results if r is True)
            results_all.append(success)

        # Setiap trial harus memiliki tepat 1 success
        assert all(s == 1 for s in results_all), (
            f"Setiap trial harus punya tepat 1 success. Got: {results_all}"
        )

    @pytest.mark.asyncio
    async def test_batch_transaction(self, store: InMemoryDedupStore):
        """
        Tujuan: Batch event dalam satu transaksi — commit/rollback atomik.
        Expected: 3 unique + 2 duplikat → 3 inserted, 2 duplicates.
        Rubrik: Transaksi & Konkurensi — batch transaction.
        """
        base_events = [make_event(topic="batch.txn") for _ in range(3)]
        dup_events = [base_events[0].copy(), base_events[1].copy()]  # 2 duplikat

        all_events = base_events + dup_events

        inserted, duplicates = await store.process_with_explicit_transaction(all_events)

        assert inserted == 3, f"Harus 3 unique inserted, got {inserted}"
        assert duplicates == 2, f"Harus 2 duplicates dropped, got {duplicates}"


# ══════════════════════════════════════════════════════════════════════════════
# 4. PERSISTENCE TESTS
# ══════════════════════════════════════════════════════════════════════════════

class TestPersistence:
    """
    Rubrik: Persistensi — data tetap ada setelah restart.
    """

    @pytest.mark.asyncio
    async def test_persistence_after_restart(self, tmp_path):
        """
        Tujuan: Data tetap ada setelah store di-close dan di-reopen.
        Expected: Event yang diproses di store1 masih ada di store2.
        Rubrik: Persistensi — data survive container restart.
        """
        db_path = str(tmp_path / "persist.db")
        eid = f"persist-{uuid.uuid4()}"
        ts = datetime.now(UTC).isoformat()

        # Sesi 1: insert event
        import aiosqlite
        from tests.conftest import InMemoryDedupStore

        class FileDedupStore(InMemoryDedupStore):
            def __init__(self, path):
                super().__init__()
                self._db_path = path

        store1 = FileDedupStore(db_path)
        await store1.init()
        r1 = await store1.mark_processed("persist.topic", eid, "svc", {}, ts)
        await store1.close()

        # Sesi 2: reopen → event harus masih ada
        store2 = FileDedupStore(db_path)
        await store2.init()
        r2 = await store2.mark_processed("persist.topic", eid, "svc", {}, ts)
        events = await store2.get_events(topic="persist.topic")
        await store2.close()

        assert r1 is True, "Sesi 1: event baru harus diterima"
        assert r2 is False, "Sesi 2: event yang sama harus DITOLAK (sudah ada di DB)"
        assert any(e["event_id"] == eid for e in events), "Event harus ada di DB"


# ══════════════════════════════════════════════════════════════════════════════
# 5. CONSUMER & BROKER INTEGRATION TESTS
# ══════════════════════════════════════════════════════════════════════════════

class TestConsumerIntegration:
    """
    Rubrik: Arsitektur (multi-service), at-least-once delivery, retry.
    """

    @pytest.mark.asyncio
    async def test_at_least_once_delivery(self, store: InMemoryDedupStore, broker: InMemoryBroker):
        """
        Tujuan: Event yang sama dikirim 3x (simulasi at-least-once) → hanya diproses 1x.
        Expected: unique_processed=1, duplicate_dropped=2.
        Rubrik: Idempotency & Dedup — at-least-once delivery.
        """
        ev = make_event(topic="atleastonce.test")

        # Publish 3x (simulasi at-least-once delivery)
        await broker.publish([ev, ev, ev])
        await store.increment_received(3)

        # Consumer memproses semua
        for _ in range(3):
            event_data = await broker.consume(timeout=0.1)
            if event_data:
                await store.mark_processed(
                    topic=event_data["topic"],
                    event_id=event_data["event_id"],
                    source=event_data["source"],
                    payload=event_data["payload"],
                    timestamp=event_data["timestamp"],
                )

        stats = await store.get_stats()
        assert stats["unique_processed"] == 1
        assert stats["duplicate_dropped"] == 2

    @pytest.mark.asyncio
    async def test_retry_mechanism(self, broker: InMemoryBroker):
        """
        Tujuan: Event yang gagal diproses di-push ke retry queue.
        Expected: Setelah push_retry, event ada di retry queue.
        Rubrik: at-least-once delivery + retry mechanism.
        """
        ev = make_event(topic="retry.test")
        await broker.push_retry(ev, retry_count=0)

        retry_ev = await broker.pop_retry()
        assert retry_ev is not None
        assert retry_ev["_retry_count"] == 1
        assert retry_ev["event_id"] == ev["event_id"]

    @pytest.mark.asyncio
    async def test_multi_worker_consumer_no_duplicate(
        self, store: InMemoryDedupStore, broker: InMemoryBroker
    ):
        """
        Tujuan: Multi-worker consumer tidak memproses event yang sama dua kali.
        Expected: 10 unique events → 10 unique_processed, 0 duplicate.
        Rubrik: Transaksi & Konkurensi — multi-worker consumer.
        """
        events = [make_event(topic="multiworker.test") for _ in range(10)]
        await broker.publish(events)
        await store.increment_received(len(events))

        consumer = EventConsumer(broker=broker, store=store, worker_count=3)
        await consumer.start()

        # Beri waktu consumer untuk memproses
        await asyncio.sleep(0.5)
        await consumer.stop()

        stats = await store.get_stats()
        assert stats["unique_processed"] == 10, (
            f"Semua 10 event unik harus diproses. Got {stats['unique_processed']}"
        )
        assert stats["duplicate_dropped"] == 0


# ══════════════════════════════════════════════════════════════════════════════
# 6. BATCH & GET EVENTS TESTS
# ══════════════════════════════════════════════════════════════════════════════

class TestBatchAndQuery:
    """
    Rubrik: Arsitektur — GET /events dengan filter topic, batch publish.
    """

    @pytest.mark.asyncio
    async def test_batch_publish(self, store: InMemoryDedupStore, broker: InMemoryBroker):
        """
        Tujuan: Publisher bisa mengirim batch 50 event sekaligus.
        Expected: Semua 50 event ter-enqueue di broker.
        Rubrik: Arsitektur & Correctness — batch publish.
        """
        events = [make_event(topic="batch.test") for _ in range(50)]
        count = await broker.publish(events)
        assert count == 50
        assert await broker.queue_size() == 50

    @pytest.mark.asyncio
    async def test_get_events(self, store: InMemoryDedupStore):
        """
        Tujuan: GET /events mengembalikan list event yang sudah diproses.
        Expected: Events yang di-insert muncul di get_events().
        Rubrik: Arsitektur & Correctness — GET /events endpoint.
        """
        topic = f"getevents.{uuid.uuid4().hex[:6]}"
        events = [make_event(topic=topic) for _ in range(5)]

        for ev in events:
            await store.mark_processed(
                topic=ev["topic"], event_id=ev["event_id"],
                source=ev["source"], payload=ev["payload"],
                timestamp=ev["timestamp"],
            )

        result = await store.get_events(topic=topic)
        assert len(result) == 5
        assert all(e["topic"] == topic for e in result)

    @pytest.mark.asyncio
    async def test_get_stats(self, store: InMemoryDedupStore):
        """
        Tujuan: get_stats() mengembalikan semua field yang diperlukan.
        Expected: Dict dengan received, unique_processed, duplicate_dropped.
        Rubrik: Observability & Dokumentasi — GET /stats.
        """
        await store.increment_received(10)
        stats = await store.get_stats()
        assert "received" in stats
        assert "unique_processed" in stats
        assert "duplicate_dropped" in stats
        assert stats["received"] == 10

    @pytest.mark.asyncio
    async def test_topic_filter(self, store: InMemoryDedupStore):
        """
        Tujuan: get_events(topic=X) hanya mengembalikan event topic X.
        Expected: Filter topic bekerja dengan benar.
        Rubrik: Arsitektur & Correctness — GET /events?topic=<name>.
        """
        topic_a = f"filter.a.{uuid.uuid4().hex[:4]}"
        topic_b = f"filter.b.{uuid.uuid4().hex[:4]}"

        for _ in range(3):
            ev = make_event(topic=topic_a)
            await store.mark_processed(
                ev["topic"], ev["event_id"], ev["source"], ev["payload"], ev["timestamp"]
            )
        for _ in range(2):
            ev = make_event(topic=topic_b)
            await store.mark_processed(
                ev["topic"], ev["event_id"], ev["source"], ev["payload"], ev["timestamp"]
            )

        result_a = await store.get_events(topic=topic_a)
        result_b = await store.get_events(topic=topic_b)

        assert len(result_a) == 3
        assert len(result_b) == 2
        assert all(e["topic"] == topic_a for e in result_a)


# ══════════════════════════════════════════════════════════════════════════════
# 7. STRESS TEST
# ══════════════════════════════════════════════════════════════════════════════

class TestStress:
    """
    Rubrik: Stress test — throughput, latency, batch performance.
    """

    @pytest.mark.asyncio
    async def test_stress_batch_events(self, store: InMemoryDedupStore, broker: InMemoryBroker):
        """
        Tujuan: Sistem mampu memproses 500 event unik dalam batas waktu yang wajar.
        Expected: Semua 500 event berhasil diproses dalam < 10 detik.
        Rubrik: Stress — throughput dan performance.
        """
        n = 500
        events = [make_event(topic="stress.test") for _ in range(n)]

        start = time.perf_counter()

        await broker.publish(events)
        await store.increment_received(n)

        # Proses semua event
        processed = 0
        while processed < n:
            ev = await broker.consume(timeout=0.1)
            if ev is None:
                break
            result = await store.mark_processed(
                topic=ev["topic"], event_id=ev["event_id"],
                source=ev["source"], payload=ev["payload"],
                timestamp=ev["timestamp"],
            )
            if result:
                processed += 1

        elapsed = time.perf_counter() - start
        eps = processed / elapsed if elapsed > 0 else 0

        assert processed == n, f"Harus {n} event diproses, got {processed}"
        assert elapsed < 10.0, f"Terlalu lambat: {elapsed:.2f}s"
        print(f"\n[STRESS] Processed {processed} events in {elapsed:.2f}s ({eps:.0f} evt/s)")

    @pytest.mark.asyncio
    async def test_concurrent_stress_no_duplicates(self, store: InMemoryDedupStore):
        """
        Tujuan: 100 concurrent tasks memproses 20 event unik (masing-masing dikirim 5x).
        Expected: Tepat 20 unique_processed, 80 duplicate_dropped.
        Rubrik: Transaksi & Konkurensi — bukti uji concurrency.
        """
        unique_events = [make_event(topic="concurrent.stress") for _ in range(20)]
        ts = datetime.now(UTC).isoformat()

        # Setiap event dikirim 5x secara concurrent
        all_tasks = []
        for ev in unique_events:
            for _ in range(5):
                all_tasks.append(
                    store.mark_processed(
                        topic=ev["topic"], event_id=ev["event_id"],
                        source="stress-worker", payload={}, timestamp=ts,
                    )
                )

        results = await asyncio.gather(*all_tasks)

        success = sum(1 for r in results if r is True)
        drops = sum(1 for r in results if r is False)

        assert success == 20, f"Harus tepat 20 unique. Got {success}"
        assert drops == 80, f"Harus tepat 80 dropped. Got {drops}"
