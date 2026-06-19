# Pub-Sub Log Aggregator

Layanan aggregator log berbasis **Pub-Sub** dengan **idempotent consumer** dan **persistent deduplication** menggunakan **PostgreSQL + Redis**. Dibangun dengan FastAPI + asyncio + asyncpg + aioredis.

---

## Arsitektur

```
Publisher (HTTP Client)
        │
        │ POST /publish (batch events)
        ▼
┌─────────────────────────────────────────┐
│           FastAPI Application           │
│                                         │
│  ┌──────────┐    Redis Queue            │
│  │ /publish │ ──────────────────►       │
│  └──────────┘                   │       │
│                          ┌──────▼────┐  │
│  ┌──────────┐            │  Event    │  │
│  │ /events  │◄───────────│ Consumer  │  │
│  └──────────┘            │(3 workers)│  │
│                          └──────┬────┘  │
│  ┌──────────┐                   │       │
│  │ /stats   │◄──────────────────┤       │
│  └──────────┘                   │       │
│                                 │       │
│  ┌──────────┐                   │       │
│  │ /health  │◄──────────────────┤       │
│  └──────────┘                   │       │
└─────────────────────────────────┼───────┘
                                  │
                    ┌─────────────┴──────────────┐
                    │                            │
            ┌───────▼────────┐         ┌────────▼────────┐
            │  PostgreSQL    │         │     Redis       │
            │  (dedup store) │         │   (message Q)   │
            │  processed_    │         │  aggregator:    │
            │  events table  │         │  event_queue    │
            └────────────────┘         └─────────────────┘
```

---

## Prasyarat

- **Docker** & **Docker Compose** (wajib)
- Linux/macOS/Windows dengan shell bash-compatible

---

## Quick Start — Docker Compose

### 1. Jalankan Seluruh Stack

```bash
# Dari direktori project
docker-compose up --build
```

Output yang diharapkan:
```
aggregator-postgres | database system is ready to accept connections
aggregator-redis    | Ready to accept connections
log-aggregator      | Aggregator service started. Workers=3 Postgres=ready Redis=ready
log-publisher       | Publisher starting — target=..., total=5000 events, dup_rate=20%
log-publisher       | Publisher done — sent=5000 ...
```

### 2. Akses Endpoint

API tersedia di `http://localhost:8080`:
- **POST /publish** — Publish event
- **GET /events** — Daftar event unik
- **GET /stats** — Statistik sistem
- **GET /health** — Health check

### 3. Hentikan Stack

```bash
docker-compose down
```

Untuk menghapus volume (database):
```bash
docker-compose down -v
```

---

## Demo Presentasi — Buktikan Deduplication, Transaction, Persistence

### Demo 1: Deduplication (At-Least-Once Delivery)

**Tujuan:** Tunjukkan bahwa event duplikat hanya diproses sekali.

```bash
# Terminal 1: Jalankan stack
docker-compose up

# Terminal 2: Kirim event yang SAMA 3 kali secara berurutan
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "events": [{
      "topic": "demo.dedup",
      "event_id": "evt-demo-001",
      "source": "demo-client",
      "timestamp": "2024-01-15T10:00:00Z",
      "payload": {"msg": "This is a test event"}
    }]
  }'

# Tunggu 1 detik
sleep 1

# Kirim lagi (duplikat)
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "events": [{
      "topic": "demo.dedup",
      "event_id": "evt-demo-001",
      "source": "demo-client",
      "timestamp": "2024-01-15T10:00:00Z",
      "payload": {"msg": "This is a test event"}
    }]
  }'

# Kirim lagi (duplikat ke-2)
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "events": [{
      "topic": "demo.dedup",
      "event_id": "evt-demo-001",
      "source": "demo-client",
      "timestamp": "2024-01-15T10:00:00Z",
      "payload": {"msg": "This is a test event"}
    }]
  }'

# Cek hasil
curl http://localhost:8080/stats | python -m json.tool
```

**Harapan:**
```json
{
  "received": 3,
  "unique_processed": 1,
  "duplicate_dropped": 2,
  "topics": ["demo.dedup"],
  "queue_size": 0,
  "events_per_second": 1250.5,
  "avg_latency_ms": 0.85,
  "worker_count": 3,
  "uptime_seconds": 5.23
}
```

**Penjelasan:**
- 3 event diterima (received=3)
- Hanya 1 yang diproses (unique_processed=1)
- 2 di-drop sebagai duplikat (duplicate_dropped=2)
- Terjamin oleh **PostgreSQL UNIQUE constraint (topic, event_id)** + **INSERT ... ON CONFLICT DO NOTHING**

---

### Demo 2: Multi-Worker Concurrency & Transaction

**Tujuan:** Tunjukkan bahwa multi-worker tidak ada race condition.

```bash
# Terminal 2: Jalankan script concurrent test
cat > /tmp/concurrent_test.sh << 'EOF'
#!/bin/bash
EVENT_ID="evt-concurrent-$(date +%s)"

echo "Sending event $EVENT_ID from 10 concurrent processes..."

for i in {1..10}; do
  (curl -s -X POST http://localhost:8080/publish \
    -H "Content-Type: application/json" \
    -d "{
      \"events\": [{
        \"topic\": \"demo.concurrent\",
        \"event_id\": \"$EVENT_ID\",
        \"source\": \"worker-$i\",
        \"timestamp\": \"2024-01-15T10:00:00Z\",
        \"payload\": {\"worker\": $i}
      }]
    }" > /dev/null) &
done

wait
echo "All 10 requests sent concurrently."
sleep 2

# Check stats
echo "Final stats:"
curl -s http://localhost:8080/stats | python -m json.tool | grep -E "(unique_processed|duplicate_dropped)"
EOF

chmod +x /tmp/concurrent_test.sh
/tmp/concurrent_test.sh
```

**Harapan:**
```
unique_processed: 1    (hanya 1 yang insert berhasil)
duplicate_dropped: 9   (9 lainnya conflict)
```

**Penjelasan:**
- 10 worker mengirim event_id yang SAMA secara concurrent
- PostgreSQL **atomically enforces UNIQUE constraint**
- Hanya INSERT pertama yang berhasil
- 9 INSERT lainnya mendapat **ON CONFLICT**, jadi di-skip (tidak error, tidak duplicate insert)
- Ini adalah bukti **race condition prevention via database-level constraint**

---

### Demo 3: Persistence (Container Recreate)

**Tujuan:** Tunjukkan data tetap ada setelah container dihentikan & dijalankan ulang.

```bash
# Terminal 2: Publish beberapa event
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "events": [
      {
        "topic": "demo.persist",
        "event_id": "evt-persist-001",
        "source": "publisher-1",
        "timestamp": "2024-01-15T10:00:00Z",
        "payload": {"data": "First event"}
      },
      {
        "topic": "demo.persist",
        "event_id": "evt-persist-002",
        "source": "publisher-1",
        "timestamp": "2024-01-15T10:01:00Z",
        "payload": {"data": "Second event"}
      }
    ]
  }'

# Tunggu consumer memproses
sleep 2

# Cek stats
echo "BEFORE RECREATE:"
curl -s http://localhost:8080/stats | python -m json.tool | grep unique_processed

# Terminal 1: Hentikan stack (CTRL+C) atau dari Terminal baru:
docker-compose down

# Tunggu 3 detik
sleep 3

# Jalankan ulang
docker-compose up -d

# Tunggu service siap
sleep 5

# Cek stats lagi
echo "AFTER RECREATE:"
curl -s http://localhost:8080/stats | python -m json.tool | grep unique_processed

# Cek GET /events
echo "Events yang dipersist:"
curl -s http://localhost:8080/events?topic=demo.persist | python -m json.tool
```

**Harapan:**
```
BEFORE RECREATE:
unique_processed: 2

(container stop & recreate)

AFTER RECREATE:
unique_processed: 2

Events yang dipersist:
[
  {"topic": "demo.persist", "event_id": "evt-persist-001", ...},
  {"topic": "demo.persist", "event_id": "evt-persist-002", ...}
]
```

**Penjelasan:**
- PostgreSQL volume `postgres_data` persisten setelah `docker-compose down`
- Data di tabel `processed_events` tidak hilang
- Saat container restart, counter & event list sama persis
- Ini membuktikan **durability via database persistence**

---

### Demo 4: GET /stats untuk Observability

```bash
# Monitoring real-time stats
watch -n 1 'curl -s http://localhost:8080/stats | python -m json.tool'
```

Atau simpler:
```bash
while true; do
  echo "=== Stats at $(date) ==="
  curl -s http://localhost:8080/stats | python -m json.tool | \
    grep -E "(received|unique_processed|duplicate_dropped|events_per_second|avg_latency_ms|uptime)"
  sleep 2
done
```

**Metrik:**
- **received:** Total event yang diterima
- **unique_processed:** Event yang diproses (1 kali per unique event_id)
- **duplicate_dropped:** Duplikat yang di-drop
- **topics:** Daftar topic yang ada
- **uptime_seconds:** Berapa lama service jalan
- **queue_size:** Event yang masih di-queue Redis (belum diproses)
- **events_per_second:** Throughput (evt/s)
- **avg_latency_ms:** Latency rerata per event (ms)
- **worker_count:** Jumlah consumer worker yang aktif

---

## Menjalankan Unit Tests (Lokal)

### Setup Environment

```bash
# Clone repo / masuk direktori project
cd uts-aggregator

# Buat virtual environment (opsional)
python3.11 -m venv venv
source venv/bin/activate  # atau: venv\Scripts\activate (Windows)

# Install dependencies
pip install -r requirements.txt
```

### Jalankan Test Suite

```bash
# Semua test
python -m pytest tests/ -v

# Hanya test tertentu
python -m pytest tests/test_aggregator.py::TestDedupStore::test_duplicate_event_dropped -v

# Dengan output log
python -m pytest tests/ -v -s

# Dengan coverage
python -m pytest tests/ --cov=src --cov-report=term-missing
```

**Output yang diharapkan:**
```
tests/test_aggregator.py::TestSchemaValidation::test_publish_valid_event PASSED
tests/test_aggregator.py::TestSchemaValidation::test_publish_invalid_schema PASSED
...
tests/test_aggregator.py::TestDedupStore::test_duplicate_event_dropped PASSED
tests/test_aggregator.py::TestTransactions::test_concurrent_workers PASSED
tests/test_aggregator.py::TestTransactions::test_race_condition_prevention PASSED
tests/test_aggregator.py::TestStress::test_stress_batch_events PASSED
...
============= 31 passed in 2.45s =============
```

---

## Endpoint API Reference

### POST /publish

Publish satu atau batch event ke sistem.

**Request:**
```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "events": [
      {
        "topic": "app.logs",
        "event_id": "uuid-or-custom-id",
        "timestamp": "2024-01-15T10:00:00Z",
        "source": "service-name",
        "payload": {"level": "INFO", "message": "..."}
      }
    ]
  }'
```

**Response (202 Accepted):**
```json
{
  "status": "accepted",
  "enqueued": 1,
  "message": "1 event(s) diterima dan sedang diproses oleh 3 worker."
}
```

---

### GET /events

Ambil daftar event unik yang sudah diproses (dari PostgreSQL).

**Request:**
```bash
# Semua event
curl http://localhost:8080/events

# Filter by topic
curl http://localhost:8080/events?topic=app.logs
```

**Response:**
```json
[
  {
    "topic": "app.logs",
    "event_id": "evt-001",
    "source": "service-a",
    "payload": {"level": "INFO", "message": "User logged in"},
    "timestamp": "2024-01-15T10:00:00+00:00",
    "processed_at": "2024-01-15T10:00:01.234567+00:00"
  }
]
```

---

### GET /stats

Statistik sistem lengkap: counters, throughput, latency, worker info.

**Request:**
```bash
curl http://localhost:8080/stats
```

**Response:**
```json
{
  "received": 5000,
  "unique_processed": 4000,
  "duplicate_dropped": 1000,
  "topics": ["app.logs", "auth.events", "payment.events"],
  "uptime_seconds": 42.5,
  "queue_size": 0,
  "events_per_second": 94.1,
  "avg_latency_ms": 0.95,
  "worker_count": 3
}
```

---

### GET /health

Health check untuk Postgres & Redis connectivity.

**Request:**
```bash
curl http://localhost:8080/health
```

**Response (200 OK):**
```json
{
  "status": "ok",
  "uptime_seconds": 10.5,
  "postgres": "ok",
  "redis": "ok",
  "queue_size": 5
}
```

**Response (503 Degraded):**
```json
{
  "status": "degraded",
  "uptime_seconds": 10.5,
  "postgres": "error: connection failed",
  "redis": "ok",
  "queue_size": 0
}
```

---

## Struktur Proyek

```
uts-aggregator/
├── src/
│   ├── __init__.py              # Package marker
│   ├── main.py                  # FastAPI app + endpoints
│   ├── models.py                # Pydantic Event, PublishRequest, etc.
│   ├── dedup_store.py           # PostgreSQL dedup store
│   ├── broker.py                # Redis Pub/Sub broker
│   ├── consumer.py              # Multi-worker idempotent consumer
│   └── publisher.py             # Publisher script (simulasi)
├── tests/
│   ├── __init__.py              # Package marker
│   ├── conftest.py              # pytest fixtures (in-memory store/broker)
│   └── test_aggregator.py       # 31 comprehensive tests
├── Dockerfile                   # Container image definition
├── docker-compose.yml           # 4 services: postgres, redis, aggregator, publisher
├── .dockerignore                # Files to exclude from docker build
├── requirements.txt             # Python dependencies
├── pytest.ini                   # pytest config
├── schema.sql                   # PostgreSQL schema reference (optional)
└── README.md                    # This file
```

---

## Design Notes

### Deduplication Strategy

**Key:** Kombinasi `(topic, event_id)` sebagai PostgreSQL UNIQUE constraint.

**Cara kerja:**
```sql
INSERT INTO processed_events (topic, event_id, source, payload, timestamp)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (topic, event_id) DO NOTHING
RETURNING 1
```

- Jika event_id baru → INSERT berhasil, return 1
- Jika duplikat → ON CONFLICT, skip INSERT, return 0 (tidak ada row)
- **Atomik di database level** → tidak ada race condition

### At-Least-Once Delivery

Publisher mengirim event ke Redis queue. Jika network rusak, event bisa dikirim >1 kali.

Consumer menjamin **exactly-once processing** via idempotency (unique constraint):

```
Publisher (at-least-once)  →  Redis Queue  →  Consumer  →  Postgres (exactly-once)
                                                              ↓
                                                      mark_processed()
                                                      ↓
                                                      ON CONFLICT → unique
```

### Multi-Worker Consumer

3 asyncio worker (configurable via `WORKER_COUNT` env) berkompetisi membaca dari Redis queue.

**Race condition handling:**
- Worker A & B membaca event_id yang sama
- Keduanya execute `mark_processed()` concurrent
- PostgreSQL unique constraint memastikan hanya 1 INSERT berhasil
- Yang satunya mendapat ON CONFLICT (bukan error) → continue (tidak crash)

Ini adalah **idempotent receiver pattern** dari Tanenbaum & Van Steen.

### Isolation Level: READ COMMITTED

PostgreSQL default, cocok untuk dedup berbasis constraint unik:

- **ATOMIC:** INSERT tunggal selalu atomik di PostgreSQL (storage layer)
- **CONFLICT DETECTION:** Unique constraint dicheck di commit time
- **THROUGHPUT:** Lebih tinggi dari SERIALIZABLE (tidak perlu SSI bookkeeping)

---

## Environment Variables

| Variable | Default | Deskripsi |
|----------|---------|-----------|
| `POSTGRES_DSN` | `postgresql://aggregator:secret@localhost:5432/aggregator_db` | PostgreSQL connection string |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `WORKER_COUNT` | `3` | Jumlah consumer worker |
| `PYTHONUNBUFFERED` | `1` | Jangan buffer stdout (untuk docker logs) |
| `PYTHONDONTWRITEBYTECODE` | `1` | Jangan tulis .pyc (untuk docker) |

**Untuk docker-compose:** Lihat `docker-compose.yml` section `environment`.

---

## Troubleshooting

### Service tidak siap (503)

```bash
docker-compose logs postgres redis aggregator
```

Pastikan postgres & redis healthy sebelum aggregator start.

### Port sudah dipakai

```bash
# Ubah di docker-compose.yml
# "8080:8080" → "8081:8080"
# "5432:5432" → "5433:5432"
# "6379:6379" → "6380:6379"
```

### Data hilang setelah `docker-compose down -v`

```bash
# -v menghapus volume! Jangan gunakan kalau ingin keep data.
docker-compose down  # Keep volume
```

### Test gagal: "sqlite" atau "DEDUP_DB_PATH"

Unit test menggunakan in-memory store (aiosqlite), bukan PostgreSQL. 
Lingkungan override environment di `tests/conftest.py`.

---

## Referensi

- Tanenbaum, A. S., & Van Steen, M. (2007). *Distributed systems: Principles and paradigms* (2nd ed.). Pearson Prentice Hall.
  - Idempotent receiver pattern (Bab 8)
  - Exactly-once semantics via duplicate detection
