# Pub-Sub Log Aggregator

Layanan aggregator log berbasis **Pub-Sub** dengan **idempotent consumer** dan **persistent deduplication** menggunakan SQLite. Dibangun dengan FastAPI + asyncio + aiosqlite.

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
│  ┌──────────┐    asyncio.Queue          │
│  │ /publish │ ──────────────────►       │
│  └──────────┘                   │       │
│                          ┌──────▼────┐  │
│  ┌──────────┐            │ Event     │  │
│  │ /events  │◄───────────│ Consumer  │  │
│  └──────────┘            │(idempotent│  │
│                          └──────┬────┘  │
│  ┌──────────┐                   │       │
│  │ /stats   │◄──────────────────┤       │
│  └──────────┘                   │       │
└─────────────────────────────────┼───────┘
                                  │
                          ┌───────▼───────┐
                          │  DedupStore   │
                          │   (SQLite)    │
                          │  /app/data/   │
                          │  dedup.db     │
                          └───────────────┘
```

---

## Cara Build & Run

### Menggunakan Docker (Wajib)

```bash
# 1. Build image
docker build -t uts-aggregator .

# 2. Jalankan container (dengan volume untuk persistensi)
docker run -p 8080:8080 -v aggregator_data:/app/data uts-aggregator
```

### Menggunakan Docker Compose 

```bash
# Jalankan semua service
docker-compose up --build

# Lihat log
docker-compose logs -f

# Hentikan
docker-compose down
```

---

## Endpoint API

| Method | Path | Deskripsi |
|--------|------|-----------|
| `POST` | `/publish` | Publish batch/single event |
| `GET` | `/events?topic=<nama>` | Daftar event unik yang diproses |
| `GET` | `/stats` | Statistik sistem |
| `GET` | `/health` | Health check |

### Contoh POST /publish

```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "events": [
      {
        "topic": "app.logs",
        "event_id": "evt-001",
        "timestamp": "2024-01-15T10:00:00",
        "source": "service-a",
        "payload": {"level": "INFO", "message": "User logged in"}
      }
    ]
  }'
```

### Simulasi Duplikat (at-least-once)

```bash
# Kirim event yang sama 3x — hanya 1 yang diproses, 2 di-drop
for i in 1 2 3; do
  curl -s -X POST http://localhost:8080/publish \
    -H "Content-Type: application/json" \
    -d '{"events": [{"topic":"test","event_id":"dup-001","source":"svc","payload":{}}]}'
done

# Cek stats
curl http://localhost:8080/stats
```

### Publisher Script (5.000 event dengan 20% duplikat)

```bash
# Jalankan dalam container yang sudah berjalan
docker exec log-aggregator python -m src.publisher --total 5000 --dup-rate 0.2

# Atau dari host (pastikan aggregator sudah berjalan di port 8080)
python -m src.publisher --host http://localhost:8080 --total 5000 --dup-rate 0.2
```

---

## Menjalankan Unit Tests

```bash
# Aktivasi env
source venv/bin/activate

# Dari direktori project
pip install -r requirements.txt
python -m pytest tests/ -v

# Deaktivasi env
deactivate
```

---

## Struktur Proyek

```
uts-aggregator/
├── src/
│   ├── __init__.py
│   ├── main.py          # FastAPI app + endpoints
│   ├── models.py        # Pydantic models (Event, Stats, dll)
│   ├── dedup_store.py   # SQLite-based dedup store
│   ├── consumer.py      # Idempotent async consumer
│   └── publisher.py     # Publisher script (simulasi)
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   └── test_aggregator.py 
├── Dockerfile
├── docker-compose.yml  
├── requirements.txt
├── pytest.ini
└── README.md
```

---

## Asumsi Desain

1. **Ordering**: Total ordering tidak dibutuhkan untuk log aggregator. Event dari source berbeda tidak memerlukan urutan global — cukup ordering per-topic (partial ordering).
2. **Dedup key**: Kombinasi `(topic, event_id)` sebagai primary key SQLite — collision-resistant karena event_id menggunakan UUID v4.
3. **At-least-once delivery**: Simulasi via publisher yang sengaja mengirim duplikat. Consumer menjamin exactly-once processing melalui idempotency.
4. **Crash recovery**: SQLite WAL mode memastikan data tidak corrupt saat crash. Volume Docker menjamin persistensi antar restart.
5. **Throughput**: In-memory asyncio.Queue + async SQLite (aiosqlite) mendukung ribuan event/detik pada beban normal.

---

## Referensi

- Tanenbaum, A. S., & Van Steen, M. (2007). *Distributed systems: Principles and paradigms* (2nd ed.). Pearson Prentice Hall.
