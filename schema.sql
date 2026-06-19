-- ============================================================
-- Pub-Sub Log Aggregator — PostgreSQL Schema
-- Isolation Level: READ COMMITTED (default PostgreSQL)
-- ============================================================

-- Enable timing for diagnostics
SET statement_timeout = '30s';

-- ── processed_events ────────────────────────────────────────
-- PRIMARY KEY (topic, event_id) menjamin idempotency.
-- INSERT ... ON CONFLICT DO NOTHING digunakan oleh consumer
-- untuk atomic upsert tanpa race condition.
CREATE TABLE IF NOT EXISTS processed_events (
    id            BIGSERIAL PRIMARY KEY,
    topic         TEXT        NOT NULL,
    event_id      TEXT        NOT NULL,
    source        TEXT        NOT NULL,
    payload       JSONB       NOT NULL DEFAULT '{}',
    timestamp     TIMESTAMPTZ NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Unique constraint sebagai dedup key
    CONSTRAINT uq_topic_event_id UNIQUE (topic, event_id)
);

-- Index untuk query GET /events?topic=<name> yang cepat
CREATE INDEX IF NOT EXISTS idx_events_topic
    ON processed_events (topic, processed_at DESC);

-- Index untuk analytics / stats
CREATE INDEX IF NOT EXISTS idx_events_processed_at
    ON processed_events (processed_at DESC);

-- ── stats ────────────────────────────────────────────────────
-- Counter global untuk received, unique_processed, duplicate_dropped.
-- Menggunakan advisory lock + UPDATE WHERE agar tidak ada lost-update.
CREATE TABLE IF NOT EXISTS stats (
    key    TEXT    PRIMARY KEY,
    value  BIGINT  NOT NULL DEFAULT 0
);

INSERT INTO stats (key, value) VALUES
    ('received',          0),
    ('unique_processed',  0),
    ('duplicate_dropped', 0)
ON CONFLICT (key) DO NOTHING;

-- ── throughput_log ───────────────────────────────────────────
-- Menyimpan snapshot throughput per-menit untuk observability.
CREATE TABLE IF NOT EXISTS throughput_log (
    logged_at          TIMESTAMPTZ PRIMARY KEY DEFAULT NOW(),
    events_per_second  NUMERIC(10,2),
    batch_size         INT,
    latency_ms         NUMERIC(10,2)
);
