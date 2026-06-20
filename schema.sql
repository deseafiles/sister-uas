SET statement_timeout = '30s';

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

CREATE INDEX IF NOT EXISTS idx_events_processed_at
    ON processed_events (processed_at DESC);

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
