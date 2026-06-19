"""
models.py — Pydantic v2 schemas untuk Pub-Sub Log Aggregator.

Event schema mencakup validasi topic (no spaces, lowercase),
event_id (non-empty), timestamp (ISO8601), source, dan payload bebas.
"""

from __future__ import annotations

import uuid
from datetime import datetime, UTC
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


# ──────────────────────────────────────────────────────────────────────────────
# Core Event
# ──────────────────────────────────────────────────────────────────────────────

class Event(BaseModel):
    """
    Representasi satu event dalam sistem Pub-Sub.

    Dedup key: (topic, event_id) — harus unik per pasang.
    """

    topic: str = Field(
        ...,
        min_length=1,
        description="Nama topic/channel event (lowercase, tanpa spasi)",
    )
    event_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="ID unik event — UUID v4 direkomendasikan",
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Waktu event dalam format ISO8601 (timezone-aware)",
    )
    source: str = Field(
        ..., min_length=1, description="Sumber/publisher event"
    )
    payload: dict[str, Any] = Field(
        default_factory=dict, description="Data event bebas (arbitrary JSON)"
    )

    @field_validator("topic")
    @classmethod
    def topic_no_spaces(cls, v: str) -> str:
        if " " in v:
            raise ValueError(
                "Topic tidak boleh mengandung spasi; gunakan underscore atau titik."
            )
        return v.lower().strip()

    @field_validator("event_id")
    @classmethod
    def event_id_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("event_id tidak boleh kosong atau hanya whitespace.")
        return v.strip()

    @field_validator("source")
    @classmethod
    def source_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("source tidak boleh kosong.")
        return v.strip()

    model_config = {"ser_json_timedelta": "iso8601"}


# ──────────────────────────────────────────────────────────────────────────────
# Request / Response Schemas
# ──────────────────────────────────────────────────────────────────────────────

class PublishRequest(BaseModel):
    """Body untuk POST /publish — mendukung single atau batch event."""

    events: list[Event] = Field(..., min_length=1)

    @model_validator(mode="after")
    def events_not_empty(self) -> "PublishRequest":
        if not self.events:
            raise ValueError("Daftar events tidak boleh kosong.")
        return self


class PublishResponse(BaseModel):
    status: str
    enqueued: int
    message: str


class EventResponse(BaseModel):
    """Schema untuk GET /events."""

    topic: str
    event_id: str
    source: str
    payload: dict[str, Any]
    timestamp: str
    processed_at: str


class StatsResponse(BaseModel):
    """Schema untuk GET /stats — mencakup semua metrik observability."""

    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: list[str]
    uptime_seconds: float
    queue_size: int = 0
    events_per_second: float = 0.0
    avg_latency_ms: float = 0.0
    worker_count: int = 1


class HealthResponse(BaseModel):
    status: str
    uptime_seconds: float
    postgres: str = "unknown"
    redis: str = "unknown"
    queue_size: int = 0
