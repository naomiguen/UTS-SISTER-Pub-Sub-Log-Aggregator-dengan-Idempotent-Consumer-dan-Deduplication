import re
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


# Regex untuk validasi topic: hanya boleh alphanumeric, titik, underscore
# Format: domain.service.event_type  contoh: system.auth.login_failed
TOPIC_PATTERN = re.compile(r'^[a-zA-Z0-9_]+(\.[a-zA-Z0-9_]+)*$')


class Event(BaseModel):
    topic: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="Kategori event, format: domain.service.event_type"
    )
    event_id: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="ID unik event, disarankan UUID v4"
    )
    timestamp: datetime = Field(
        ...,
        description="Waktu kejadian event dalam format ISO 8601"
    )
    source: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="Nama service/komponen yang menghasilkan event"
    )
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="Data tambahan bebas berformat JSON object"
    )

    @field_validator("topic")
    @classmethod
    def validate_topic_format(cls, v: str) -> str:
        if not TOPIC_PATTERN.match(v):
            raise ValueError(
                f"Format topic tidak valid: '{v}'. "
                "Gunakan format: domain.service.event_type "
                "(hanya huruf, angka, titik, underscore)"
            )
        return v.lower()  # normalisasi ke lowercase

    @field_validator("event_id")
    @classmethod
    def validate_event_id_not_empty(cls, v: str) -> str:
        
        stripped = v.strip()
        if not stripped:
            raise ValueError("event_id tidak boleh kosong atau hanya spasi")
        return stripped

    class Config:
        json_schema_extra = {
            "example": {
                "topic": "system.auth.login_failed",
                "event_id": "550e8400-e29b-41d4-a716-446655440000",
                "timestamp": "2024-06-10T10:30:00Z",
                "source": "auth-service",
                "payload": {
                    "user_id": "usr_123",
                    "ip_address": "192.168.1.1",
                    "reason": "invalid_password"
                }
            }
        }


class PublishRequest(BaseModel):
    events: list[Event] = Field(
        ...,
        min_length=1,
        description="Daftar event yang akan dipublikasikan (minimal 1)"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "events": [
                    {
                        "topic": "system.auth.login_failed",
                        "event_id": "550e8400-e29b-41d4-a716-446655440000",
                        "timestamp": "2024-06-10T10:30:00Z",
                        "source": "auth-service",
                        "payload": {"user_id": "usr_123"}
                    }
                ]
            }
        }


class PublishResponse(BaseModel):
    #Response dari POST /publish.
    received: int = Field(..., description="Jumlah event yang diterima dalam request ini")
    message: str = Field(..., description="Pesan konfirmasi")


class StatsResponse(BaseModel):
    #Response dari GET /stats — metrik sistem aggregator
    received: int = Field(..., description="Total event yang diterima sejak startup")
    unique_processed: int = Field(..., description="Total event unik yang berhasil diproses")
    duplicate_dropped: int = Field(..., description="Total event duplikat yang dibuang")
    duplicate_rate_pct: float = Field(..., description="Persentase duplikasi dari total received")
    topics: list[str] = Field(..., description="Daftar topic yang sudah pernah diproses")
    uptime_seconds: float = Field(..., description="Waktu aktif aggregator dalam detik")
    throughput_eps: float = Field(..., description="Event unik per detik (events per second)")