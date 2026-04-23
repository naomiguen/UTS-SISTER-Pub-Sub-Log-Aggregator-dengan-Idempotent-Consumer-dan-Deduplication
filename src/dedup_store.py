import asyncio
import logging
import sqlite3
import time
from pathlib import Path

logger = logging.getLogger(__name__)

import os
# Path database SQLite — bisa dioverride via env var DEDUP_DB_PATH
DB_PATH = Path(os.getenv("DEDUP_DB_PATH", "/tmp/dedup_store.db"))


class DedupStore:
    def __init__(self, db_path: Path | None = None):
        self.db_path = db_path or DB_PATH
        self.db_path = db_path
        self._lock = asyncio.Lock()  # Proteksi concurrent access dari asyncio coroutines
        self._conn: sqlite3.Connection | None = None

    def initialize(self) -> None:
        self._conn = sqlite3.connect(
            str(self.db_path),
            check_same_thread=False,  # Kita handle locking sendiri via asyncio.Lock
        )
        # WAL mode: Write-Ahead Logging — meningkatkan performa concurrent read/write
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")  # Balance antara safety & speed

        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_events (
                topic       TEXT NOT NULL,
                event_id    TEXT NOT NULL,
                processed_at REAL NOT NULL,    -- Unix timestamp (float)
                source      TEXT,
                PRIMARY KEY (topic, event_id)  -- Composite key = dedup guarantee
            )
        """)
        # Index tambahan untuk query GET /events?topic=...
        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_topic
            ON processed_events (topic)
        """)
        self._conn.commit()
        logger.info(f"DedupStore initialized at {self.db_path}")

    async def is_duplicate(self, topic: str, event_id: str) -> bool:
        async with self._lock:
            cursor = self._conn.execute(
                "SELECT 1 FROM processed_events WHERE topic = ? AND event_id = ? LIMIT 1",
                (topic, event_id)
            )
            return cursor.fetchone() is not None

    async def mark_as_processed(self, topic: str, event_id: str, source: str) -> bool:
        async with self._lock:
            cursor = self._conn.execute(
                """
                INSERT OR IGNORE INTO processed_events (topic, event_id, processed_at, source)
                VALUES (?, ?, ?, ?)
                """,
                (topic, event_id, time.time(), source)
            )
            self._conn.commit()
            # rowcount = 1 berarti INSERT berhasil (event baru)
            # rowcount = 0 berarti IGNORE (duplikat)
            return cursor.rowcount == 1

    async def get_processed_events(self, topic: str | None = None) -> list[dict]:
        async with self._lock:
            if topic:
                cursor = self._conn.execute(
                    """
                    SELECT topic, event_id, processed_at, source
                    FROM processed_events
                    WHERE topic = ?
                    ORDER BY processed_at ASC
                    """,
                    (topic,)
                )
            else:
                cursor = self._conn.execute(
                    """
                    SELECT topic, event_id, processed_at, source
                    FROM processed_events
                    ORDER BY processed_at ASC
                    """
                )
            rows = cursor.fetchall()
            return [
                {
                    "topic": row[0],
                    "event_id": row[1],
                    "processed_at": row[2],
                    "source": row[3],
                }
                for row in rows
            ]

    async def get_all_topics(self) -> list[str]:
        #Ambil daftar semua topic yang sudah pernah diproses
        async with self._lock:
            cursor = self._conn.execute(
                "SELECT DISTINCT topic FROM processed_events ORDER BY topic"
            )
            return [row[0] for row in cursor.fetchall()]

    async def count_processed(self) -> int:
        #Hitung total event unik yang sudah diproses
        async with self._lock:
            cursor = self._conn.execute("SELECT COUNT(*) FROM processed_events")
            return cursor.fetchone()[0]

    def close(self) -> None:
        #Tutup koneksi database saat shutdown
        if self._conn:
            self._conn.close()
            logger.info("DedupStore connection closed")


# Singleton instance — digunakan oleh seluruh aplikasi
dedup_store = DedupStore()