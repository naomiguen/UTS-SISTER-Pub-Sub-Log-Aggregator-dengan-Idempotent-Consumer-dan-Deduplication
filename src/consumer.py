import asyncio
import logging
import time

from src.dedup_store import DedupStore
from src.models import Event

logger = logging.getLogger(__name__)


class AggregatorStats:
    def __init__(self):
        self.received: int = 0          # Total event yang diterima (termasuk duplikat)
        self.unique_processed: int = 0  # Event unik yang berhasil diproses
        self.duplicate_dropped: int = 0 # Event duplikat yang dibuang
        self.start_time: float = time.time()

    @property
    def uptime_seconds(self) -> float:
        return round(time.time() - self.start_time, 2)

    @property
    def duplicate_rate_pct(self) -> float:
        if self.received == 0:
            return 0.0
        return round(self.duplicate_dropped / self.received * 100, 2)

    @property
    def throughput_eps(self) -> float:
        uptime = self.uptime_seconds
        if uptime <= 0:
            return 0.0
        return round(self.unique_processed / uptime, 2)


class EventConsumer:
    def __init__(self, store: DedupStore | None = None):
        from src.dedup_store import DedupStore as DS, DB_PATH
        # Terima store via injection, atau buat default baru
        self.store: DedupStore = store or DS()
        self.queue: asyncio.Queue[Event] = asyncio.Queue()
        self.stats = AggregatorStats()
        self._running = False
        self._task: asyncio.Task | None = None

    async def enqueue(self, event: Event) -> None:
        self.stats.received += 1
        await self.queue.put(event)

    async def _process_one(self, event: Event) -> None:
        is_new = await self.store.mark_as_processed(
            topic=event.topic,
            event_id=event.event_id,
            source=event.source,
        )

        if is_new:
            self.stats.unique_processed += 1
            logger.info(
                f"[PROCESSED] topic={event.topic} | event_id={event.event_id} "
                f"| source={event.source}"
            )
        else:
            self.stats.duplicate_dropped += 1
            logger.warning(
                f"[DUPLICATE DROPPED] topic={event.topic} | event_id={event.event_id} "
                f"| source={event.source} — event ini sudah pernah diproses sebelumnya"
            )

    async def _run_loop(self) -> None:
        logger.info("Consumer loop started — menunggu event dari queue...")
        while self._running:
            try:
                # Tunggu event dari queue, timeout 1 detik agar bisa cek _running
                event = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                await self._process_one(event)
                self.queue.task_done()
            except asyncio.TimeoutError:
                # Tidak ada event dalam 1 detik — normal, lanjut loop
                continue
            except Exception as e:
                logger.error(f"Error saat memproses event: {e}", exc_info=True)

        logger.info("Consumer loop stopped")

    def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("EventConsumer started")

    async def stop(self) -> None:
        self._running = False
        # Tunggu semua event di queue selesai diproses
        await self.queue.join()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("EventConsumer stopped gracefully")


# Singleton instance
consumer = EventConsumer()