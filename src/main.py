import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Query, Request

from src.consumer import EventConsumer
from src.dedup_store import DedupStore
from src.models import PublishRequest, PublishResponse, StatsResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def create_app(
    dedup_store: DedupStore | None = None,
    consumer: EventConsumer | None = None,
) -> FastAPI:
    store = dedup_store or DedupStore()
    cons = consumer or EventConsumer(store=store)

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        logger.info("=== Aggregator starting up ===")
        store.initialize()
        cons.start()
        logger.info("=== Aggregator ready ===")
        yield
        logger.info("=== Aggregator shutting down ===")
        await cons.stop()
        store.close()
        logger.info("=== Aggregator shutdown complete ===")

    application = FastAPI(
        title="Pub-Sub Log Aggregator",
        description="Layanan aggregator log berbasis pub-sub dengan idempotent consumer dan deduplication.",
        version="1.0.0",
        lifespan=lifespan,
    )
    application.state.dedup_store = store
    application.state.consumer = cons

    # Routes 

    @application.get("/health", tags=["System"])
    async def health_check(request: Request):
        """Health check — untuk monitoring Docker container."""
        c: EventConsumer = request.app.state.consumer
        return {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "uptime_seconds": c.stats.uptime_seconds,
        }

    @application.post("/publish", response_model=PublishResponse, tags=["Publisher"])
    async def publish_events(request: Request, body: PublishRequest):

        c: EventConsumer = request.app.state.consumer
        count = len(body.events)
        for event in body.events:
            await c.enqueue(event)
        logger.info(f"[PUBLISH] Diterima {count} event")
        return PublishResponse(
            received=count,
            message=f"{count} event diterima dan masuk antrian untuk diproses",
        )

    @application.get("/events", tags=["Consumer"])
    async def get_events(
        request: Request,
        topic: str | None = Query(
            default=None,
            description="Filter by topic. Contoh: ?topic=system.auth.login_failed",
        ),
    ):
        s: DedupStore = request.app.state.dedup_store
        events = await s.get_processed_events(topic=topic)
        return {"topic_filter": topic, "count": len(events), "events": events}

    @application.get("/stats", response_model=StatsResponse, tags=["System"])
    async def get_stats(request: Request):
    
        s: DedupStore = request.app.state.dedup_store
        c: EventConsumer = request.app.state.consumer
        topics = await s.get_all_topics()
        return StatsResponse(
            received=c.stats.received,
            unique_processed=c.stats.unique_processed,
            duplicate_dropped=c.stats.duplicate_dropped,
            duplicate_rate_pct=c.stats.duplicate_rate_pct,
            topics=topics,
            uptime_seconds=c.stats.uptime_seconds,
            throughput_eps=c.stats.throughput_eps,
        )

    return application


# Instance production — dijalankan oleh uvicorn
app = create_app()