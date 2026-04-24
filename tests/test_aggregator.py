import asyncio
import time
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

import tempfile

# Gunakan Windows-compatible temp directory
TEST_DB_PATH = Path(tempfile.gettempdir()) / "test_dedup_store.db"


@pytest.fixture(autouse=True)
def clean_test_db():
    #Hapus DB test sebelum dan sesudah setiap test 
    if TEST_DB_PATH.exists():
        TEST_DB_PATH.unlink()
    yield
    if TEST_DB_PATH.exists():
        TEST_DB_PATH.unlink()


@pytest_asyncio.fixture
async def client():
    #AsyncClient dengan fresh app instance per test. Store di-initialize secara eksplisit di fixture (bukan via lifespan) agar bisa digunakan langsung tanpa harus trigger ASGI lifespan event.
    from src.dedup_store import DedupStore
    from src.consumer import EventConsumer
    from src.main import create_app

    store = DedupStore(db_path=TEST_DB_PATH)
    store.initialize()                      # Inisialisasi SQLite langsung
    consumer = EventConsumer(store=store)
    app = create_app(dedup_store=store, consumer=consumer)

    # Jalankan consumer secara manual (tanpa lifespan)
    consumer.start()

    async with AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=True),
        base_url="http://test",
    ) as ac:
        yield ac

    # Teardown
    await consumer.stop()
    store.close()


def make_event(
    topic: str = "system.auth.login",
    event_id: str | None = None,
    source: str = "test-service",
    payload: dict | None = None,
) -> dict:
    #Helper: buat dict event valid untuk testing
    return {
        "topic": topic,
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": "2024-06-10T10:30:00Z",
        "source": source,
        "payload": payload or {"test": True},
    }


# Test 1: Validasi skema event yang valid
@pytest.mark.asyncio
async def test_valid_event_schema_accepted(client):
    #Event dengan skema lengkap dan valid harus diterima dengan status 200
    event = make_event()
    response = await client.post("/publish", json={"events": [event]})

    assert response.status_code == 200
    data = response.json()
    assert data["received"] == 1
    assert "diterima" in data["message"]


# Test 2: Validasi topic format tidak valid ditolak 
@pytest.mark.asyncio
async def test_invalid_topic_format_rejected(client):
    #Topic dengan karakter tidak valid (spasi, @, /) harus ditolak.
    bad_topics = [
        "system auth",     # spasi
        "system@auth",     # karakter @
        "system/auth",     # slash
        "",                # kosong
    ]
    for bad_topic in bad_topics:
        event = make_event(topic=bad_topic)
        response = await client.post("/publish", json={"events": [event]})
        assert response.status_code == 422, f"Seharusnya ditolak untuk topic: '{bad_topic}'"


# Test 3: Validasi event tanpa field wajib ditolak 
async def test_missing_required_fields_rejected(client):
    #Event tanpa field wajib (event_id, topic) harus ditolak
    # Event tanpa event_id
    event_no_id = {
        "topic": "system.auth.login",
        "timestamp": "2024-06-10T10:30:00Z",
        "source": "test",
        "payload": {},
    }
    response = await client.post("/publish", json={"events": [event_no_id]})
    assert response.status_code == 422

    # Event tanpa topic
    event_no_topic = {
        "event_id": str(uuid.uuid4()),
        "timestamp": "2024-06-10T10:30:00Z",
        "source": "test",
        "payload": {},
    }
    response = await client.post("/publish", json={"events": [event_no_topic]})
    assert response.status_code == 422


# Test 4: Deduplication — duplikat hanya diproses sekali
@pytest.mark.asyncio
async def test_duplicate_event_processed_only_once(client):
    #Kirim event yang sama (topic + event_id identik) 5 kali. Hanya 1 yang boleh lolos ke unique_processed, sisanya duplicate_dropped.
    
    event = make_event(event_id="fixed-id-001")

    # Kirim 5x event yang sama
    for _ in range(5):
        response = await client.post("/publish", json={"events": [event]})
        assert response.status_code == 200

    # Tunggu consumer memproses semua
    await asyncio.sleep(0.5)

    stats = (await client.get("/stats")).json()
    assert stats["received"] == 5
    assert stats["unique_processed"] == 1
    assert stats["duplicate_dropped"] == 4


# Test 5: GET /events mengembalikan hanya event unik
@pytest.mark.asyncio
async def test_get_events_returns_only_unique(client):
    #Setelah mengirim 3 event unik + 2 duplikat, GET /events harus mengembalikan tepat 3 event.
    
    events = [
        make_event(event_id="id-A"),
        make_event(event_id="id-B"),
        make_event(event_id="id-C"),
        make_event(event_id="id-A"),  # duplikat id-A
        make_event(event_id="id-B"),  # duplikat id-B
    ]
    await client.post("/publish", json={"events": events})
    await asyncio.sleep(0.5)

    response = await client.get("/events")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 3

    # Pastikan tidak ada duplikasi dalam list yang dikembalikan
    ids = [e["event_id"] for e in data["events"]]
    assert len(ids) == len(set(ids)), "Ada duplikasi dalam hasil GET /events!"


# Test 6: GET /events filter by topic 
@pytest.mark.asyncio
async def test_get_events_filter_by_topic(client):
    #GET /events?topic=system.auth harus mengembalikan hanya event dari topic itu.
    
    events = [
        make_event(topic="system.auth.login", event_id="auth-1"),
        make_event(topic="system.auth.login", event_id="auth-2"),
        make_event(topic="system.payment.done", event_id="pay-1"),
    ]
    await client.post("/publish", json={"events": events})
    await asyncio.sleep(0.5)

    # Filter by topic auth
    response = await client.get("/events?topic=system.auth.login")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 2
    for ev in data["events"]:
        assert ev["topic"] == "system.auth.login"

    # Filter by topic payment
    response = await client.get("/events?topic=system.payment.done")
    data = response.json()
    assert data["count"] == 1


#  Test 7: GET /stats konsisten dengan data 
@pytest.mark.asyncio
async def test_stats_consistent_with_data(client):
    #Stats harus konsisten: received = unique_processed + duplicate_dropped. Duplicate rate harus akurat.
    
    # 10 event unik + 5 duplikat = 15 received, 10 unique, 5 dropped
    unique_events = [make_event(event_id=f"unique-{i}") for i in range(10)]
    dup_events = [make_event(event_id="unique-0") for _ in range(5)]  # duplikat id-0

    all_events = unique_events + dup_events
    await client.post("/publish", json={"events": all_events})
    await asyncio.sleep(0.8)

    stats = (await client.get("/stats")).json()
    assert stats["received"] == 15
    assert stats["unique_processed"] == 10
    assert stats["duplicate_dropped"] == 5
    # Konsistensi: received = unique + dropped
    assert stats["received"] == stats["unique_processed"] + stats["duplicate_dropped"]
    # Duplicate rate = 5/15 = 33.33%
    assert abs(stats["duplicate_rate_pct"] - 33.33) < 1.0


# Test 8: Persistensi dedup store setelah simulasi restart
@pytest.mark.asyncio
async def test_dedup_store_persists_across_restart(client):
    #Event yang sama harus terdeteksi duplikat oleh store baru.
    
    from src.dedup_store import DedupStore

    event_id = "persistent-event-xyz"
    event = make_event(event_id=event_id)

    # Kirim dan tunggu consumer memproses
    await client.post("/publish", json={"events": [event]})
    await asyncio.sleep(0.5)

    # Pastikan event sudah diproses (verifikasi via GET /events)
    resp = await client.get("/events")
    assert resp.json()["count"] == 1

    # Simulasi restart: buat DedupStore baru yang baca DB yang sama
    restarted_store = DedupStore(db_path=TEST_DB_PATH)
    restarted_store.initialize()

    # Event harus terdeteksi duplikat oleh store baru
    is_dup = await restarted_store.is_duplicate("system.auth.login", event_id)
    restarted_store.close()

    assert is_dup is True, (
        "Dedup store tidak persisten! "
        "Event seharusnya masih tercatat setelah simulasi restart."
    )


# Test 9: Batch publish — banyak event sekaligus
@pytest.mark.asyncio
async def test_batch_publish_accepted(client):
    #Batch publish 50 event sekaligus harus diterima dan diproses semua
    events = [make_event(event_id=str(uuid.uuid4())) for _ in range(50)]
    response = await client.post("/publish", json={"events": events})

    assert response.status_code == 200
    assert response.json()["received"] == 50

    await asyncio.sleep(1.0)
    stats = (await client.get("/stats")).json()
    assert stats["unique_processed"] == 50
    assert stats["duplicate_dropped"] == 0


# Test 10: Stress test — 5000 event dengan 20% duplikasi
@pytest.mark.asyncio
async def test_stress_5000_events_with_20_percent_duplicates(client):
    TOTAL_UNIQUE = 4000
    TOTAL_DUPS = 1000  # 20% dari 5000
    TOTAL = TOTAL_UNIQUE + TOTAL_DUPS
    BATCH_SIZE = 250  # Kirim per batch untuk menghindari request terlalu besar

    # Buat 4000 event unik
    unique_ids = [str(uuid.uuid4()) for _ in range(TOTAL_UNIQUE)]
    all_events_data = [
        make_event(event_id=uid, topic="stress.test.event")
        for uid in unique_ids
    ]

    # Tambahkan 1000 duplikat (ambil dari 1000 ID pertama)
    dup_events_data = [
        make_event(event_id=unique_ids[i % 1000], topic="stress.test.event")
        for i in range(TOTAL_DUPS)
    ]

    all_events_data.extend(dup_events_data)

    # Kirim dalam batch
    start_time = time.time()
    for i in range(0, TOTAL, BATCH_SIZE):
        batch = all_events_data[i : i + BATCH_SIZE]
        response = await client.post("/publish", json={"events": batch})
        assert response.status_code == 200

    elapsed_send = time.time() - start_time
    assert elapsed_send < 30, f"Pengiriman terlalu lambat: {elapsed_send:.1f}s"

    # Tunggu consumer selesai memproses semua
    await asyncio.sleep(5.0)

    stats = (await client.get("/stats")).json()
    assert stats["received"] == TOTAL
    assert stats["unique_processed"] == TOTAL_UNIQUE
    assert stats["duplicate_dropped"] == TOTAL_DUPS

    # Duplicate rate harus sekitar 20%
    assert abs(stats["duplicate_rate_pct"] - 20.0) < 2.0, (
        f"Duplicate rate tidak akurat: {stats['duplicate_rate_pct']}%"
    )