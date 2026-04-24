# Pub-Sub Log Aggregator
**UTS Sistem Terdistribusi dan Parallel**

Layanan log aggregator berbasis **Publish-Subscribe** dengan **idempotent consumer** dan **persistent deduplication store**. Dibangun menggunakan Python (FastAPI + asyncio + SQLite), berjalan sepenuhnya lokal di dalam Docker container.

---

## Cara Build & Run

```bash
# 1. Build image
docker build -t uts-aggregator .

# 2. Run container
docker run -p 8080:8080 uts-aggregator
```

Akses API di: **http://localhost:8080**

Dokumentasi interaktif (Swagger UI): **http://localhost:8080/docs**

### Dengan Persistent Volume

Agar SQLite dedup store tetap ada setelah container *dihapus* (bukan sekadar restart):

```bash
docker run -p 8080:8080 -v aggregator_data:/tmp uts-aggregator
```

### Docker Compose 

Menjalankan aggregator + publisher simulator sebagai dua service terpisah:

```bash
docker compose up --build
```

---

## Endpoint API

| Method | Endpoint | Deskripsi |
|--------|----------|-----------|
| `GET` | `/health` | Health check container |
| `POST` | `/publish` | Kirim batch/single event |
| `GET` | `/events` | Ambil semua event unik yang diproses |
| `GET` | `/events?topic=X` | Filter event berdasarkan topic |
| `GET` | `/stats` | Metrik sistem real-time |

---

### `POST /publish` — Kirim Event

**Request body:**
```json
{
  "events": [
    {
      "topic": "system.auth.login_failed",
      "event_id": "550e8400-e29b-41d4-a716-446655440000",
      "timestamp": "2024-06-10T10:30:00Z",
      "source": "auth-service",
      "payload": {
        "user_id": "usr_123",
        "ip_address": "192.168.1.1"
      }
    }
  ]
}
```

**Response (200 OK):**
```json
{
  "received": 1,
  "message": "1 event diterima dan masuk antrian untuk diproses"
}
```

**Validasi field:**
- `topic` — wajib, format `domain.service.event_type`, hanya boleh huruf/angka/titik/underscore, max 128 karakter
- `event_id` — wajib, string unik (disarankan UUID v4), max 128 karakter
- `timestamp` — wajib, format ISO 8601 (contoh: `2024-06-10T10:30:00Z`)
- `source` — wajib, nama service/komponen pengirim
- `payload` — opsional, JSON object bebas

**Response error (422 Unprocessable Entity)** jika validasi gagal:
```json
{
  "detail": [
    {
      "loc": ["body", "events", 0, "topic"],
      "msg": "Format topic tidak valid...",
      "type": "value_error"
    }
  ]
}
```

**Contoh curl:**
```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "events": [
      {
        "topic": "system.auth.login_failed",
        "event_id": "550e8400-e29b-41d4-a716-446655440000",
        "timestamp": "2024-06-10T10:30:00Z",
        "source": "auth-service",
        "payload": {"user_id": "usr_123"}
      }
    ]
  }'
```

---

### `GET /events` — Ambil Event Unik

Mengembalikan daftar event yang sudah berhasil diproses (duplikat sudah dibuang).

**Tanpa filter:**
```bash
curl http://localhost:8080/events
```
```json
{
  "topic_filter": null,
  "count": 3,
  "events": [
    {
      "topic": "system.auth.login_failed",
      "event_id": "550e8400-e29b-41d4-a716-446655440000",
      "processed_at": 1718012400.123,
      "source": "auth-service"
    },
    {
      "topic": "system.payment.done",
      "event_id": "661f9511-f3ac-52e5-b827-557766551111",
      "processed_at": 1718012401.456,
      "source": "payment-service"
    }
  ]
}
```

**Dengan filter topic:**
```bash
curl "http://localhost:8080/events?topic=system.auth.login_failed"
```
```json
{
  "topic_filter": "system.auth.login_failed",
  "count": 1,
  "events": [
    {
      "topic": "system.auth.login_failed",
      "event_id": "550e8400-e29b-41d4-a716-446655440000",
      "processed_at": 1718012400.123,
      "source": "auth-service"
    }
  ]
}
```

---

### `GET /stats` — Metrik Sistem

Menampilkan statistik aggregator secara real-time.

```bash
curl http://localhost:8080/stats
```
```json
{
  "received": 6250,
  "unique_processed": 5000,
  "duplicate_dropped": 1250,
  "duplicate_rate_pct": 20.0,
  "topics": ["system.auth.login_failed", "system.payment.done"],
  "uptime_seconds": 142.3,
  "throughput_eps": 35.2
}
```

| Field | Deskripsi |
|-------|-----------|
| `received` | Total event masuk sejak startup (termasuk duplikat) |
| `unique_processed` | Event unik yang berhasil diproses |
| `duplicate_dropped` | Event yang dibuang karena duplikat |
| `duplicate_rate_pct` | `duplicate_dropped / received × 100` |
| `topics` | Daftar topic yang aktif |
| `uptime_seconds` | Waktu aktif sejak container start |
| `throughput_eps` | Event unik per detik |

---

### `GET /health` — Health Check

```bash
curl http://localhost:8080/health
```
```json
{
  "status": "healthy",
  "timestamp": "2024-06-10T10:30:05.123Z",
  "uptime_seconds": 5.1
}
```

---

## Demo Simulasi Duplikasi (At-Least-Once)

Kirim event yang sama dua kali — duplikat kedua harus di-drop:

```bash
# Kirim pertama kali: diproses
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{"events": [{"topic": "demo.test.event", "event_id": "demo-001", "timestamp": "2024-06-10T10:00:00Z", "source": "demo", "payload": {}}]}'

# Kirim ulang (simulasi retry at-least-once): DUPLICATE DROPPED
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{"events": [{"topic": "demo.test.event", "event_id": "demo-001", "timestamp": "2024-06-10T10:00:00Z", "source": "demo", "payload": {}}]}'

# Cek stats — duplicate_dropped harus = 1
curl http://localhost:8080/stats
```

Log container akan menampilkan:
```
[DUPLICATE DROPPED] topic=demo.test.event | event_id=demo-001 | source=demo
```

---

## Menjalankan Unit Tests

```bash
# Install dependencies
pip install -r requirements.txt

# Jalankan semua 10 tests
python -m pytest tests/ -v
```

Output:
```
tests/test_aggregator.py::test_valid_event_schema_accepted            PASSED
tests/test_aggregator.py::test_invalid_topic_format_rejected          PASSED
tests/test_aggregator.py::test_missing_required_fields_rejected       PASSED
tests/test_aggregator.py::test_duplicate_event_processed_only_once    PASSED
tests/test_aggregator.py::test_get_events_returns_only_unique         PASSED
tests/test_aggregator.py::test_get_events_filter_by_topic             PASSED
tests/test_aggregator.py::test_stats_consistent_with_data             PASSED
tests/test_aggregator.py::test_dedup_store_persists_across_restart    PASSED
tests/test_aggregator.py::test_batch_publish_accepted                 PASSED
tests/test_aggregator.py::test_stress_5000_events_with_20_percent_duplicates PASSED

10 passed in ~13s
```

---

## Struktur Project

```
uts-aggregator/
├── src/
│   ├── __init__.py
│   ├── models.py          # Skema Event + validasi Pydantic
│   ├── dedup_store.py     # SQLite dedup store (INSERT OR IGNORE, WAL mode)
│   ├── consumer.py        # Async EventConsumer + AggregatorStats
│   └── main.py            # FastAPI app factory + semua endpoint
├── tests/
│   ├── __init__.py
│   └── test_aggregator.py # 10 unit tests (pytest + httpx)
├── Dockerfile             # Image aggregator — wajib
├── Dockerfile.publisher   # Image publisher simulator — bonus
├── docker-compose.yml     # Bonus +10% (dua service terpisah)
├── publisher_sim.py       # Simulator publisher (200 event + 20% duplikat)
├── requirements.txt
├── pytest.ini
├── report.md              # Laporan teori T1–T8 + desain implementasi
└── README.md
```

---

## Desain

1. **Single-node, lokal** — Tidak ada clustering atau komunikasi ke layanan eksternal. Seluruh komponen berjalan di dalam satu container.

2. **At-least-once delivery** — Publisher diperbolehkan mengirim ulang event yang sama (retry). Consumer yang bertanggung jawab membuang duplikat melalui dedup store.

3. **Idempotency via SQLite PRIMARY KEY** — Duplikasi dicegah pada level database menggunakan `INSERT OR IGNORE` dengan `PRIMARY KEY (topic, event_id)`. Ini atomik dan bebas race condition.

4. **Ordering per-topic, bukan total ordering** — Event diurutkan berdasarkan `timestamp` saat `GET /events` dipanggil. Total ordering lintas topic tidak diperlukan untuk use case log aggregation (dibahas di T5 laporan).

5. **event_id disarankan UUID v4** — Publisher diharapkan men-generate UUID v4 per event. Dua event berbeda yang menggunakan ID yang sama pada topic yang sama akan menyebabkan yang kedua di-drop — ini tanggung jawab publisher.

6. **SQLite tersimpan di `/tmp`** — Di dalam container, DB berada di `/tmp/dedup_store.db`. Untuk persistensi antar penghapusan container, gunakan flag `-v aggregator_data:/tmp`. Restart container biasa (tanpa `rm`) tidak menghapus file di `/tmp` selama proses container masih hidup.

7. **Tidak ada autentikasi/enkripsi** — Sesuai scope tugas: semua berjalan lokal, tidak ada koneksi ke internet.

8. **Port default 8080** — Sesuai spesifikasi Dockerfile tugas.

---

## Referensi

Van Steen, M., & Tanenbaum, A. S. (2023). *Distributed systems* (Edisi ke-4, Versi 4.01). Maarten van Steen.