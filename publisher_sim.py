#Simulator publisher untuk demo Docker Compose.

import json
import os
import time
import uuid
import urllib.request
import urllib.error

AGGREGATOR_URL = os.getenv("AGGREGATOR_URL", "http://localhost:8080")
TOTAL_UNIQUE = 200
DUP_RATIO = 0.2  # 20% duplikasi


def post_events(events: list[dict]) -> dict:
    """Kirim batch event ke aggregator via HTTP POST."""
    payload = json.dumps({"events": events}).encode("utf-8")
    req = urllib.request.Request(
        f"{AGGREGATOR_URL}/publish",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def get_stats() -> dict:
    with urllib.request.urlopen(f"{AGGREGATOR_URL}/stats", timeout=5) as resp:
        return json.loads(resp.read())


def make_event(topic: str, event_id: str, source: str = "publisher-sim") -> dict:
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": "2024-06-10T10:30:00Z",
        "source": source,
        "payload": {"sim": True, "generated_at": time.time()},
    }


def main():
    print(f"[Publisher] Menghubungi aggregator di {AGGREGATOR_URL} ...")
    time.sleep(2)  # Tunggu aggregator benar-benar ready

    # Generate event unik
    unique_ids = [str(uuid.uuid4()) for _ in range(TOTAL_UNIQUE)]
    events = [make_event("sim.publisher.event", uid) for uid in unique_ids]

    # Tambahkan duplikat (20% dari TOTAL_UNIQUE = 40 duplikat)
    dup_count = int(TOTAL_UNIQUE * DUP_RATIO)
    dups = [make_event("sim.publisher.event", unique_ids[i % dup_count]) for i in range(dup_count)]
    events.extend(dups)

    total = len(events)
    print(f"[Publisher] Mengirim {total} event ({TOTAL_UNIQUE} unik + {dup_count} duplikat) ...")

    # Kirim dalam batch 50
    for i in range(0, total, 50):
        batch = events[i:i+50]
        result = post_events(batch)
        print(f"[Publisher] Batch {i//50 + 1}: {result}")
        time.sleep(0.5)

    print("[Publisher] Selesai mengirim semua event. Menunggu consumer memproses...")
    time.sleep(3)

    stats = get_stats()
    print(f"\n[Publisher] === STATS AKHIR ===")
    print(f"  received:          {stats['received']}")
    print(f"  unique_processed:  {stats['unique_processed']}")
    print(f"  duplicate_dropped: {stats['duplicate_dropped']}")
    print(f"  duplicate_rate:    {stats['duplicate_rate_pct']}%")
    print(f"  topics:            {stats['topics']}")
    print(f"  uptime:            {stats['uptime_seconds']}s")


if __name__ == "__main__":
    main()