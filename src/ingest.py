"""Ingest layer: produces deterministic synthetic order-event batches.

Every batch is generated from ``base_seed + cycle_id`` so a given demo command
replays identically. In a real deployment this would wrap a Kafka/Kinesis
consumer; the contract (`get_next_batch`) stays the same.
"""

import random

REGIONS = ["us-east", "us-west", "eu-west", "ap-south", "sa-east"]
SKUS = [f"SKU-{i:04d}" for i in range(100)]


def get_next_batch(cycle_id: int, size: int = 300, base_seed: int = 42) -> list[dict]:
    rng = random.Random(base_seed + cycle_id)
    batch = []
    for i in range(size):
        batch.append(
            {
                "order_id": f"ORD-{cycle_id:03d}-{i:05d}",
                "user_id": f"USR-{rng.randint(1, 5000):05d}",
                "item_sku": rng.choice(SKUS),
                "quantity": rng.randint(1, 5),
                "price": round(rng.uniform(5.0, 500.0), 2),
                "timestamp": f"2026-07-07T{rng.randint(0, 23):02d}:{rng.randint(0, 59):02d}:00Z",
                "region": rng.choice(REGIONS),
            }
        )
    return batch
