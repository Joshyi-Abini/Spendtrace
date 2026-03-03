"""Benchmark SQLite batch write throughput."""

import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cost_attribution.core.tracker import CostRecord
from cost_attribution.storage.sqlite import SQLiteStorage

BATCH_SIZE = 1000
ROUNDS = 10


def make_record(i: int) -> CostRecord:
    now = time.time()
    return CostRecord(
        tx_id=f"tx-{i}-{now}",
        timestamp=now,
        function_name="bench",
        feature="bench",
        duration_ms=1.0,
        cpu_time_ms=1.0,
        memory_mb=1.0,
        api_calls={},
        cpu_cost=0.0,
        memory_cost=0.0,
        api_cost=0.0,
        total_cost=0.0,
        tags={},
    )


if __name__ == "__main__":
    storage = SQLiteStorage("bench_storage.db")

    total_records = BATCH_SIZE * ROUNDS
    start = time.perf_counter()

    for r in range(ROUNDS):
        batch = [make_record(r * BATCH_SIZE + i) for i in range(BATCH_SIZE)]
        storage.store_batch(batch)

    elapsed = time.perf_counter() - start
    writes_per_sec = total_records / elapsed if elapsed > 0 else 0

    print(f"records={total_records}")
    print(f"elapsed_sec={elapsed:.3f}")
    print(f"writes_per_sec={writes_per_sec:.2f}")
