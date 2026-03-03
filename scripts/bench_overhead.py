"""Benchmark decorator overhead for cost tracking."""

import statistics
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cost_attribution import AsyncLogger, SQLiteStorage, cost_track
from cost_attribution.utils.async_logger import get_async_logger, set_async_logger

ITERATIONS = 2000
ROUNDS = 5


def plain_fn(x: int) -> int:
    return x + 1


@cost_track(feature="bench", redact_args=True)
def tracked_fn(x: int) -> int:
    return x + 1


def run_once(fn):
    start = time.perf_counter()
    for i in range(ITERATIONS):
        fn(i)
    end = time.perf_counter()
    return (end - start) * 1000


if __name__ == "__main__":
    storage = SQLiteStorage("bench_overhead.db")
    set_async_logger(AsyncLogger(storage_backend=storage, flush_interval=0.2))

    plain = [run_once(plain_fn) for _ in range(ROUNDS)]
    tracked = [run_once(tracked_fn) for _ in range(ROUNDS)]

    plain_ms = statistics.mean(plain)
    tracked_ms = statistics.mean(tracked)
    overhead_pct = ((tracked_ms - plain_ms) / plain_ms * 100.0) if plain_ms > 0 else 0.0

    print(f"plain_avg_ms={plain_ms:.3f}")
    print(f"tracked_avg_ms={tracked_ms:.3f}")
    print(f"overhead_pct={overhead_pct:.2f}")

    get_async_logger().stop()
