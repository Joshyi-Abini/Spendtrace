"""Basic usage example for cost attribution."""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cost_attribution import AsyncLogger, SQLiteStorage, add_api_call, cost_track, get_async_logger
from cost_attribution.utils.async_logger import set_async_logger

storage = SQLiteStorage("example_basic.db")
set_async_logger(AsyncLogger(storage_backend=storage))


@cost_track(feature="search")
def search(query: str):
    time.sleep(0.01)
    add_api_call("dynamodb_query", 1)
    return [query]


if __name__ == "__main__":
    search("laptop")
    time.sleep(0.2)
    get_async_logger().stop()
    print(storage.aggregate_by_feature())
