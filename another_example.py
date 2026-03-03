"""Complete runnable demo for the cost attribution system."""

import time
from pathlib import Path

from cost_attribution import (
    AWSCostModel,
    AsyncLogger,
    SQLiteStorage,
    add_api_call,
    cost_track,
    get_async_logger,
    get_tracker,
    track_request,
)


@cost_track(feature="search")
def search_products(query: str):
    time.sleep(0.01)
    add_api_call("dynamodb_query", count=1)
    return [f"product_{i}" for i in range(10)]


@cost_track(feature="ai_recommendations")
def generate_recommendations(user_id: str):
    time.sleep(0.05)
    add_api_call("bedrock_claude_3_sonnet", count=1)
    return [f"rec_{i}" for i in range(5)]


@cost_track(feature="product_details")
def fetch_product_details(product_id: str):
    time.sleep(0.002)
    add_api_call("dynamodb_read", count=1)
    return {"id": product_id, "name": "Product", "price": 99.99}


@track_request(feature="api", endpoint="/api/search")
@cost_track(feature="api")
def handle_search_request(query: str, user_id: str):
    products = search_products(query)
    details = [fetch_product_details(p) for p in products[:3]]
    recommendations = generate_recommendations(user_id)
    return {"products": details, "recommendations": recommendations}


def process_batch_orders(orders):
    from cost_attribution import track

    with track(feature="batch_processing", operation="process_orders"):
        for _order in orders:
            time.sleep(0.005)
            add_api_call("dynamodb_write", count=1)

    return len(orders)


def setup_storage(db_path: str = "cost_data.db"):
    storage = SQLiteStorage(db_path=db_path)
    logger = AsyncLogger(storage_backend=storage, flush_interval=2.0)

    from cost_attribution.utils.async_logger import set_async_logger

    set_async_logger(logger)
    return storage


def run_demo():
    print("=" * 80)
    print("COST ATTRIBUTION SYSTEM - COMPLETE DEMO")
    print("=" * 80)

    print("\n1. Setting up storage...")
    storage = setup_storage(str(Path.cwd() / "cost_data.db"))
    print("   [ok] SQLite storage initialized")

    print("\n2. Running operations...")
    for i in range(10):
        handle_search_request(f"query_{i}", f"user_{i % 3}")
    process_batch_orders([f"order_{i}" for i in range(20)])
    for i in range(5):
        generate_recommendations(f"user_{i}")
    print("   [ok] Executed 35 operations")

    print("\n3. Flushing data to storage...")
    time.sleep(3)
    print("   [ok] Data flushed")

    print("\n4. Cost Analysis:")
    print("-" * 80)

    feature_costs = storage.aggregate_by_feature()
    print("\nCost by Feature:")
    print(f"{'Feature':<30} {'Transactions':>12} {'Total Cost':>15} {'Avg Cost':>15}")
    print("-" * 80)
    for item in feature_costs:
        print(
            f"{item['feature']:<30} "
            f"{item['transaction_count']:>12} "
            f"${item['total_cost']:>14.6f} "
            f"${item['avg_cost']:>14.6f}"
        )

    user_costs = storage.aggregate_by_user()
    print("\nTop Users by Cost:")
    print(f"{'User ID':<30} {'Transactions':>12} {'Total Cost':>15}")
    print("-" * 80)
    for item in user_costs[:5]:
        print(
            f"{item['user_id']:<30} "
            f"{item['transaction_count']:>12} "
            f"${item['total_cost']:>14.6f}"
        )

    total_cost = storage.get_total_cost()
    print(f"\nTotal Cost: ${total_cost:.6f}")

    print("\n5. Key Insights:")
    print("-" * 80)
    if feature_costs:
        most_expensive = feature_costs[0]
        print(f"Most expensive feature: {most_expensive['feature']}")
        print(f"Total cost: ${most_expensive['total_cost']:.6f}")
        print(f"Avg cost per request: ${most_expensive['avg_cost']:.6f}")

    print("=" * 80)
    print("[ok] Demo complete")

    get_async_logger().stop()


def demo_custom_cost_model():
    print("=" * 80)
    print("CUSTOM COST MODEL DEMO")
    print("=" * 80)

    custom_model = AWSCostModel()
    custom_model.api_costs.update({"my_api_call": 0.001, "expensive_ml_model": 0.1})

    tracker = get_tracker()
    tracker.cost_model = custom_model

    print("[ok] Custom cost model configured")
    print(f"Custom API costs include: {list(custom_model.api_costs.keys())[:5]}")


if __name__ == "__main__":
    run_demo()

    print("\nFor more examples, see:")
    print("  - examples/basic_usage.py")
    print("  - examples/fastapi_integration.py")
    print("  - examples/flask_integration.py")
