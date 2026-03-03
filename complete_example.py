"""
cost_attribution - complete demo

Shows the full loop:
1. auto_instrument() at startup
2. @cost_track on feature entry points
3. @track_request with user_id for per-tenant attribution
4. Cost table: feature x service breakdown
5. Per-tenant cost
6. Fully-loaded vs direct cost (graph query)
7. Per-request cost and call-tree drill-down
8. Spend alert setup
"""

import time
from pathlib import Path

from cost_attribution import (
    AsyncLogger,
    SQLiteStorage,
    add_api_call,
    auto_instrument,
    cost_track,
    get_async_logger,
    get_feature_cost_breakdown,
    get_request_cost,
    get_request_subtree,
    set_alert,
    track_request,
)
from cost_attribution.utils.async_logger import set_async_logger


@cost_track(feature="product_details")
def fetch_product(product_id: str):
    add_api_call("dynamodb_read", count=1)
    return {"id": product_id, "name": "Product", "price": 99.99}


@cost_track(feature="search")
def search_products(query: str):
    add_api_call("dynamodb_query", count=1)
    return [fetch_product(f"p{i}") for i in range(3)]


@cost_track(feature="ai_recommendations")
def generate_recommendations(user_id: str):
    del user_id
    add_api_call("bedrock_claude_3_sonnet", count=1, input_tokens=800, output_tokens=220)
    return [f"rec_{i}" for i in range(5)]


@track_request(endpoint="/api/search")
@cost_track(feature="api")
def handle_search(query: str, user_id: str):
    products = search_products(query)
    recs = generate_recommendations(user_id)
    return {"products": products, "recommendations": recs}


def setup():
    db = str(Path.cwd() / "cost_data_demo.db")
    storage = SQLiteStorage(db_path=db)
    logger = AsyncLogger(storage_backend=storage, flush_interval=1.0)
    set_async_logger(logger)
    return storage, db


def run():
    sep = "=" * 70
    print(sep)
    print("  cost_attribution - complete demo")
    print(sep)

    auto_instrument()
    storage, _ = setup()

    tenants = ["tenant-acme", "tenant-globex", "tenant-initech"]
    print("\n[1/7]  Simulating traffic from 3 tenants ...")
    for i in range(15):
        handle_search(f"query_{i}", user_id=tenants[i % 3])
    print("       Done (45 transactions generated)")

    print("\n[2/7]  Flushing to storage ...")
    time.sleep(2.5)
    print("       Done")

    print(f"\n[3/7]  Cost by feature\n{'-' * 70}")
    print(f"{'Feature':<25}  {'DynamoDB':>10}  {'Bedrock':>10}  {'Total':>10}  {'Calls':>6}")
    print("-" * 70)
    for row in storage.aggregate_by_feature():
        svc = row["service_costs"]
        print(
            f"{row['feature']:<25}"
            f"  {svc.get('dynamodb', 0):>10.6f}"
            f"  {svc.get('bedrock', 0):>10.6f}"
            f"  {row['total_cost']:>10.6f}"
            f"  {row['transaction_count']:>6}"
        )
    print(f"\n  Total: ${storage.get_total_cost():.6f}")

    print(f"\n[4/7]  Cost by tenant\n{'-' * 70}")
    print(f"{'Tenant':<25}  {'Total cost':>12}  {'Requests':>10}")
    print("-" * 70)
    for row in storage.aggregate_by_user():
        print(
            f"{row['user_id']:<25}"
            f"  ${row['total_cost']:>11.6f}"
            f"  {row['transaction_count']:>10}"
        )

    print(f"\n[5/7]  Fully-loaded vs direct cost\n{'-' * 70}")
    print(f"  {'Feature':<25}  {'Direct':>12}  {'Fully loaded':>14}  {'Children':>12}")
    print("  " + "-" * 66)
    for row in get_feature_cost_breakdown(storage=storage):
        print(
            f"  {row['feature']:<25}"
            f"  ${row['direct_cost']:>11.6f}"
            f"  ${row['fully_loaded_cost']:>13.6f}"
            f"  ${row['children_cost']:>11.6f}"
        )

    print(f"\n[6/7]  Cost per HTTP request (sample)\n{'-' * 70}")
    requests = get_request_cost(storage=storage, limit=3)
    for request_row in requests:
        print(
            f"  {request_row['endpoint']:<30}  ${request_row['total_cost']:.6f}/call  "
            f"({request_row['tx_count']} spans)"
        )

    if requests:
        most_expensive = requests[0]
        short_id = most_expensive["request_id"][:12]
        print(f"\n  Call tree for most expensive request ({short_id}...):")
        for node in get_request_subtree(most_expensive["request_id"], storage=storage):
            indent = "  " * (node["depth"] + 2)
            print(
                f"{indent}{node['feature']:<22}"
                f"  direct=${node['total_cost']:.6f}"
                f"  subtree=${node['subtree_cost']:.6f}"
            )

    print("\n[7/7]  Spend alert registered")
    set_alert(
        feature="ai_recommendations",
        threshold=1.00,
        window_hours=24,
        webhook=None,
        storage=storage,
    )
    print("  ai_recommendations: alert at $1.00 / 24h (webhook: stdout for demo)")

    print(f"\n{sep}")
    print("  Done. Database: cost_data_demo.db")
    print("  Next steps:")
    print("    python -m cost_attribution.cli.main --db cost_data_demo.db by-feature")
    print("    uvicorn cost_attribution.api.app:app --port 8000")
    print(sep)

    get_async_logger().stop()


if __name__ == "__main__":
    run()
