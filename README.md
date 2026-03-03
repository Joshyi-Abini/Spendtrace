# spendtrace

**Feature-level AWS cost attribution for Python applications.**

You're getting surprise AWS bills. Cost Cure shows you which services are spending money — but not which *features* in your product are responsible, or which *customers* are driving the cost. This library closes that gap.

```
feature                   dynamodb     bedrock       s3        total/call
ai_recommendations        $0.001       $0.714       $0.002     $0.717
search                    $0.041       —            $0.001     $0.042
batch_processing          $0.000       —            —          $0.000
```

It instruments your Python application directly — not your AWS tags, not your billing data after the fact — so the attribution is exact, not inferred.

---

## Install

```bash
pip install spendtrace
```

---

## The three-step loop

### 1. Instrument

Call `auto_instrument()` once at startup. Every boto3 and LLM SDK call made inside a `@cost_track` scope is captured automatically.

```python
import spendtrace as cost_attribution
cost_attribution.auto_instrument()   # patches boto3, openai, anthropic SDKs
```

Decorate your feature entry points:

```python
from spendtrace import cost_track

@cost_track(feature="ai_recommendations")
def recommend(user_id: str):
    items = dynamo.get_item(...)          # captured automatically
    response = bedrock.invoke_model(...)  # captured automatically
    return response
```

That's the whole install. One import, one call, one decorator per feature.

### 2. Observe

```python
from spendtrace import SQLiteStorage

storage = SQLiteStorage("cost_data.db")
for row in storage.aggregate_by_feature():
    svc = row["service_costs"]
    print(
        f"{row['feature']:<25}"
        f"  dynamodb=${svc.get('dynamodb', 0):.4f}"
        f"  bedrock=${svc.get('bedrock', 0):.4f}"
        f"  total=${row['total_cost']:.4f}"
        f"  ({row['transaction_count']} calls)"
    )
```

```
ai_recommendations        dynamodb=$0.0010  bedrock=$0.7140  total=$0.7150  (142 calls)
search                    dynamodb=$0.0410  bedrock=$0.0000  total=$0.0420  (1831 calls)
batch_processing          dynamodb=$0.0003  bedrock=$0.0000  total=$0.0003  (204 calls)
```

### 3. Verify

```python
from spendtrace import reconcile

report = reconcile(
    db_path="cost_data.db",
    start="2026-02-01",
    end="2026-03-01",
)
print(report.summary())
```

```
============================================================
  Reconciliation Report  2026-02-01 → 2026-03-01
============================================================
  Modelled total :    $142.3810
  Actual total   :    $149.0200
  Delta          :     +$6.6390
  Calibration Δ  :     1.0466×

  Top features by modelled cost:
    ai_recommendations   modelled $98.2100  actual $103.4400  gap +$5.2300
    search               modelled $31.0400  actual $31.8800   gap  +$0.8400
    batch_processing     modelled $13.1300  actual $13.7000   gap  +$0.5700
============================================================
```

The reconciliation compares your modelled costs against AWS Cost Explorer by service
(DynamoDB, S3, Bedrock separately — not the blended total). If the model is off,
restate historical records in one call:

```python
storage.restate_historical_costs(
    factor=report.global_calibration_factor,
    start_date="2026-02-01",
    end_date="2026-03-01",
)
```

---

## Per-customer cost attribution

Add `user_id` to your request boundary and every nested call inherits it automatically.

```python
from spendtrace import track_request

@track_request(endpoint="/api/search", user_id=current_tenant_id)
def handle_request(query):
    return search_products(query)   # all nested costs attributed to this tenant
```

```python
for row in storage.aggregate_by_user():
    print(f"{row['user_id']:<20}  ${row['total_cost']:.4f}/month  ({row['transaction_count']} requests)")
```

```
tenant-acme           $47.2100/month  (8,341 requests)
tenant-globex         $12.8800/month  (3,102 requests)
tenant-initech         $3.1200/month    (891 requests)
```

This is exact attribution — recorded at the moment each AWS API call was made, not
inferred later from billing tags or machine learning.

---

## Fully-loaded vs direct cost per feature

When `search` calls `product_details` which reads from DynamoDB, those reads are
attributed to `product_details`. That's right for debugging individual functions.
But a product manager asking "what does search cost?" wants the number that includes
everything it triggered downstream.

```python
from spendtrace import get_feature_cost_breakdown

for row in get_feature_cost_breakdown(db_path="cost_data.db"):
    print(
        f"{row['feature']:<25}"
        f"  direct=${row['direct_cost']:.6f}"
        f"  fully_loaded=${row['fully_loaded_cost']:.6f}"
    )
```

```
api                       direct=$0.000001  fully_loaded=$0.018101
search                    direct=$0.005000  fully_loaded=$0.014000
product_details           direct=$0.009000  fully_loaded=$0.009000
```

---

## Cost per HTTP request

Every transaction in a single HTTP request shares a `request_id`. This gives you
the end-to-end cost per call — the number your infrastructure team needs for
capacity planning and pricing.

```python
from spendtrace import get_request_cost

for req in get_request_cost(db_path="cost_data.db", limit=20):
    print(f"{req['endpoint']:<30}  ${req['total_cost']:.6f}/call  ({req['tx_count']} spans)")
```

```
/api/search                   $0.018101/call  (5 spans)
/api/recommend                $0.008400/call  (3 spans)
/api/batch                    $0.000003/call  (2 spans)
```

---

## Drill into a single request

```python
from spendtrace import get_request_subtree

tree = get_request_subtree("req-abc-123", db_path="cost_data.db")
for node in tree:
    indent = "  " * node["depth"]
    print(f"{indent}{node['feature']:<20}  subtree=${node['subtree_cost']:.6f}")
```

```
api                   subtree=$0.018101
  search              subtree=$0.014000
    product_details   subtree=$0.003000
    product_details   subtree=$0.003000
    product_details   subtree=$0.003000
  cache               subtree=$0.000100
```

---

## Spend alerts

```python
from spendtrace import set_alert

set_alert(
    feature="ai_recommendations",
    threshold=10.00,          # USD
    window_hours=24,
    webhook="https://hooks.slack.com/services/...",   # Slack, PagerDuty, or any HTTP POST
)
```

Fires when the 24-hour rolling spend on `ai_recommendations` exceeds $10. Respects
a 1-hour cooldown to avoid alert storms.

---

## Cost trends

```python
from spendtrace import get_cost_trend

for day in get_cost_trend(feature="ai_recommendations", days=30):
    print(day["date"], f"${day['total_cost']:.4f}")
```

---

## Manual instrumentation

`auto_instrument()` patches boto3, openai, and anthropic automatically. For anything
else, use `add_api_call()`:

```python
from spendtrace import cost_track, add_api_call

@cost_track(feature="search")
def search_products(query):
    add_api_call("dynamodb_read", count=3)
    add_api_call("bedrock_claude_3_haiku", input_tokens=512, output_tokens=128)
```

Calls made outside any `@cost_track` scope are attributed to `__unattributed__`
so nothing is silently lost.

---

## Async support

```python
from spendtrace import track

async def process():
    async with track(feature="embeddings", operation="batch"):
        await embed_documents(...)
```

Context propagates correctly across `asyncio` tasks:

```python
from spendtrace import create_task_with_context

task = create_task_with_context(child_coroutine())
```

---

## Sampling and circuit breaker

```python
from spendtrace import set_global_sample_rate, set_circuit_breaker, CircuitBreaker

# Sample 10% of calls in high-traffic paths
set_global_sample_rate(0.10)

# Per-decorator override
@cost_track(feature="search", sample_rate=0.05)
def search(): ...

# Protect your app if the storage layer struggles
set_circuit_breaker(CircuitBreaker(error_threshold=10, recovery_timeout_sec=300))
```

---

## Storage and retention

```python
from spendtrace import SQLiteStorage

storage = SQLiteStorage("cost_data.db")
storage.set_retention(raw_data_days=30, hourly_rollups_days=365, daily_rollups_days=1825)
```

The async logger buffers writes in a background thread and spills to a local overflow
file when the queue is under pressure — no records are silently dropped.

---

## Pricing model

Rates are pulled from the **AWS Pricing API** by default (requires `boto3` and AWS
credentials). Static fallbacks are used automatically if that fails.

| Service | Key | Pricing source |
|---------|-----|----------------|
| DynamoDB reads | `dynamodb_read` | [On-demand pricing](https://aws.amazon.com/dynamodb/pricing/on-demand/) |
| DynamoDB writes | `dynamodb_write` | [On-demand pricing](https://aws.amazon.com/dynamodb/pricing/on-demand/) |
| S3 GET | `s3_get` | [S3 pricing](https://aws.amazon.com/s3/pricing/) |
| S3 PUT | `s3_put` | [S3 pricing](https://aws.amazon.com/s3/pricing/) |
| Lambda requests | `aws_lambda_request` | [Lambda pricing](https://aws.amazon.com/lambda/pricing/) |
| SQS | `sqs_send`, `sqs_receive` | [SQS pricing](https://aws.amazon.com/sqs/pricing/) |
| Bedrock Claude Haiku | `bedrock_claude_3_haiku` | [Bedrock pricing](https://aws.amazon.com/bedrock/pricing/) |
| Bedrock Claude Sonnet | `bedrock_claude_3_sonnet` | [Bedrock pricing](https://aws.amazon.com/bedrock/pricing/) |
| Bedrock Claude Opus | `bedrock_claude_3_opus` | [Bedrock pricing](https://aws.amazon.com/bedrock/pricing/) |
| OpenAI GPT-4 | `openai_gpt4` | [OpenAI pricing](https://openai.com/pricing) |

Override any rate:

```python
from spendtrace import AWSCostModel, get_tracker

model = AWSCostModel()
model.api_costs["dynamodb_read"] = 0.000000275   # your negotiated rate
get_tracker().cost_model = model
```

---

## API server

```bash
uvicorn cost_attribution.api.app:app --port 8000
```

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check |
| `GET /metrics` | Prometheus metrics — includes `cost_total_usd`, `cost_by_feature_usd{feature=...}`, and `cost_async_logger_dropped_total` for alerting |
| `GET /aggregate/feature` | Feature cost summary |
| `GET /aggregate/user` | Per-user/tenant cost summary |
| `GET /api/services` | Per-service API cost breakdown |
| `GET /v2/feature-breakdown` | Fully-loaded vs direct cost per feature |
| `GET /v2/request` | Per-request cost grouped by endpoint |
| `GET /v2/endpoint` | Cost aggregated by endpoint |
| `GET /total` | Total cost with optional filters |
| `GET /transactions` | Raw transaction query |

---

## CLI

```bash
python -m cost_attribution.cli.main --db cost_data.db total
python -m cost_attribution.cli.main --db cost_data.db by-feature
python -m cost_attribution.cli.main --db cost_data.db by-user --limit 10
```

---

## Reconciliation (CLI)

```bash
python scripts/reconcile_aws_costs.py \
    --db cost_data.db \
    --start 2026-02-01 --end 2026-03-01 \
    --tag-key feature \
    --out reports/reconciliation.json
```

---

## Docker

```bash
docker compose up --build
# API:       http://localhost:8000
# Dashboard: http://localhost:8080
```

---

## Tests

```bash
python -m pytest
python scripts/bench_overhead.py
python scripts/bench_storage.py
```

---

## Operations

- `docs/DEPLOYMENT.md` — production deployment guide
- `docs/RUNBOOK.md` — operational runbook
- `docs/MIGRATIONS.md` — schema migration notes
- `CHANGELOG.md` — version history
