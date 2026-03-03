# Changelog

## [1.2.0] - 2026-03-01

### Added

**Graph-aware cost queries** (`cost_attribution/graph.py`)
- `get_feature_cost_breakdown()` — fully-loaded vs direct cost per feature, using the `parent_tx_id` call tree. Answers "what does *search* actually cost including everything it triggers?"
- `get_request_cost()` — total cost per HTTP request grouped by `request_id`, with endpoint, span count, and error count.
- `get_request_subtree(request_id)` — depth-first call tree for a single request with subtree costs at each node, the primary debugging entry point.
- `get_transaction_subtree(tx_id)` — subtree rooted at any specific transaction.
- All four functions exported from the top-level `cost_attribution` namespace.

**Auto-instrumentation** (`cost_attribution/auto_instrument.py`)
- `auto_instrument()` — patches boto3 (`BaseClient._make_api_call`), openai, and anthropic SDKs at startup. No per-function `add_api_call()` required inside instrumented scopes.
- Captures token usage (`input_tokens`, `output_tokens`) from Bedrock and OpenAI responses automatically.
- Detects model ID from Bedrock `modelId` param to select the correct pricing key (haiku/sonnet/opus).
- Calls outside any `@cost_track` scope are attributed to `__unattributed__`.
- `is_instrumented()` guard — idempotent, safe to call multiple times.

**Spend alerts and cost trends** (`cost_attribution/alerts.py`)
- `set_alert(feature, threshold, window_hours, webhook)` — threshold-based spend alerting with configurable rolling window and cooldown. Fires to Slack, PagerDuty, or any HTTP POST webhook.
- `get_cost_trend(feature, days)` — daily cost totals for a feature over a look-back window, with zero-filled gaps.
- Both exported from top-level namespace.

**Reconciliation improvements** (`cost_attribution/reconciliation/`)
- `reconcile(db_path, start, end)` — new top-level convenience function, exported as `from cost_attribution import reconcile`.
- `reconcile()` returns a `ReconcileReport` with `.summary()` for human-readable output and `.to_dict()` for structured access.
- `AWSBillingReconciler._actual_by_service()` now groups Cost Explorer by `DIMENSION: SERVICE` and maps internal keys to CE service names (Amazon DynamoDB, Amazon S3, etc.) before computing calibration factors — fixes the previous issue where the global factor absorbed unrelated charges (support plan, data transfer, taxes).
- `SQLiteStorage.restate_historical_costs(factor, start_date, end_date)` — retroactively scales stored cost columns so historical dashboards reflect calibrated numbers after a reconciliation run. Atomic `UPDATE`, reversible by applying `1/factor`.

**Schema and index improvements** (`cost_attribution/storage/sqlite.py`)
- Added `idx_parent_tx_id` index — required for efficient recursive CTE traversal of the call tree.
- Added `idx_endpoint` index — enables fast endpoint-level aggregation.
- Added `idx_error` index — enables fast failure queries.
- `aggregate_by_feature()` now returns a `service_costs` dict (e.g. `{"dynamodb": 0.041, "bedrock": 0.714}`) grouping API costs by service family.
- New `get_feature_cost_breakdown()`, `aggregate_by_request()`, `aggregate_by_endpoint()`, `get_request_subtree()`, `get_transaction_subtree()` methods on `SQLiteStorage`.

**Reliability: overflow buffer replaces silent drops** (`cost_attribution/utils/async_logger.py`)
- The previous `put_nowait` path silently dropped records when the queue was full. The queue now uses a short-timeout `put()` first, then spills to an on-disk SQLite overflow buffer. The worker drains the overflow back into the main pipeline periodically.
- `cost_async_logger_dropped_total` counter is pre-initialized to zero at startup so Prometheus alerting rules fire correctly even before the first event.
- `cost_async_logger_overflow_total` and `cost_async_logger_overflow_replayed_total` added for operational visibility.

**Cost metrics in Prometheus** (`cost_attribution/utils/async_logger.py`)
- `cost_total_usd` counter — cumulative USD tracked, updated on every flush.
- `cost_by_feature_usd{feature="..."}` counter — per-feature cumulative USD.
- Both visible at the `/metrics` endpoint alongside existing instrumentation health counters.

**API improvements** (`cost_attribution/api/app.py`)
- Storage is now cached per `db_path` — eliminates the previous pattern of creating a new `SQLiteStorage` (and running schema checks) on every HTTP request.
- New `/v2/feature-breakdown`, `/v2/request`, `/v2/endpoint`, `/v2/user` endpoints exposing the graph query results.

### Fixed
- `aggregate_by_feature()` `service_costs` key was missing — now populated correctly.
- Reconciliation global calibration factor inflated by non-service charges (support plan, data transfer) — now computed per-service against matching Cost Explorer line items.
- README `row["service_costs"]` and `from cost_attribution import reconcile` examples were broken — both now work correctly.
- README `set_retention(days=30)` example updated to `set_retention(raw_data_days=30, ...)`.

### Verified (2026-03-01)
- All graph queries confirmed working against live SQLite data
- Auto-instrumentation idempotency confirmed
- `restate_historical_costs` 2× scaling confirmed exact
- Overflow buffer spill and replay confirmed
- `cost_total_usd` and `cost_by_feature_usd` confirmed emitting from flush path
- API storage cache confirmed (same object returned on second call)
- All three new indexes confirmed in schema

---

## [1.1.0] - 2026-02-11

### Added
- TimescaleDB backend module (`TimescaleDBStorage`).
- Circuit breaker utility and instrumentation integration.
- SQLite retention APIs: `set_retention`, `get_retention_policy`, `cleanup_old_data`.
- `redact_args` support in `@cost_track`.
- Minimal FastAPI API app, CLI, and dashboard app.
- Observability utilities:
  - structured JSON logging helpers
  - in-process metrics registry
  - `/metrics` endpoint on API
- Pricing provider abstraction with dynamic AWS pricing pulls (`AWSDynamicPricingProvider`) and static fallback.
- AWS billing reconciliation module and job script (`AWSBillingReconciler`, `scripts/reconcile_aws_costs.py`) for modeled-vs-actual calibration factors.
- Unit-aware API service cost metadata in transaction breakdowns, reconciliation reports, and dashboard views.
- Added `GET /api/services` endpoint exposing the same unit-aware service breakdown JSON as dashboard.
- Added fleet-wide sampling controls (`set_global_sample_rate`, sampling policy hook, env reload via `COST_ATTRIBUTION_SAMPLE_RATE`).
- Benchmark scripts for overhead and storage throughput.
- Example scripts for basic/FastAPI/Flask usage.
- Operational docs and release checklist.

### Changed
- `complete_example.py` made cross-platform and runnable on Windows.
- README rewritten to match validated behavior and run commands.

### Fixed
- Stale async logger reference in instrumentation path.
- Unicode console portability issues in demo output.
- AWS baseline pricing defaults corrected for Lambda-like compute scale and lambda request cost.
- API usage payload supports token-billed LLM calls (`input_tokens`, `output_tokens`) in cost calculations.
- Cost tracker now supports memory modes (`process`, `tracemalloc`, `none`) and persists `allocated_memory_mb` for lambda GB-second billing alignment.

### Verification
- Verified on 2026-02-11 (local + Docker Desktop):
  - `python -m pytest` -> 9 passed
  - `python complete_example.py` -> success
  - `docker compose build` -> success
  - `docker compose up -d --no-build` -> success
  - `GET /health` -> `{"status":"ok"}`
  - `GET /metrics` -> non-empty Prometheus text
  - Dashboard `http://localhost:8080/` -> HTTP 200