# Deployment Guide

## Local (Docker Compose)

```bash
docker compose up --build
```

Services:
- API: http://localhost:8000
- Dashboard: http://localhost:8080

Metrics endpoint:
- http://localhost:8000/metrics

## Local (without Docker)

```bash
python -m pip install -r requirements.txt
python -m uvicorn cost_attribution.api.app:app --host 0.0.0.0 --port 8000
python -m uvicorn cost_attribution.dashboard.app:app --host 0.0.0.0 --port 8080
```

For local development tooling (pytest/ruff/mypy/black), install:

```bash
python -m pip install -r requirements-dev.txt
```

## Environment variables

- `COST_ATTRIBUTION_JSON_LOGS=true|false`
- `COST_ATTRIBUTION_LOG_LEVEL=DEBUG|INFO|WARNING|ERROR`
- `COST_ATTRIBUTION_DB_ROOT=/absolute/path` (restricts `db_path` query params to this root)

## Data path

Use `db_path` query parameter when calling API endpoints, for example:

```text
GET /transactions?db_path=/app/data/cost_data.db
```
