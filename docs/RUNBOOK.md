# Operations Runbook

## Health checks

- API health: `GET /health`
- API metrics: `GET /metrics`

## Common checks

1. API status
```bash
curl http://localhost:8000/health
```

2. Metrics output
```bash
curl http://localhost:8000/metrics
```

3. Cost totals
```bash
python -m cost_attribution.cli.main --db cost_data.db total
```

## Incident triage

1. Check container logs:
```bash
docker compose logs -f cost-api
```

2. Check dropped records metric:
- `cost_async_logger_dropped_total`

3. If queue pressure is high:
- Increase logger buffer size in code/config.
- Reduce instrumentation scope temporarily.

4. If storage write errors increase:
- Validate DB path permissions.
- Validate SQLite file lock contention.

## Safe maintenance

- Stop writes before manual DB file operations.
- Back up SQLite DB before migration/cleanup tasks.
