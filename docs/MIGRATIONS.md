# Data Migration and Retention

## Schema management

SQLite schema is created automatically by `SQLiteStorage`.

For controlled upgrades, use additive changes first:
1. Add new nullable columns.
2. Backfill data in batches.
3. Update readers/writers.
4. Only then enforce constraints.

## Retention

Set policy in code:

```python
from cost_attribution import SQLiteStorage

storage = SQLiteStorage("cost_data.db")
storage.set_retention(raw_data_days=30, hourly_rollups_days=365, daily_rollups_days=1825)
```

Run cleanup manually:

```python
deleted = storage.cleanup_old_data()
print(deleted)
```

## Backup

For SQLite:
1. Pause write load if possible.
2. Copy DB file and WAL files together.
3. Validate by opening backup with sqlite client.
