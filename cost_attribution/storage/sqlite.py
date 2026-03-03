"""SQLite storage backend for cost attribution."""

import sqlite3
import json
import time
from typing import List, Dict, Any, Optional
from pathlib import Path
from ..core.tracker import CostRecord


class SQLiteStorage:
    """SQLite-based storage for cost records."""
    
    SCHEMA = """
    CREATE TABLE IF NOT EXISTS transactions (
        tx_id TEXT PRIMARY KEY,
        timestamp REAL NOT NULL,
        function_name TEXT NOT NULL,
        feature TEXT,
        user_id TEXT,
        request_id TEXT,
        endpoint TEXT,
        parent_tx_id TEXT,
        duration_ms REAL NOT NULL,
        cpu_time_ms REAL NOT NULL,
        memory_mb REAL NOT NULL,
        allocated_memory_mb REAL,
        network_bytes INTEGER DEFAULT 0,
        api_calls TEXT,  -- JSON
        cpu_cost REAL NOT NULL,
        memory_cost REAL NOT NULL,
        api_cost REAL NOT NULL,
        api_cost_breakdown TEXT,  -- JSON
        total_cost REAL NOT NULL,
        tags TEXT,  -- JSON
        error TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_timestamp ON transactions(timestamp DESC);
    CREATE INDEX IF NOT EXISTS idx_feature ON transactions(feature);
    CREATE INDEX IF NOT EXISTS idx_user_id ON transactions(user_id);
    CREATE INDEX IF NOT EXISTS idx_request_id ON transactions(request_id);
    CREATE INDEX IF NOT EXISTS idx_total_cost ON transactions(total_cost DESC);
    CREATE INDEX IF NOT EXISTS idx_endpoint ON transactions(endpoint);
    CREATE INDEX IF NOT EXISTS idx_error ON transactions(error);
    CREATE INDEX IF NOT EXISTS idx_parent_tx_id ON transactions(parent_tx_id);

    -- Gap 1 fix: rollup tables so data survives past the raw retention window.
    -- hourly_rollup: one row per (hour_bucket, feature, endpoint)
    CREATE TABLE IF NOT EXISTS hourly_rollup (
        hour_bucket  REAL NOT NULL,   -- Unix timestamp truncated to hour
        feature      TEXT,
        endpoint     TEXT,
        tx_count     INTEGER NOT NULL DEFAULT 0,
        error_count  INTEGER NOT NULL DEFAULT 0,
        total_cost   REAL    NOT NULL DEFAULT 0,
        api_cost     REAL    NOT NULL DEFAULT 0,
        cpu_cost     REAL    NOT NULL DEFAULT 0,
        memory_cost  REAL    NOT NULL DEFAULT 0,
        PRIMARY KEY (hour_bucket, feature, endpoint)
    );
    CREATE INDEX IF NOT EXISTS idx_hr_bucket  ON hourly_rollup(hour_bucket DESC);
    CREATE INDEX IF NOT EXISTS idx_hr_feature ON hourly_rollup(feature);

    -- daily_rollup: one row per (day_bucket, feature, endpoint)
    CREATE TABLE IF NOT EXISTS daily_rollup (
        day_bucket   REAL NOT NULL,   -- Unix timestamp truncated to day (UTC midnight)
        feature      TEXT,
        endpoint     TEXT,
        tx_count     INTEGER NOT NULL DEFAULT 0,
        error_count  INTEGER NOT NULL DEFAULT 0,
        total_cost   REAL    NOT NULL DEFAULT 0,
        api_cost     REAL    NOT NULL DEFAULT 0,
        cpu_cost     REAL    NOT NULL DEFAULT 0,
        memory_cost  REAL    NOT NULL DEFAULT 0,
        PRIMARY KEY (day_bucket, feature, endpoint)
    );
    CREATE INDEX IF NOT EXISTS idx_day_bucket  ON daily_rollup(day_bucket DESC);
    CREATE INDEX IF NOT EXISTS idx_day_feature ON daily_rollup(feature);

    CREATE TABLE IF NOT EXISTS retention_policy (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        raw_data_days INTEGER NOT NULL DEFAULT 30,
        hourly_rollups_days INTEGER NOT NULL DEFAULT 365,
        daily_rollups_days INTEGER NOT NULL DEFAULT 1825,
        updated_at REAL NOT NULL
    );
    """
    
    def __init__(self, db_path: str = "cost_attribution.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        """Initialize database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(self.SCHEMA)
            self._ensure_columns(conn)
            # Enable WAL mode for better concurrency
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                INSERT OR IGNORE INTO retention_policy
                (id, raw_data_days, hourly_rollups_days, daily_rollups_days, updated_at)
                VALUES (1, 30, 365, 1825, ?)
                """,
                (time.time(),),
            )
            conn.commit()

    def _ensure_columns(self, conn: sqlite3.Connection):
        """Apply additive schema migrations for existing databases."""
        existing = {row[1] for row in conn.execute("PRAGMA table_info(transactions)").fetchall()}
        if "allocated_memory_mb" not in existing:
            conn.execute("ALTER TABLE transactions ADD COLUMN allocated_memory_mb REAL")
        if "api_cost_breakdown" not in existing:
            conn.execute("ALTER TABLE transactions ADD COLUMN api_cost_breakdown TEXT")
    
    def store(self, record: CostRecord):
        """Store a single cost record."""
        self.store_batch([record])
    
    def store_batch(self, records: List[CostRecord]):
        """Store multiple cost records efficiently."""
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO transactions (
                    tx_id, timestamp, function_name, feature, user_id, request_id, endpoint,
                    parent_tx_id, duration_ms, cpu_time_ms, memory_mb, allocated_memory_mb, network_bytes, api_calls,
                    cpu_cost, memory_cost, api_cost, api_cost_breakdown, total_cost, tags, error
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                [self._record_to_tuple(r) for r in records]
            )
            conn.commit()
    
    def _record_to_tuple(self, record: CostRecord) -> tuple:
        """Convert CostRecord to database tuple."""
        return (
            record.tx_id,
            record.timestamp,
            record.function_name,
            record.feature,
            record.user_id,
            record.request_id,
            record.endpoint,
            record.parent_tx_id,
            record.duration_ms,
            record.cpu_time_ms,
            record.memory_mb,
            record.allocated_memory_mb,
            record.network_bytes,
            json.dumps(record.api_calls) if record.api_calls else None,
            record.cpu_cost,
            record.memory_cost,
            record.api_cost,
            json.dumps(record.api_cost_breakdown) if record.api_cost_breakdown else None,
            record.total_cost,
            json.dumps(record.tags) if record.tags else None,
            record.error,
        )
    
    def query(
        self,
        feature: Optional[str] = None,
        user_id: Optional[str] = None,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Query cost records."""
        conditions = []
        params: List[Any] = []
        
        if feature:
            conditions.append("feature = ?")
            params.append(feature)
        
        if user_id:
            conditions.append("user_id = ?")
            params.append(user_id)
        
        if start_time:
            conditions.append("timestamp >= ?")
            params.append(start_time)
        
        if end_time:
            conditions.append("timestamp <= ?")
            params.append(end_time)
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
            SELECT * FROM transactions
            WHERE {where_clause}
            ORDER BY timestamp DESC
            LIMIT ?
        """
        params.append(limit)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def aggregate_by_feature(
        self,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        include_service_breakdown: bool = True,
    ) -> List[Dict[str, Any]]:
        """Aggregate costs by feature.

        When *include_service_breakdown* is True (default), each row also
        contains a ``service_costs`` dict keyed by AWS service (e.g.
        ``dynamodb``, ``bedrock``, ``s3``) with the USD total for that feature.

        Example row::

            {
                "feature": "ai_recommendations",
                "transaction_count": 412,
                "total_cost": 0.719,
                "service_costs": {
                    "dynamodb": 0.003,
                    "bedrock": 0.714,
                    "s3": 0.002,
                },
            }
        """
        conditions = []
        params = []

        if start_time:
            conditions.append("timestamp >= ?")
            params.append(start_time)

        if end_time:
            conditions.append("timestamp <= ?")
            params.append(end_time)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
            SELECT
                feature,
                COUNT(*) as transaction_count,
                SUM(total_cost) as total_cost,
                AVG(total_cost) as avg_cost,
                MIN(total_cost) as min_cost,
                MAX(total_cost) as max_cost,
                GROUP_CONCAT(api_cost_breakdown) as all_breakdowns
            FROM transactions
            WHERE {where_clause} AND feature IS NOT NULL
            GROUP BY feature
            ORDER BY total_cost DESC
        """

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            rows = [dict(row) for row in cursor.fetchall()]

        if not include_service_breakdown:
            for row in rows:
                row.pop("all_breakdowns", None)
            return rows

        # Build per-service totals from the JSON blobs stored per transaction.
        # GROUP_CONCAT gives us all breakdowns for a feature as a comma-joined
        # string of JSON objects; we parse each one and accumulate.
        for row in rows:
            raw_concat = row.pop("all_breakdowns", None) or ""
            service_costs: Dict[str, float] = {}

            # Each element is a standalone JSON object; they were concatenated
            # by SQLite with commas, so we split carefully.
            if raw_concat:
                # Wrap in array brackets so json.loads handles it
                try:
                    blobs = json.loads(f"[{raw_concat}]")
                except Exception:
                    # Fallback: try splitting on }{
                    import re
                    blobs_raw = re.split(r"(?<=\}),(?=\{)", raw_concat)
                    blobs = []
                    for b in blobs_raw:
                        try:
                            blobs.append(json.loads(b))
                        except Exception:
                            pass

                for breakdown in blobs:
                    if not isinstance(breakdown, dict):
                        continue
                    for service_key, details in breakdown.items():
                        if not isinstance(details, dict):
                            continue
                        # Map granular keys (e.g. dynamodb_read) → service group
                        service_group = _service_group(service_key)
                        cost = float(details.get("total_cost_usd", 0) or 0)
                        service_costs[service_group] = service_costs.get(service_group, 0.0) + cost

            row["service_costs"] = service_costs
        return rows
    
    def aggregate_by_feature_rollup(
        self,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """Aggregate costs by *root* feature using a recursive parent_tx_id walk.

        Unlike :meth:`aggregate_by_feature`, which groups transactions by their
        own ``feature`` column, this method walks the ``parent_tx_id`` tree to
        find each transaction's root ancestor.  The root's feature is then used
        as the attribution bucket.

        This fixes the reconciliation problem where an orchestrator function
        (``feature="api"``) delegates all real work to helpers tagged with
        different features (e.g. ``feature="search"``).  A flat GROUP BY gives
        ``api`` near-zero cost; the recursive rollup correctly attributes the
        full request cost to ``api``.

        Returns rows with the same shape as :meth:`aggregate_by_feature` plus a
        ``root_feature`` key (which equals ``feature`` for root transactions).
        """
        conditions = []
        params: List[Any] = []
        if start_time:
            conditions.append("timestamp >= ?")
            params.append(start_time)
        if end_time:
            conditions.append("timestamp <= ?")
            params.append(end_time)
        where = " AND ".join(conditions) if conditions else "1=1"

        # Recursive CTE: walk parent_tx_id upward to find the root of each tx.
        # root_feature is the feature of whichever ancestor has no parent.
        cte_query = f"""
            WITH RECURSIVE ancestors(tx_id, feature, parent_tx_id, root_feature) AS (
                -- Base: every transaction is its own root candidate
                SELECT tx_id, feature, parent_tx_id, feature
                FROM transactions
                WHERE {where}

                UNION ALL

                -- Recursive: replace root_feature with the parent's feature
                SELECT t.tx_id, t.feature, t.parent_tx_id, p.feature
                FROM transactions t
                JOIN ancestors a ON t.tx_id = a.tx_id
                JOIN transactions p ON a.parent_tx_id = p.tx_id
                WHERE a.parent_tx_id IS NOT NULL
            ),
            -- Keep only the final root_feature for each tx_id
            roots AS (
                SELECT tx_id, root_feature
                FROM ancestors
                GROUP BY tx_id
                HAVING parent_tx_id IS NULL OR parent_tx_id NOT IN (
                    SELECT tx_id FROM transactions WHERE {where}
                )
            )
            SELECT
                COALESCE(r.root_feature, tx.feature) AS root_feature,
                COUNT(*)                              AS transaction_count,
                SUM(tx.total_cost)                   AS total_cost,
                AVG(tx.total_cost)                   AS avg_cost,
                SUM(tx.api_cost)                     AS api_cost
            FROM transactions tx
            LEFT JOIN roots r ON tx.tx_id = r.tx_id
            WHERE {where} AND COALESCE(r.root_feature, tx.feature) IS NOT NULL
            GROUP BY root_feature
            ORDER BY total_cost DESC
        """

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            try:
                cursor = conn.execute(cte_query, params + params + params)
                return [
                    {
                        "feature": dict(row)["root_feature"],
                        "root_feature": dict(row)["root_feature"],
                        "transaction_count": dict(row)["transaction_count"],
                        "total_cost": dict(row)["total_cost"],
                        "avg_cost": dict(row)["avg_cost"],
                        "api_cost": dict(row).get("api_cost", 0.0),
                    }
                    for row in cursor.fetchall()
                ]
            except sqlite3.OperationalError:
                # SQLite version too old for recursive CTEs — fall back to flat
                return self.aggregate_by_feature(
                    start_time=start_time,
                    end_time=end_time,
                    include_service_breakdown=False,
                )

    def aggregate_by_user(
        self,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Aggregate costs by user."""
        conditions = []
        params = []
        
        if start_time:
            conditions.append("timestamp >= ?")
            params.append(start_time)
        
        if end_time:
            conditions.append("timestamp <= ?")
            params.append(end_time)
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
            SELECT
                user_id,
                COUNT(*) as transaction_count,
                SUM(total_cost) as total_cost,
                AVG(total_cost) as avg_cost
            FROM transactions
            WHERE {where_clause} AND user_id IS NOT NULL
            GROUP BY user_id
            ORDER BY total_cost DESC
            LIMIT ?
        """
        params.append(limit)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def aggregate_api_services(
        self,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        tx_limit: int = 1000,
        service_limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Aggregate unit-aware API service usage/cost from recent transactions."""
        rows = self.query(start_time=start_time, end_time=end_time, limit=tx_limit)
        totals: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            raw = row.get("api_cost_breakdown")
            if not raw:
                continue
            try:
                data = json.loads(raw) if isinstance(raw, str) else raw
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            for service, details in data.items():
                if not isinstance(details, dict):
                    continue
                current = totals.get(
                    service,
                    {
                        "service": service,
                        "count_unit": details.get("count_unit", "request"),
                        "count": 0.0,
                        "input_tokens": 0.0,
                        "output_tokens": 0.0,
                        "count_cost_usd": 0.0,
                        "input_cost_usd": 0.0,
                        "output_cost_usd": 0.0,
                        "total_cost_usd": 0.0,
                    },
                )
                current["count"] += float(details.get("count", 0.0) or 0.0)
                current["input_tokens"] += float(details.get("input_tokens", 0.0) or 0.0)
                current["output_tokens"] += float(details.get("output_tokens", 0.0) or 0.0)
                current["count_cost_usd"] += float(details.get("count_cost_usd", 0.0) or 0.0)
                current["input_cost_usd"] += float(details.get("input_cost_usd", 0.0) or 0.0)
                current["output_cost_usd"] += float(details.get("output_cost_usd", 0.0) or 0.0)
                current["total_cost_usd"] += float(details.get("total_cost_usd", 0.0) or 0.0)
                totals[service] = current
        return sorted(totals.values(), key=lambda x: x["total_cost_usd"], reverse=True)[:service_limit]
    
    def get_total_cost(
        self,
        feature: Optional[str] = None,
        user_id: Optional[str] = None,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None
    ) -> float:
        """Get total cost for given criteria."""
        conditions = []
        params: List[Any] = []
        
        if feature:
            conditions.append("feature = ?")
            params.append(feature)
        
        if user_id:
            conditions.append("user_id = ?")
            params.append(user_id)
        
        if start_time:
            conditions.append("timestamp >= ?")
            params.append(start_time)
        
        if end_time:
            conditions.append("timestamp <= ?")
            params.append(end_time)
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"SELECT SUM(total_cost) as total FROM transactions WHERE {where_clause}"
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query, params)
            result = cursor.fetchone()
            return result[0] if result[0] else 0.0

    def set_retention(
        self,
        raw_data_days: int = 30,
        hourly_rollups_days: int = 365,
        daily_rollups_days: int = 1825,
    ):
        """Set retention policy and enforce raw data retention immediately."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO retention_policy
                (id, raw_data_days, hourly_rollups_days, daily_rollups_days, updated_at)
                VALUES (1, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    raw_data_days=excluded.raw_data_days,
                    hourly_rollups_days=excluded.hourly_rollups_days,
                    daily_rollups_days=excluded.daily_rollups_days,
                    updated_at=excluded.updated_at
                """,
                (int(raw_data_days), int(hourly_rollups_days), int(daily_rollups_days), time.time()),
            )
            conn.commit()
        self.cleanup_old_data()

    def get_retention_policy(self) -> Dict[str, int]:
        """Return configured retention policy."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM retention_policy WHERE id = 1").fetchone()
            if not row:
                return {
                    "raw_data_days": 30,
                    "hourly_rollups_days": 365,
                    "daily_rollups_days": 1825,
                }
            return {
                "raw_data_days": int(row["raw_data_days"]),
                "hourly_rollups_days": int(row["hourly_rollups_days"]),
                "daily_rollups_days": int(row["daily_rollups_days"]),
            }

    def _rollup_transactions(self, cutoff: float) -> None:
        """
        Aggregate raw transactions that are about to be deleted into the
        hourly_rollup and daily_rollup tables so long-term history is preserved.

        Called automatically by :meth:`cleanup_old_data` before deletion.
        """
        with sqlite3.connect(self.db_path) as conn:
            # ── Hourly rollup ────────────────────────────────────────────
            conn.execute(
                """
                INSERT INTO hourly_rollup
                    (hour_bucket, feature, endpoint,
                     tx_count, error_count, total_cost,
                     api_cost, cpu_cost, memory_cost)
                SELECT
                    CAST(timestamp / 3600 AS INTEGER) * 3600  AS hour_bucket,
                    feature,
                    endpoint,
                    COUNT(*)                                   AS tx_count,
                    SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END) AS error_count,
                    SUM(total_cost)                            AS total_cost,
                    SUM(api_cost)                              AS api_cost,
                    SUM(cpu_cost)                              AS cpu_cost,
                    SUM(memory_cost)                           AS memory_cost
                FROM transactions
                WHERE timestamp < ?
                GROUP BY hour_bucket, feature, endpoint
                ON CONFLICT(hour_bucket, feature, endpoint) DO UPDATE SET
                    tx_count    = tx_count    + excluded.tx_count,
                    error_count = error_count + excluded.error_count,
                    total_cost  = total_cost  + excluded.total_cost,
                    api_cost    = api_cost    + excluded.api_cost,
                    cpu_cost    = cpu_cost    + excluded.cpu_cost,
                    memory_cost = memory_cost + excluded.memory_cost
                """,
                (cutoff,),
            )
            # ── Daily rollup ─────────────────────────────────────────────
            conn.execute(
                """
                INSERT INTO daily_rollup
                    (day_bucket, feature, endpoint,
                     tx_count, error_count, total_cost,
                     api_cost, cpu_cost, memory_cost)
                SELECT
                    CAST(timestamp / 86400 AS INTEGER) * 86400 AS day_bucket,
                    feature,
                    endpoint,
                    COUNT(*)                                    AS tx_count,
                    SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END) AS error_count,
                    SUM(total_cost)                             AS total_cost,
                    SUM(api_cost)                               AS api_cost,
                    SUM(cpu_cost)                               AS cpu_cost,
                    SUM(memory_cost)                            AS memory_cost
                FROM transactions
                WHERE timestamp < ?
                GROUP BY day_bucket, feature, endpoint
                ON CONFLICT(day_bucket, feature, endpoint) DO UPDATE SET
                    tx_count    = tx_count    + excluded.tx_count,
                    error_count = error_count + excluded.error_count,
                    total_cost  = total_cost  + excluded.total_cost,
                    api_cost    = api_cost    + excluded.api_cost,
                    cpu_cost    = cpu_cost    + excluded.cpu_cost,
                    memory_cost = memory_cost + excluded.memory_cost
                """,
                (cutoff,),
            )
            conn.commit()

    def restate_historical_costs(
        self,
        factor: float,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> int:
        """Scale stored cost columns by *factor* for a date range.

        When reconciliation reveals your model was systematically off, this
        method applies the correction retroactively so historical dashboards
        reflect the calibrated numbers rather than the original wrong rates.

        The operation is a single ``UPDATE … SET … * ?`` — atomic in SQLite,
        fast, and reversible (run again with ``1 / factor``).

        .. warning::
            This mutates stored audit data.  Make a backup of the SQLite file
            before calling this in production, or snapshot the rows into an
            audit table first.  Consider it a deliberate accounting adjustment,
            not a transparent background operation.

        Args:
            factor:     Scalar to multiply into ``total_cost``, ``api_cost``,
                        ``cpu_cost``, and ``memory_cost``.  Use the
                        ``global_calibration_factor`` from a
                        ``ReconciliationReport``, or a per-service factor for
                        surgical corrections.
            start_date: ISO date string ``"YYYY-MM-DD"`` (inclusive).
                        Defaults to the beginning of time.
            end_date:   ISO date string ``"YYYY-MM-DD"`` (exclusive).
                        Defaults to now.

        Returns:
            Number of rows updated.

        Example::

            from cost_attribution import SQLiteStorage
            from cost_attribution.reconciliation.aws import AWSBillingReconciler

            storage = SQLiteStorage("cost_data.db")
            reconciler = AWSBillingReconciler(storage)
            report = reconciler.reconcile("2026-02-01", "2026-03-01")

            # Correct last month's stored records
            updated = storage.restate_historical_costs(
                factor=report.global_calibration_factor,
                start_date="2026-02-01",
                end_date="2026-03-01",
            )
            print(f"Restated {updated} records with factor {report.global_calibration_factor:.4f}")
        """
        if factor <= 0:
            raise ValueError(f"factor must be positive, got {factor}")

        conditions: List[str] = []
        params: List[Any] = []

        if start_date:
            from datetime import datetime, timezone
            start_ts = datetime.strptime(start_date, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            ).timestamp()
            conditions.append("timestamp >= ?")
            params.append(start_ts)

        if end_date:
            from datetime import datetime, timezone
            end_ts = datetime.strptime(end_date, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            ).timestamp()
            conditions.append("timestamp < ?")
            params.append(end_ts)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        sql = f"""
            UPDATE transactions
            SET
                total_cost   = total_cost   * ?,
                api_cost     = api_cost     * ?,
                cpu_cost     = cpu_cost     * ?,
                memory_cost  = memory_cost  * ?
            {where}
        """
        # factor appears 4 times (once per column), then the date params
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(sql, [factor, factor, factor, factor] + params)
            conn.commit()
            return int(cursor.rowcount or 0)

    def cleanup_old_data(self) -> int:
        """Roll up and then delete raw transaction rows per the retention policy.

        The sequence is:
        1. Aggregate rows older than ``raw_data_days`` into the hourly and
           daily rollup tables (so history is not lost).
        2. Delete those raw rows.
        3. Prune rollup rows older than their own retention windows.

        Returns the number of raw rows deleted.
        """
        policy = self.get_retention_policy()
        raw_cutoff = time.time() - (policy["raw_data_days"] * 86400)

        # Step 1 — preserve data in rollup tables before deleting raw rows
        self._rollup_transactions(raw_cutoff)

        # Step 2 — delete expired raw rows
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM transactions WHERE timestamp < ?", (raw_cutoff,)
            )
            conn.commit()
            deleted = int(cursor.rowcount or 0)

        # Step 3 — expire old rollup rows according to their own TTLs
        hourly_cutoff = time.time() - (policy["hourly_rollups_days"] * 86400)
        daily_cutoff  = time.time() - (policy["daily_rollups_days"]  * 86400)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM hourly_rollup WHERE hour_bucket < ?", (hourly_cutoff,)
            )
            conn.execute(
                "DELETE FROM daily_rollup WHERE day_bucket < ?", (daily_cutoff,)
            )
            conn.commit()

        return deleted

    def aggregate_by_error(
        self,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """Aggregate costs grouped by whether a transaction had an error.

        Gap 3 fix: answers "how much did failed requests cost us?" which is
        otherwise invisible even though ``error`` is stored on every row.

        Returns two rows (or one if no errors/successes exist):
          - ``{"error_status": "success", "tx_count": N, "total_cost": X, ...}``
          - ``{"error_status": "error",   "tx_count": N, "total_cost": X, ...}``
        """
        conditions = []
        params: List[Any] = []
        if start_time:
            conditions.append("timestamp >= ?")
            params.append(start_time)
        if end_time:
            conditions.append("timestamp <= ?")
            params.append(end_time)
        where = " AND ".join(conditions) if conditions else "1=1"
        query = f"""
            SELECT
                CASE WHEN error IS NULL THEN 'success' ELSE 'error' END AS error_status,
                COUNT(*)                      AS tx_count,
                SUM(total_cost)               AS total_cost,
                AVG(total_cost)               AS avg_cost,
                SUM(api_cost)                 AS api_cost,
                feature
            FROM transactions
            WHERE {where}
            GROUP BY error_status, feature
            ORDER BY total_cost DESC
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def get_error_cost(
        self,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        feature: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return a simple success/error cost summary dict.

        Gap 3 fix: two-line SQL answer to "what did our errors cost today?"

        Returns::

            {
                "success_cost":  12.34,
                "error_cost":     1.56,
                "error_pct":      11.2,     # % of total spend on failed requests
                "success_count":  9812,
                "error_count":     204,
            }
        """
        conditions = []
        params: List[Any] = []
        if start_time:
            conditions.append("timestamp >= ?")
            params.append(start_time)
        if end_time:
            conditions.append("timestamp <= ?")
            params.append(end_time)
        if feature:
            conditions.append("feature = ?")
            params.append(feature)
        where = " AND ".join(conditions) if conditions else "1=1"
        query = f"""
            SELECT
                SUM(CASE WHEN error IS NULL     THEN total_cost ELSE 0 END) AS success_cost,
                SUM(CASE WHEN error IS NOT NULL THEN total_cost ELSE 0 END) AS error_cost,
                SUM(CASE WHEN error IS NULL     THEN 1 ELSE 0 END)          AS success_count,
                SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END)          AS error_count
            FROM transactions
            WHERE {where}
        """
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(query, params).fetchone()
            success_cost  = float(row[0] or 0)
            error_cost    = float(row[1] or 0)
            success_count = int(row[2] or 0)
            error_count   = int(row[3] or 0)
            total = success_cost + error_cost
            return {
                "success_cost":  round(success_cost,  8),
                "error_cost":    round(error_cost,    8),
                "error_pct":     round(100 * error_cost / total, 2) if total > 0 else 0.0,
                "success_count": success_count,
                "error_count":   error_count,
            }

    def aggregate_by_endpoint(
        self,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Aggregate costs by endpoint (now that Bug 1 is fixed and the column is non-NULL).

        Returns rows sorted by total cost descending, each with:
          - ``endpoint``, ``tx_count``, ``total_cost``, ``avg_cost``,
          - ``error_count``, ``error_pct``
        """
        conditions = ["endpoint IS NOT NULL"]
        params: List[Any] = []
        if start_time:
            conditions.append("timestamp >= ?")
            params.append(start_time)
        if end_time:
            conditions.append("timestamp <= ?")
            params.append(end_time)
        where = " AND ".join(conditions)
        query = f"""
            SELECT
                endpoint,
                COUNT(*)                                                    AS tx_count,
                SUM(total_cost)                                             AS total_cost,
                AVG(total_cost)                                             AS avg_cost,
                SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END)         AS error_count,
                ROUND(100.0 * SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END)
                      / COUNT(*), 2)                                        AS error_pct
            FROM transactions
            WHERE {where}
            GROUP BY endpoint
            ORDER BY total_cost DESC
            LIMIT ?
        """
        params.append(limit)
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def query_daily_rollup(
        self,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        feature: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Query the daily rollup table for long-term trend data.

        Falls back gracefully if no rollup data exists yet (newly deployed DB).
        Rows have the same shape as :meth:`get_cost_trend` results.
        """
        conditions = []
        params: List[Any] = []
        if start_time:
            conditions.append("day_bucket >= ?")
            params.append(start_time)
        if end_time:
            conditions.append("day_bucket <= ?")
            params.append(end_time)
        if feature is not None:
            if feature == "":
                conditions.append("feature IS NULL")
            else:
                conditions.append("feature = ?")
                params.append(feature)
        where = " AND ".join(conditions) if conditions else "1=1"
        query = f"""
            SELECT
                day_bucket,
                feature,
                endpoint,
                SUM(tx_count)    AS tx_count,
                SUM(error_count) AS error_count,
                SUM(total_cost)  AS total_cost,
                SUM(api_cost)    AS api_cost
            FROM daily_rollup
            WHERE {where}
            GROUP BY day_bucket, feature, endpoint
            ORDER BY day_bucket DESC
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            try:
                cursor = conn.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]
            except sqlite3.OperationalError:
                return []

    # ------------------------------------------------------------------
    # Graph-aware queries  (the call-tree payoff)
    # ------------------------------------------------------------------

    def get_feature_cost_breakdown(
        self,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """Return direct *and* fully-loaded cost for every feature.

        **Direct cost** is what the feature spent on its own ``add_api_call``
        recordings — the number already returned by ``aggregate_by_feature``.

        **Fully-loaded cost** sums the direct cost of the feature's own
        transactions *plus* the cost of every descendant transaction they
        triggered (via ``parent_tx_id`` links), regardless of what feature
        label the children carry.

        Example — given the tree::

            api ($0.000001)
            ├── search ($0.005)
            │   ├── product_details ($0.003)  ×3
            └── cache ($0.0001)

        Results::

            feature          direct      fully_loaded   children
            api              $0.000001   $0.0181         $0.0181
            search           $0.005      $0.014          $0.009
            product_details  $0.009      $0.009          $0.000
            cache            $0.0001     $0.0001         $0.000

        The recursive CTE walks **downward** from every transaction to all its
        descendants and computes a per-root subtree total.  Each feature row
        then joins that subtree total against its own direct cost.

        Returns rows sorted by ``fully_loaded_cost`` descending, each with:
        ``feature``, ``direct_cost``, ``fully_loaded_cost``, ``children_cost``,
        ``transaction_count``, ``direct_tx_count``.
        """
        conditions: List[str] = []
        params: List[Any] = []
        if start_time:
            conditions.append("timestamp >= ?")
            params.append(start_time)
        if end_time:
            conditions.append("timestamp <= ?")
            params.append(end_time)
        where = " AND ".join(conditions) if conditions else "1=1"

        # The recursive CTE walks from every root downward to all descendants.
        # We double the params list because WHERE appears twice in the CTE.
        query = f"""
            WITH RECURSIVE subtree(root_tx_id, node_tx_id, node_cost) AS (
                -- Anchor: every qualifying transaction seeds its own subtree
                SELECT tx_id, tx_id, total_cost
                FROM   transactions
                WHERE  {where}

                UNION ALL

                -- Recursive: for each node already in a subtree,
                --             pull in its direct children
                SELECT s.root_tx_id, t.tx_id, t.total_cost
                FROM   transactions t
                JOIN   subtree s ON t.parent_tx_id = s.node_tx_id
                WHERE  {where}
            ),
            -- Collapse subtree costs to one number per root transaction
            subtree_totals AS (
                SELECT root_tx_id, SUM(node_cost) AS subtree_cost
                FROM   subtree
                GROUP BY root_tx_id
            )
            SELECT
                tx.feature,
                COUNT(*)                           AS direct_tx_count,
                SUM(tx.total_cost)                 AS direct_cost,
                -- fully_loaded = sum of each transaction's subtree
                SUM(COALESCE(st.subtree_cost, tx.total_cost)) AS fully_loaded_cost
            FROM   transactions tx
            LEFT JOIN subtree_totals st ON st.root_tx_id = tx.tx_id
            WHERE  {where} AND tx.feature IS NOT NULL
            GROUP BY tx.feature
            ORDER BY fully_loaded_cost DESC
        """

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            try:
                cursor = conn.execute(query, params + params + params)
                rows = []
                for row in cursor.fetchall():
                    d = dict(row)
                    direct       = float(d.get("direct_cost", 0) or 0)
                    fully_loaded = float(d.get("fully_loaded_cost", 0) or 0)
                    rows.append({
                        "feature":           d["feature"],
                        "direct_cost":       round(direct, 8),
                        "fully_loaded_cost": round(fully_loaded, 8),
                        "children_cost":     round(max(0.0, fully_loaded - direct), 8),
                        "direct_tx_count":   int(d.get("direct_tx_count", 0) or 0),
                    })
                return rows
            except sqlite3.OperationalError:
                # Fallback for environments without recursive CTE support
                return [
                    {
                        "feature":           r["feature"],
                        "direct_cost":       round(float(r.get("total_cost", 0) or 0), 8),
                        "fully_loaded_cost": round(float(r.get("total_cost", 0) or 0), 8),
                        "children_cost":     0.0,
                        "direct_tx_count":   int(r.get("transaction_count", 0) or 0),
                    }
                    for r in self.aggregate_by_feature(
                        start_time=start_time, end_time=end_time,
                        include_service_breakdown=False,
                    )
                ]

    def aggregate_by_request(
        self,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """Cost per HTTP request — every transaction in a request shares a ``request_id``.

        This is the number your infrastructure team needs for capacity planning
        and pricing: "our ``/api/search`` endpoint costs $0.0000010 per call."

        Returns rows sorted by ``total_cost`` descending, each with:
        ``request_id``, ``endpoint``, ``total_cost``, ``api_cost``,
        ``tx_count``, ``error_count``, ``started_at``.

        Only requests with a non-NULL ``request_id`` are returned.
        """
        conditions = ["request_id IS NOT NULL"]
        params: List[Any] = []
        if start_time:
            conditions.append("timestamp >= ?")
            params.append(start_time)
        if end_time:
            conditions.append("timestamp <= ?")
            params.append(end_time)
        where = " AND ".join(conditions)

        query = f"""
            SELECT
                request_id,
                -- Use the endpoint from the root transaction (no parent)
                MAX(CASE WHEN parent_tx_id IS NULL THEN endpoint END) AS endpoint,
                SUM(total_cost)                                         AS total_cost,
                SUM(api_cost)                                           AS api_cost,
                COUNT(*)                                                AS tx_count,
                SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END)     AS error_count,
                MIN(timestamp)                                          AS started_at
            FROM   transactions
            WHERE  {where}
            GROUP BY request_id
            ORDER BY total_cost DESC
            LIMIT  ?
        """
        params.append(limit)

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def get_request_subtree(
        self,
        request_id: str,
    ) -> List[Dict[str, Any]]:
        """Return the full annotated call tree for a single HTTP request.

        Each node in the returned list represents one transaction and carries:
        ``tx_id``, ``parent_tx_id``, ``depth``, ``function_name``, ``feature``,
        ``total_cost``, ``api_cost``, ``duration_ms``, ``error``,
        ``subtree_cost`` (its own cost + all descendants).

        Nodes are ordered depth-first (parent before children) so callers can
        render the tree by iterating the list in order.

        Example output for a single search request::

            depth  feature          function_name    direct    subtree
            0      api              handle_api       $0.000001 $0.0181
            1      search           search_products  $0.005    $0.014
            2      product_details  product_details  $0.003    $0.003
            2      product_details  product_details  $0.003    $0.003
            2      product_details  product_details  $0.003    $0.003
            1      cache            cache_lookup     $0.0001   $0.0001
        """
        # Step 1 — fetch all transactions for this request (flat)
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT tx_id, parent_tx_id, function_name, feature,
                       total_cost, api_cost, duration_ms, error, timestamp
                FROM   transactions
                WHERE  request_id = ?
                ORDER BY timestamp ASC
                """,
                (request_id,),
            ).fetchall()

        if not rows:
            return []

        # Step 2 — build an in-Python tree: {tx_id: {node data, children: [...]}}
        nodes: Dict[str, Any] = {}
        for row in rows:
            d = dict(row)
            d["children"] = []
            d["depth"] = 0
            d["subtree_cost"] = float(d.get("total_cost") or 0)
            nodes[d["tx_id"]] = d

        # Attach children to parents
        roots: List[str] = []
        for node in nodes.values():
            pid = node.get("parent_tx_id")
            if pid and pid in nodes:
                nodes[pid]["children"].append(node["tx_id"])
            else:
                roots.append(node["tx_id"])

        # Step 3 — two-pass DFS:
        #   Pass 1 (post-order): compute subtree_cost bottom-up for every node
        #   Pass 2 (pre-order):  emit nodes parent-first
        result: List[Dict[str, Any]] = []

        def _compute_subtree(tx_id: str) -> float:
            node = nodes[tx_id]
            child_total = sum(_compute_subtree(c) for c in node["children"])
            node["subtree_cost"] = round(float(node.get("total_cost") or 0) + child_total, 8)
            return node["subtree_cost"]

        def _emit_preorder(tx_id: str, depth: int) -> None:
            node = nodes[tx_id]
            node["depth"] = depth
            result.append({k: v for k, v in node.items() if k != "children"})
            for child_id in node["children"]:
                _emit_preorder(child_id, depth + 1)

        # Walk roots in timestamp order (already sorted above)
        for root_id in sorted(roots, key=lambda tid: nodes[tid].get("timestamp", 0)):
            _compute_subtree(root_id)
            _emit_preorder(root_id, depth=0)

        return result

    def get_transaction_subtree(
        self,
        tx_id: str,
    ) -> List[Dict[str, Any]]:
        """Return the subtree rooted at a specific transaction.

        Useful for drilling into one feature call to see exactly what it
        triggered and how much each step cost.

        Returns the same shape as :meth:`get_request_subtree` but scoped to
        a single root transaction and all its descendants.
        """
        # Resolve the request_id so we can load the full context, then filter
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT request_id FROM transactions WHERE tx_id = ?", (tx_id,)
            ).fetchone()
        if not row:
            return []

        request_id = row[0]
        if request_id:
            # Load via request subtree and filter to the requested subtree
            full_tree = self.get_request_subtree(request_id)
        else:
            # No request context — fall back to loading just this tx and children
            full_tree = self._load_subtree_by_parent(tx_id)

        # Find the target node in the tree and re-root the list at it
        if not full_tree:
            return []

        # Collect the target tx_id and all its descendants from the flat list
        # by walking the pre-order list: once we see our root, collect until
        # we reach a sibling at the same or lower depth.
        target_depth: Optional[int] = None
        in_subtree = False
        filtered: List[Dict[str, Any]] = []
        for node in full_tree:
            if node["tx_id"] == tx_id:
                target_depth = node["depth"]
                in_subtree = True
            if not in_subtree:
                continue
            # Stop once we reach a sibling or ancestor of our root
            # (but always include the root itself)
            if node["tx_id"] != tx_id and target_depth is not None and node["depth"] <= target_depth:
                break
            node_copy = dict(node)
            node_copy["depth"] = node["depth"] - (target_depth or 0)
            filtered.append(node_copy)

        return filtered

    def _load_subtree_by_parent(self, root_tx_id: str) -> List[Dict[str, Any]]:
        """Load a subtree when there is no request_id to anchor on."""
        # Iteratively load children level by level
        collected: Dict[str, Any] = {}
        frontier = [root_tx_id]
        while frontier:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                placeholders = ",".join("?" * len(frontier))
                rows = conn.execute(
                    f"""
                    SELECT tx_id, parent_tx_id, function_name, feature,
                           total_cost, api_cost, duration_ms, error, timestamp
                    FROM   transactions
                    WHERE  tx_id IN ({placeholders})
                       OR  parent_tx_id IN ({placeholders})
                    """,
                    frontier + frontier,
                ).fetchall()
            new_frontier = []
            for row in rows:
                d = dict(row)
                if d["tx_id"] not in collected:
                    d["children"] = []
                    d["depth"] = 0
                    d["subtree_cost"] = float(d.get("total_cost") or 0)
                    collected[d["tx_id"]] = d
                    if d["tx_id"] != root_tx_id and d["parent_tx_id"] in collected:
                        new_frontier.append(d["tx_id"])
            frontier = new_frontier

        # Re-use the two-pass DFS logic
        nodes = collected
        roots = [root_tx_id] if root_tx_id in nodes else []
        for node in nodes.values():
            pid = node.get("parent_tx_id")
            if pid and pid in nodes and node["tx_id"] != root_tx_id:
                nodes[pid]["children"].append(node["tx_id"])

        result: List[Dict[str, Any]] = []

        def _compute(tid: str) -> float:
            node = nodes[tid]
            child_total = sum(_compute(c) for c in node.get("children", []))
            node["subtree_cost"] = round(float(node.get("total_cost") or 0) + child_total, 8)
            return node["subtree_cost"]

        def _emit(tid: str, depth: int) -> None:
            node = nodes[tid]
            node["depth"] = depth
            result.append({k: v for k, v in node.items() if k != "children"})
            for child_id in node.get("children", []):
                _emit(child_id, depth + 1)

        for rid in roots:
            _compute(rid)
            _emit(rid, 0)
        return result


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

_SERVICE_GROUP_MAP: Dict[str, str] = {
    "dynamodb_read": "dynamodb",
    "dynamodb_write": "dynamodb",
    "dynamodb_query": "dynamodb",
    "s3_get": "s3",
    "s3_put": "s3",
    "s3_list": "s3",
    "sqs_send": "sqs",
    "sqs_receive": "sqs",
    "sns_publish": "sns",
    "api_gateway_request": "api_gateway",
    "aws_lambda_request": "lambda",
    "bedrock_claude_3_haiku": "bedrock",
    "bedrock_claude_3_sonnet": "bedrock",
    "bedrock_claude_3_opus": "bedrock",
    "openai_gpt4": "openai",
    "openai_gpt4_turbo": "openai",
    "anthropic_claude": "anthropic",
}


def _service_group(service_key: str) -> str:
    """Map a granular service key to its top-level service name."""
    return _SERVICE_GROUP_MAP.get(service_key, service_key.split("_")[0])
