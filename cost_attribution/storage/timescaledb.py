"""TimescaleDB storage backend for cost attribution."""

import json
import time
from typing import Any, Dict, List, Optional

from ..core.tracker import CostRecord

try:
    import psycopg2  # type: ignore[import-untyped]
    from psycopg2.extras import Json, RealDictCursor  # type: ignore[import-untyped]
except Exception:  # pragma: no cover - optional dependency
    psycopg2 = None
    Json = None
    RealDictCursor = None


class TimescaleDBStorage:
    """TimescaleDB backend with a compatible API to SQLiteStorage."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "cost_attribution",
        user: str = "postgres",
        password: str = "",
    ):
        if psycopg2 is None:
            raise ImportError(
                "TimescaleDBStorage requires psycopg2-binary. Install with: pip install psycopg2-binary"
            )
        self._conn_params = {
            "host": host,
            "port": port,
            "dbname": database,
            "user": user,
            "password": password,
        }
        self._init_db()

    def _connect(self):
        return psycopg2.connect(**self._conn_params)

    def _init_db(self):
        schema = """
        CREATE TABLE IF NOT EXISTS transactions (
            tx_id TEXT PRIMARY KEY,
            timestamp DOUBLE PRECISION NOT NULL,
            function_name TEXT NOT NULL,
            feature TEXT,
            user_id TEXT,
            request_id TEXT,
            endpoint TEXT,
            parent_tx_id TEXT,
            duration_ms DOUBLE PRECISION NOT NULL,
            cpu_time_ms DOUBLE PRECISION NOT NULL,
            memory_mb DOUBLE PRECISION NOT NULL,
            allocated_memory_mb DOUBLE PRECISION,
            network_bytes BIGINT DEFAULT 0,
            api_calls JSONB,
            cpu_cost DOUBLE PRECISION NOT NULL,
            memory_cost DOUBLE PRECISION NOT NULL,
            api_cost DOUBLE PRECISION NOT NULL,
            api_cost_breakdown JSONB,
            total_cost DOUBLE PRECISION NOT NULL,
            tags JSONB,
            error TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions (timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_transactions_feature ON transactions (feature);
        CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions (user_id);
        CREATE INDEX IF NOT EXISTS idx_transactions_request_id ON transactions (request_id);
        CREATE INDEX IF NOT EXISTS idx_transactions_total_cost ON transactions (total_cost DESC);
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(schema)
            conn.commit()

    def store(self, record: CostRecord):
        self.store_batch([record])

    def store_batch(self, records: List[CostRecord]):
        if not records:
            return
        query = """
        INSERT INTO transactions VALUES (
            %(tx_id)s, %(timestamp)s, %(function_name)s, %(feature)s, %(user_id)s, %(request_id)s,
            %(endpoint)s, %(parent_tx_id)s, %(duration_ms)s, %(cpu_time_ms)s, %(memory_mb)s, %(allocated_memory_mb)s, %(network_bytes)s,
            %(api_calls)s, %(cpu_cost)s, %(memory_cost)s, %(api_cost)s, %(api_cost_breakdown)s, %(total_cost)s, %(tags)s, %(error)s
        )
        ON CONFLICT (tx_id) DO UPDATE SET
            timestamp = EXCLUDED.timestamp,
            function_name = EXCLUDED.function_name,
            feature = EXCLUDED.feature,
            user_id = EXCLUDED.user_id,
            request_id = EXCLUDED.request_id,
            endpoint = EXCLUDED.endpoint,
            parent_tx_id = EXCLUDED.parent_tx_id,
            duration_ms = EXCLUDED.duration_ms,
            cpu_time_ms = EXCLUDED.cpu_time_ms,
            memory_mb = EXCLUDED.memory_mb,
            allocated_memory_mb = EXCLUDED.allocated_memory_mb,
            network_bytes = EXCLUDED.network_bytes,
            api_calls = EXCLUDED.api_calls,
            cpu_cost = EXCLUDED.cpu_cost,
            memory_cost = EXCLUDED.memory_cost,
            api_cost = EXCLUDED.api_cost,
            api_cost_breakdown = EXCLUDED.api_cost_breakdown,
            total_cost = EXCLUDED.total_cost,
            tags = EXCLUDED.tags,
            error = EXCLUDED.error
        """
        payload = [self._record_to_dict(r) for r in records]
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, payload)
            conn.commit()

    def _record_to_dict(self, record: CostRecord) -> Dict[str, Any]:
        return {
            "tx_id": record.tx_id,
            "timestamp": record.timestamp,
            "function_name": record.function_name,
            "feature": record.feature,
            "user_id": record.user_id,
            "request_id": record.request_id,
            "endpoint": record.endpoint,
            "parent_tx_id": record.parent_tx_id,
            "duration_ms": record.duration_ms,
            "cpu_time_ms": record.cpu_time_ms,
            "memory_mb": record.memory_mb,
            "allocated_memory_mb": record.allocated_memory_mb,
            "network_bytes": record.network_bytes,
            "api_calls": Json(record.api_calls) if Json else json.dumps(record.api_calls),
            "cpu_cost": record.cpu_cost,
            "memory_cost": record.memory_cost,
            "api_cost": record.api_cost,
            "api_cost_breakdown": Json(record.api_cost_breakdown) if Json else json.dumps(record.api_cost_breakdown),
            "total_cost": record.total_cost,
            "tags": Json(record.tags) if Json else json.dumps(record.tags),
            "error": record.error,
        }

    def query(
        self,
        feature: Optional[str] = None,
        user_id: Optional[str] = None,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        conditions = []
        params: List[Any] = []

        if feature is not None:
            conditions.append("feature = %s")
            params.append(feature)
        if user_id is not None:
            conditions.append("user_id = %s")
            params.append(user_id)
        if start_time is not None:
            conditions.append("timestamp >= %s")
            params.append(start_time)
        if end_time is not None:
            conditions.append("timestamp <= %s")
            params.append(end_time)

        where = " AND ".join(conditions) if conditions else "TRUE"
        sql = f"SELECT * FROM transactions WHERE {where} ORDER BY timestamp DESC LIMIT %s"
        params.append(limit)

        with self._connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(row) for row in cur.fetchall()]

    def aggregate_by_feature(
        self, start_time: Optional[float] = None, end_time: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        conditions = ["feature IS NOT NULL"]
        params: List[Any] = []
        if start_time is not None:
            conditions.append("timestamp >= %s")
            params.append(start_time)
        if end_time is not None:
            conditions.append("timestamp <= %s")
            params.append(end_time)
        where = " AND ".join(conditions)
        sql = f"""
            SELECT
                feature,
                COUNT(*) AS transaction_count,
                SUM(total_cost) AS total_cost,
                AVG(total_cost) AS avg_cost,
                MIN(total_cost) AS min_cost,
                MAX(total_cost) AS max_cost
            FROM transactions
            WHERE {where}
            GROUP BY feature
            ORDER BY total_cost DESC
        """
        with self._connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(row) for row in cur.fetchall()]

    def aggregate_by_user(
        self, start_time: Optional[float] = None, end_time: Optional[float] = None, limit: int = 100
    ) -> List[Dict[str, Any]]:
        conditions = ["user_id IS NOT NULL"]
        params: List[Any] = []
        if start_time is not None:
            conditions.append("timestamp >= %s")
            params.append(start_time)
        if end_time is not None:
            conditions.append("timestamp <= %s")
            params.append(end_time)
        where = " AND ".join(conditions)
        sql = f"""
            SELECT
                user_id,
                COUNT(*) AS transaction_count,
                SUM(total_cost) AS total_cost,
                AVG(total_cost) AS avg_cost
            FROM transactions
            WHERE {where}
            GROUP BY user_id
            ORDER BY total_cost DESC
            LIMIT %s
        """
        params.append(limit)
        with self._connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(row) for row in cur.fetchall()]

    def get_total_cost(
        self,
        feature: Optional[str] = None,
        user_id: Optional[str] = None,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
    ) -> float:
        conditions = []
        params: List[Any] = []
        if feature is not None:
            conditions.append("feature = %s")
            params.append(feature)
        if user_id is not None:
            conditions.append("user_id = %s")
            params.append(user_id)
        if start_time is not None:
            conditions.append("timestamp >= %s")
            params.append(start_time)
        if end_time is not None:
            conditions.append("timestamp <= %s")
            params.append(end_time)
        where = " AND ".join(conditions) if conditions else "TRUE"
        sql = f"SELECT COALESCE(SUM(total_cost), 0.0) AS total FROM transactions WHERE {where}"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                return float(row[0] if row else 0.0)

    def set_retention(
        self, raw_data_days: int = 30, hourly_rollups_days: int = 365, daily_rollups_days: int = 1825
    ):
        """Apply raw data retention policy by deleting old rows."""
        del hourly_rollups_days, daily_rollups_days
        cutoff = time.time() - (int(raw_data_days) * 24 * 3600)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM transactions WHERE timestamp < %s", (cutoff,))
            conn.commit()
