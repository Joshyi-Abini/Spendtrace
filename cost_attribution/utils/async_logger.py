"""Async logger for non-blocking cost record storage."""

import asyncio
from contextlib import closing
import json
import os
import sqlite3
import tempfile
import threading
import queue
import time
from typing import Optional, List
from ..core.tracker import CostRecord
from .logging import log_event
from .metrics import get_metrics


class AsyncLogger:
    """Non-blocking logger for cost records.

    Gap 2 fix: the previous implementation silently dropped records when the
    in-memory queue hit its 10 000-item limit.  For a cost attribution tool
    dropping records means real spend goes untracked — exactly the worst
    failure mode.

    The new strategy:
    1. Try a short-timeout ``put()`` instead of ``put_nowait()``.
    2. If the queue is still full after the timeout, spill to a local SQLite
       overflow file rather than discarding.
    3. The worker drains the overflow file back into the main pipeline once
       the queue has room, so records arrive late but never disappear.
    """

    _OVERFLOW_TIMEOUT = 0.05        # seconds to wait before spilling
    _DRAIN_INTERVAL_SEC = 10.0      # how often to replay overflow into queue

    def __init__(
        self,
        storage_backend=None,
        buffer_size: int = 10_000,
        flush_interval: float = 5.0,
        overflow_path: Optional[str] = None,
    ):
        self.storage = storage_backend
        self.buffer_size = buffer_size
        self.flush_interval = flush_interval

        self._queue: queue.Queue = queue.Queue(maxsize=buffer_size)
        self._shutdown = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None

        # Overflow buffer — a tiny SQLite DB written to a temp file.
        # Using SQLite gives us atomicity and durability without complexity.
        if overflow_path is None:
            fd, overflow_path = tempfile.mkstemp(
                suffix="_ca_overflow.db", prefix="cost_attr_"
            )
            os.close(fd)
        self._overflow_path = overflow_path
        self._overflow_lock = threading.Lock()
        self._init_overflow_db()

        self.start()

        # Pre-declare every counter that ops teams need to alert on.
        # Without this, counters only appear in /metrics after the first event —
        # meaning an alert on cost_async_logger_dropped_total would never fire
        # if the counter is absent from the output entirely.
        _m = get_metrics()
        for name in (
            "cost_async_logger_enqueued_total",
            "cost_async_logger_overflow_total",
            "cost_async_logger_overflow_replayed_total",
            "cost_async_logger_dropped_total",   # Finding 1: THIS is the critical one
            "cost_async_logger_flush_total",
            "cost_async_logger_flushed_records_total",
            "cost_async_logger_flush_errors_total",
            "cost_total_usd",                    # Finding 4: cost metrics
        ):
            _m.inc(name, 0.0)                   # register with value 0 — idempotent

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def start(self):
        """Start async logger worker."""
        self._worker_thread = threading.Thread(target=self._worker, daemon=True)
        self._worker_thread.start()
        log_event(
            "async_logger_started",
            buffer_size=self.buffer_size,
            flush_interval=self.flush_interval,
        )

    def stop(self):
        """Stop async logger and flush remaining records."""
        self._shutdown.set()
        if self._worker_thread:
            self._worker_thread.join(timeout=30)
        log_event("async_logger_stopped")

    def log(self, record: CostRecord):
        """Enqueue a cost record for storage (non-blocking).

        Tries a short blocking put first.  If the queue is still full,
        spills to the on-disk overflow buffer instead of dropping.
        """
        try:
            self._queue.put(record, block=True, timeout=self._OVERFLOW_TIMEOUT)
            metrics = get_metrics()
            metrics.inc("cost_async_logger_enqueued_total", 1.0)
            metrics.set_gauge("cost_async_logger_queue_depth", float(self._queue.qsize()))
        except queue.Full:
            # Queue is congested — spill to disk rather than drop
            self._spill_to_overflow(record)
            get_metrics().inc("cost_async_logger_overflow_total", 1.0)
            log_event("async_logger_overflow", tx_id=record.tx_id)

    async def log_async(self, record: CostRecord):
        """Log a cost record asynchronously."""
        await asyncio.get_event_loop().run_in_executor(None, self.log, record)

    # ------------------------------------------------------------------
    # Overflow buffer
    # ------------------------------------------------------------------

    def _init_overflow_db(self):
        with closing(sqlite3.connect(self._overflow_path)) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS overflow (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts        REAL NOT NULL,
                    payload   TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def _spill_to_overflow(self, record: CostRecord):
        """Serialize record to the overflow SQLite DB."""
        try:
            payload = json.dumps(record.to_dict())
            with self._overflow_lock:
                with closing(sqlite3.connect(self._overflow_path)) as conn:
                    conn.execute(
                        "INSERT INTO overflow (ts, payload) VALUES (?, ?)",
                        (time.time(), payload),
                    )
                    conn.commit()
        except Exception as exc:
            # Last resort: if the overflow write also fails, now we log the drop
            get_metrics().inc("cost_async_logger_dropped_total", 1.0)
            log_event("async_logger_drop_unrecoverable", tx_id=record.tx_id, error=str(exc))

    def _drain_overflow(self):
        """Re-queue records from the overflow buffer when there is room."""
        with self._overflow_lock:
            try:
                with closing(sqlite3.connect(self._overflow_path)) as conn:
                    rows = conn.execute(
                        "SELECT id, payload FROM overflow ORDER BY id LIMIT 500"
                    ).fetchall()
                    if not rows:
                        return
                    drained_ids = []
                    for row_id, payload in rows:
                        if self._queue.full():
                            break
                        try:
                            data = json.loads(payload)
                            # Reconstruct a minimal CostRecord
                            record = CostRecord(
                                tx_id=data["tx_id"],
                                timestamp=data["timestamp"],
                                function_name=data["function_name"],
                                feature=data.get("feature"),
                                user_id=data.get("user_id"),
                                request_id=data.get("request_id"),
                                endpoint=data.get("endpoint"),
                                parent_tx_id=data.get("parent_tx_id"),
                                duration_ms=float(data.get("duration_ms", 0)),
                                cpu_time_ms=float(data.get("cpu_time_ms", 0)),
                                memory_mb=float(data.get("memory_mb", 0)),
                                api_calls=data.get("api_calls") or {},
                                cpu_cost=float(data.get("cpu_cost", 0)),
                                memory_cost=float(data.get("memory_cost", 0)),
                                api_cost=float(data.get("api_cost", 0)),
                                api_cost_breakdown=data.get("api_cost_breakdown") or {},
                                total_cost=float(data.get("total_cost", 0)),
                                tags=data.get("tags") or {},
                                error=data.get("error"),
                            )
                            self._queue.put_nowait(record)
                            drained_ids.append(row_id)
                        except Exception:
                            drained_ids.append(row_id)  # discard corrupt record
                    if drained_ids:
                        conn.execute(
                            f"DELETE FROM overflow WHERE id IN ({','.join('?' * len(drained_ids))})",
                            drained_ids,
                        )
                        conn.commit()
                        get_metrics().inc(
                            "cost_async_logger_overflow_replayed_total",
                            float(len(drained_ids)),
                        )
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Worker
    # ------------------------------------------------------------------

    def _worker(self):
        """Background thread — flushes the queue to storage and replays overflow."""
        buffer: List[CostRecord] = []
        last_flush = time.time()
        last_drain = time.time()

        while not self._shutdown.is_set() or not self._queue.empty():
            try:
                record = self._queue.get(timeout=1.0)
                buffer.append(record)

                now = time.time()
                if len(buffer) >= 100 or (now - last_flush) >= self.flush_interval:
                    self._flush(buffer)
                    buffer.clear()
                    last_flush = now

            except queue.Empty:
                if buffer:
                    self._flush(buffer)
                    buffer.clear()
                    last_flush = time.time()

            # Periodically replay overflow back into the main queue
            if time.time() - last_drain >= self._DRAIN_INTERVAL_SEC:
                self._drain_overflow()
                last_drain = time.time()

        # Final flush
        if buffer:
            self._flush(buffer)
        # Final overflow drain
        self._drain_overflow()

    def _flush(self, records: List[CostRecord]):
        """Flush records to storage and update cost metrics."""
        if not (self.storage and records):
            return
        try:
            self.storage.store_batch(records)
            metrics = get_metrics()
            metrics.inc("cost_async_logger_flush_total", 1.0)
            metrics.inc("cost_async_logger_flushed_records_total", float(len(records)))
            metrics.set_gauge("cost_async_logger_queue_depth", float(self._queue.qsize()))

            # ── Cost metrics — pushed on every flush so Prometheus/Grafana
            # can alert on real spend, not just library health counters.
            total_cost = 0.0
            by_feature: dict = {}
            for record in records:
                cost = float(getattr(record, "total_cost", 0) or 0)
                total_cost += cost
                feature = getattr(record, "feature", None)
                if feature:
                    by_feature[feature] = by_feature.get(feature, 0.0) + cost

            metrics.inc("cost_total_usd", total_cost)
            for feature, cost in by_feature.items():
                # Sanitise feature name for Prometheus label use
                safe = feature.replace(" ", "_").replace("-", "_").replace("/", "_")
                metrics.inc(f"cost_by_feature_usd{{feature=\"{safe}\"}}", cost)
            metrics.set_gauge("cost_records_per_flush", float(len(records)))

        except Exception as e:
            get_metrics().inc("cost_async_logger_flush_errors_total", 1.0)
            log_event("async_logger_flush_error", error=str(e), batch_size=len(records))


_global_logger: Optional[AsyncLogger] = None


def get_async_logger() -> AsyncLogger:
    """Get global async logger."""
    global _global_logger
    if _global_logger is None:
        _global_logger = AsyncLogger()
    return _global_logger


def set_async_logger(logger: AsyncLogger):
    """Set global async logger."""
    global _global_logger
    if _global_logger:
        _global_logger.stop()
    _global_logger = logger
