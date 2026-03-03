"""
Cost Attribution — Spend Alerts & Cost Trends

Simple threshold-based alerting and daily cost trend queries.

Usage:
    from cost_attribution import set_alert, get_cost_trend

    set_alert(
        feature="ai_recommendations",
        threshold=10.00,
        window_hours=24,
        webhook="https://hooks.slack.com/...",
    )

    trend = get_cost_trend(feature="ai_recommendations", days=30)
    for day in trend:
        print(day["date"], day["total_cost"])
"""

from __future__ import annotations

import json
import threading
import time
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Spend alerts
# ---------------------------------------------------------------------------

@dataclass
class AlertRule:
    feature: str
    threshold: float          # USD
    window_hours: float
    webhook: Optional[str]
    last_triggered: float = field(default=0.0)
    cooldown_hours: float = field(default=1.0)  # prevent alert storms


_alert_rules: List[AlertRule] = []
_alert_lock = threading.Lock()
_alert_thread: Optional[threading.Thread] = None
_alert_stop = threading.Event()
_alert_storage = None  # injected on first use


def set_alert(
    feature: str,
    threshold: float,
    window_hours: float = 24,
    webhook: Optional[str] = None,
    cooldown_hours: float = 1.0,
    storage=None,
) -> None:
    """
    Register a spend alert for a feature.

    Args:
        feature:        Feature name to monitor (matches @cost_track feature=).
        threshold:      Alert when cumulative cost exceeds this USD amount.
        window_hours:   Look-back window in hours (default 24 h).
        webhook:        URL to POST an alert payload to (Slack, PagerDuty, HTTP).
                        If None the alert is only logged to stderr.
        cooldown_hours: Minimum hours between repeated alerts for the same rule.
        storage:        Optional SQLiteStorage instance.  Falls back to the
                        global tracker's storage when omitted.
    """
    global _alert_storage
    with _alert_lock:
        if storage is not None:
            _alert_storage = storage
        # Remove existing rule for the same feature
        _alert_rules[:] = [r for r in _alert_rules if r.feature != feature]
        _alert_rules.append(
            AlertRule(
                feature=feature,
                threshold=threshold,
                window_hours=window_hours,
                webhook=webhook,
                cooldown_hours=cooldown_hours,
            )
        )
        _ensure_alert_thread()


def clear_alerts() -> None:
    """Remove all registered alert rules."""
    with _alert_lock:
        _alert_rules.clear()


def _ensure_alert_thread() -> None:
    global _alert_thread
    if _alert_thread is not None and _alert_thread.is_alive():
        return
    _alert_stop.clear()
    _alert_thread = threading.Thread(target=_alert_loop, daemon=True, name="cost-alert")
    _alert_thread.start()


def _alert_loop() -> None:
    """Check alert rules every 60 seconds."""
    while not _alert_stop.wait(60):
        with _alert_lock:
            rules = list(_alert_rules)
            storage = _alert_storage

        if storage is None:
            storage = _get_default_storage()

        if storage is None:
            continue

        for rule in rules:
            _check_rule(rule, storage)


def _check_rule(rule: AlertRule, storage) -> None:
    now = time.time()
    window_start = now - rule.window_hours * 3600

    try:
        total = float(storage.get_total_cost(feature=rule.feature, start_time=window_start) or 0)
    except Exception:
        return

    if total < rule.threshold:
        return

    # Respect cooldown
    if now - rule.last_triggered < rule.cooldown_hours * 3600:
        return

    rule.last_triggered = now
    payload = {
        "alert": "cost_threshold_exceeded",
        "feature": rule.feature,
        "window_hours": rule.window_hours,
        "threshold_usd": rule.threshold,
        "actual_usd": round(total, 6),
        "timestamp": now,
    }
    _fire_alert(payload, rule.webhook)


def _fire_alert(payload: Dict[str, Any], webhook: Optional[str]) -> None:
    import sys
    msg = (
        f"[cost_attribution] ALERT: feature '{payload['feature']}' cost "
        f"${payload['actual_usd']:.4f} exceeded threshold "
        f"${payload['threshold_usd']:.4f} in last {payload['window_hours']}h"
    )
    print(msg, file=sys.stderr)

    if not webhook:
        return

    body = json.dumps({"text": msg, **payload}).encode()
    try:
        req = urllib.request.Request(
            webhook,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5):
            pass
    except Exception as exc:
        print(f"[cost_attribution] Alert webhook POST failed: {exc}", file=sys.stderr)


def _get_default_storage():
    try:
        from .core.tracker import get_tracker
        tracker = get_tracker()
        return getattr(tracker, "_storage", None) or getattr(tracker, "storage", None)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Cost trend
# ---------------------------------------------------------------------------

def get_cost_trend(
    feature: Optional[str] = None,
    days: int = 30,
    db_path: Optional[str] = None,
    storage=None,
) -> List[Dict[str, Any]]:
    """
    Return daily cost totals for the given feature over the last N days.

    Args:
        feature:  Feature name to filter on.  None returns all features.
        days:     Look-back window in days (default 30).
        db_path:  Path to SQLite database (uses global tracker default if omitted).
        storage:  Pre-built SQLiteStorage instance.

    Returns:
        List of dicts with keys:
          - ``date``        (str, YYYY-MM-DD)
          - ``total_cost``  (float, USD)
          - ``tx_count``    (int)
    """
    if storage is None and db_path:
        from .storage.sqlite import SQLiteStorage
        storage = SQLiteStorage(db_path=db_path)

    if storage is None:
        storage = _get_default_storage()

    if storage is None:
        raise RuntimeError(
            "No storage backend available.  Pass db_path= or storage= to get_cost_trend()."
        )

    now = time.time()
    window_start = now - days * 86400
    rows = storage.query(start_time=window_start, limit=100_000)

    # Bucket into calendar days (UTC)
    from datetime import datetime, timezone, timedelta

    daily: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        ts = row.get("timestamp", 0)
        if feature and row.get("feature") != feature:
            continue
        day = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        if day not in daily:
            daily[day] = {"date": day, "total_cost": 0.0, "tx_count": 0}
        daily[day]["total_cost"] += float(row.get("total_cost") or 0)
        daily[day]["tx_count"] += 1

    # Fill gaps and sort — include today, go back `days` calendar days
    result = []
    today = datetime.fromtimestamp(now, tz=timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    for i in range(days - 1, -1, -1):
        d = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        result.append(daily.get(d, {"date": d, "total_cost": 0.0, "tx_count": 0}))

    return result
