"""Lightweight in-process metrics with Prometheus text exposition."""

import threading
from typing import Dict


class MetricsRegistry:
    """Thread-safe counter and gauge registry."""

    def __init__(self):
        self._lock = threading.Lock()
        self._counters: Dict[str, float] = {}
        self._gauges: Dict[str, float] = {}

    def inc(self, name: str, value: float = 1.0):
        with self._lock:
            self._counters[name] = self._counters.get(name, 0.0) + value

    def set_gauge(self, name: str, value: float):
        with self._lock:
            self._gauges[name] = value

    def snapshot(self) -> Dict[str, Dict[str, float]]:
        with self._lock:
            return {
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
            }

    def to_prometheus_text(self) -> str:
        snap = self.snapshot()
        lines = []
        for name, value in sorted(snap["counters"].items()):
            lines.append(f"# TYPE {name} counter")
            lines.append(f"{name} {value}")
        for name, value in sorted(snap["gauges"].items()):
            lines.append(f"# TYPE {name} gauge")
            lines.append(f"{name} {value}")
        return "\n".join(lines) + ("\n" if lines else "")


_metrics = MetricsRegistry()


def get_metrics() -> MetricsRegistry:
    return _metrics
