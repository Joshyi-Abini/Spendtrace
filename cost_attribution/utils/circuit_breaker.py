"""Simple circuit breaker to protect tracking path from repeated failures."""

import threading
import time
from typing import List


class CircuitBreaker:
    """Thread-safe circuit breaker with CLOSED, OPEN, and HALF_OPEN states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __init__(
        self,
        error_threshold: int = 10,
        error_window_sec: int = 60,
        recovery_timeout_sec: int = 300,
    ):
        self.error_threshold = max(1, int(error_threshold))
        self.error_window_sec = max(1, int(error_window_sec))
        self.recovery_timeout_sec = max(1, int(recovery_timeout_sec))

        self._lock = threading.Lock()
        self._state = self.CLOSED
        self._errors: List[float] = []
        self._opened_at = 0.0

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    def allow_request(self) -> bool:
        """Return True when instrumentation path is allowed."""
        with self._lock:
            now = time.time()
            if self._state == self.OPEN:
                if (now - self._opened_at) >= self.recovery_timeout_sec:
                    self._state = self.HALF_OPEN
                    return True
                return False
            return True

    def record_success(self):
        with self._lock:
            self._errors.clear()
            self._state = self.CLOSED
            self._opened_at = 0.0

    def record_failure(self):
        with self._lock:
            now = time.time()
            cutoff = now - self.error_window_sec
            self._errors = [ts for ts in self._errors if ts >= cutoff]
            self._errors.append(now)

            if self._state == self.HALF_OPEN:
                self._trip(now)
                return

            if len(self._errors) >= self.error_threshold:
                self._trip(now)

    def _trip(self, now: float):
        self._state = self.OPEN
        self._opened_at = now
