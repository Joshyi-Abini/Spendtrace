"""Storage backends."""

from typing import TYPE_CHECKING

from .sqlite import SQLiteStorage as SQLiteStorage

if TYPE_CHECKING:
    from .timescaledb import TimescaleDBStorage as TimescaleDBStorage
else:
    try:
        from .timescaledb import TimescaleDBStorage as TimescaleDBStorage
    except Exception:  # Optional dependency
        TimescaleDBStorage = None  # type: ignore[assignment]

__all__ = ["SQLiteStorage", "TimescaleDBStorage"]
