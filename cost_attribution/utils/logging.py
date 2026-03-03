"""Structured logging helpers."""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any


def configure_structured_logging(level: str = "INFO"):
    """Configure root logger for JSON line output."""
    logger = logging.getLogger()
    logger.setLevel(level.upper())
    logger.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)


def log_event(event: str, **fields: Any):
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        **fields,
    }
    logging.getLogger(__name__).info(json.dumps(payload, default=str))


def maybe_configure_from_env():
    """Enable JSON logging when COST_ATTRIBUTION_JSON_LOGS is true."""
    raw = os.getenv("COST_ATTRIBUTION_JSON_LOGS", "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        configure_structured_logging(os.getenv("COST_ATTRIBUTION_LOG_LEVEL", "INFO"))
