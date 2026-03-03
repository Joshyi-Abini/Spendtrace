"""FastAPI application exposing cost attribution query endpoints."""

import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import PlainTextResponse

from ..storage.sqlite import SQLiteStorage
from ..utils.logging import log_event, maybe_configure_from_env
from ..utils.metrics import get_metrics

app = FastAPI(title="Cost Attribution API", version="1.1.0")
maybe_configure_from_env()

_storage_cache: dict[str, SQLiteStorage] = {}
_DB_ROOT_RAW = os.getenv("COST_ATTRIBUTION_DB_ROOT", "").strip()
_DB_ROOT = Path(_DB_ROOT_RAW).resolve() if _DB_ROOT_RAW else None
_ALLOWED_DB_SUFFIXES = {".db", ".sqlite", ".sqlite3"}


def _storage(db_path: str) -> SQLiteStorage:
    """Return a cached SQLiteStorage for *db_path*, creating it on first call."""
    if db_path not in _storage_cache:
        _storage_cache[db_path] = SQLiteStorage(db_path=db_path)
    return _storage_cache[db_path]


def _v(value: Any):
    """Unwrap FastAPI Query default sentinels to plain values."""
    return None if hasattr(value, "default") else value


def _safe_db_path(db_path_value: Any) -> str:
    """Resolve db_path under an optional configured root."""
    raw = str(_v(db_path_value) or "cost_data.db").strip()
    candidate = Path(raw)

    if _DB_ROOT is not None and not candidate.is_absolute():
        candidate = (_DB_ROOT / candidate).resolve()
    else:
        candidate = candidate.resolve()

    if _DB_ROOT is not None:
        try:
            candidate.relative_to(_DB_ROOT)
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail=f"db_path must stay under {_DB_ROOT}",
            ) from exc

    if candidate.suffix.lower() not in _ALLOWED_DB_SUFFIXES:
        raise HTTPException(
            status_code=400,
            detail="db_path must use .db, .sqlite, or .sqlite3",
        )

    return str(candidate)


@app.get("/health")
def health():
    get_metrics().inc("cost_api_requests_total", 1.0)
    return {"status": "ok"}


@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    return get_metrics().to_prometheus_text()


@app.get("/transactions")
def transactions(
    db_path: str = Query("cost_data.db"),
    feature: str | None = Query(None),
    user_id: str | None = Query(None),
    start_time: float | None = Query(None),
    end_time: float | None = Query(None),
    limit: int = Query(100, ge=1, le=5000),
):
    db_path = _safe_db_path(db_path)
    feature = _v(feature)
    user_id = _v(user_id)
    start_time = _v(start_time)
    end_time = _v(end_time)
    limit = int(_v(limit) or 100)

    get_metrics().inc("cost_api_requests_total", 1.0)
    log_event("api_transactions_query", feature=feature, user_id=user_id, limit=limit)
    return _storage(db_path).query(
        feature=feature,
        user_id=user_id,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
    )


@app.get("/aggregate/feature")
def aggregate_feature(
    db_path: str = Query("cost_data.db"),
    start_time: float | None = Query(None),
    end_time: float | None = Query(None),
):
    db_path = _safe_db_path(db_path)
    get_metrics().inc("cost_api_requests_total", 1.0)
    return _storage(db_path).aggregate_by_feature(
        start_time=_v(start_time),
        end_time=_v(end_time),
    )


@app.get("/aggregate/feature/loaded")
def aggregate_feature_loaded(
    db_path: str = Query("cost_data.db"),
    start_time: float | None = Query(None),
    end_time: float | None = Query(None),
):
    """Fully-loaded vs direct cost per feature (call-graph aware)."""
    db_path = _safe_db_path(db_path)
    get_metrics().inc("cost_api_requests_total", 1.0)
    return _storage(db_path).get_feature_cost_breakdown(
        start_time=_v(start_time),
        end_time=_v(end_time),
    )


@app.get("/v2/feature-breakdown")
def v2_feature_breakdown(
    db_path: str = Query("cost_data.db"),
    start_time: float | None = Query(None),
    end_time: float | None = Query(None),
):
    """Backward-compatible alias for fully loaded feature breakdown."""
    return aggregate_feature_loaded(db_path=db_path, start_time=start_time, end_time=end_time)


@app.get("/aggregate/user")
def aggregate_user(
    db_path: str = Query("cost_data.db"),
    start_time: float | None = Query(None),
    end_time: float | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
):
    db_path = _safe_db_path(db_path)
    get_metrics().inc("cost_api_requests_total", 1.0)
    return _storage(db_path).aggregate_by_user(
        start_time=_v(start_time),
        end_time=_v(end_time),
        limit=int(_v(limit) or 100),
    )


@app.get("/aggregate/endpoint")
def aggregate_endpoint(
    db_path: str = Query("cost_data.db"),
    start_time: float | None = Query(None),
    end_time: float | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
):
    """Cost per HTTP endpoint for capacity planning."""
    db_path = _safe_db_path(db_path)
    get_metrics().inc("cost_api_requests_total", 1.0)
    return _storage(db_path).aggregate_by_endpoint(
        start_time=_v(start_time),
        end_time=_v(end_time),
        limit=int(_v(limit) or 100),
    )


@app.get("/v2/endpoint")
def v2_endpoint(
    db_path: str = Query("cost_data.db"),
    start_time: float | None = Query(None),
    end_time: float | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
):
    """Backward-compatible alias for endpoint aggregation."""
    return aggregate_endpoint(
        db_path=db_path,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
    )


@app.get("/aggregate/request")
def aggregate_request(
    db_path: str = Query("cost_data.db"),
    start_time: float | None = Query(None),
    end_time: float | None = Query(None),
    limit: int = Query(1000, ge=1, le=10000),
):
    """Cost per individual HTTP request (request_id-level)."""
    db_path = _safe_db_path(db_path)
    get_metrics().inc("cost_api_requests_total", 1.0)
    return _storage(db_path).aggregate_by_request(
        start_time=_v(start_time),
        end_time=_v(end_time),
        limit=int(_v(limit) or 1000),
    )


@app.get("/v2/request")
def v2_request(
    db_path: str = Query("cost_data.db"),
    start_time: float | None = Query(None),
    end_time: float | None = Query(None),
    limit: int = Query(1000, ge=1, le=10000),
):
    """Backward-compatible alias for request-level aggregation."""
    return aggregate_request(
        db_path=db_path,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
    )


@app.get("/request/{request_id}/subtree")
def request_subtree(request_id: str, db_path: str = Query("cost_data.db")):
    """Full annotated call tree for one request_id."""
    db_path = _safe_db_path(db_path)
    get_metrics().inc("cost_api_requests_total", 1.0)
    return _storage(db_path).get_request_subtree(request_id)


@app.get("/transaction/{tx_id}/subtree")
def transaction_subtree(tx_id: str, db_path: str = Query("cost_data.db")):
    """Subtree rooted at a specific transaction."""
    db_path = _safe_db_path(db_path)
    get_metrics().inc("cost_api_requests_total", 1.0)
    return _storage(db_path).get_transaction_subtree(tx_id)


@app.get("/aggregate/error")
def aggregate_error(
    db_path: str = Query("cost_data.db"),
    start_time: float | None = Query(None),
    end_time: float | None = Query(None),
):
    """Success vs error cost breakdown."""
    db_path = _safe_db_path(db_path)
    get_metrics().inc("cost_api_requests_total", 1.0)
    return _storage(db_path).get_error_cost(
        start_time=_v(start_time),
        end_time=_v(end_time),
    )


@app.get("/total")
def total(
    db_path: str = Query("cost_data.db"),
    feature: str | None = Query(None),
    user_id: str | None = Query(None),
    start_time: float | None = Query(None),
    end_time: float | None = Query(None),
):
    db_path = _safe_db_path(db_path)
    get_metrics().inc("cost_api_requests_total", 1.0)
    return {
        "total_cost": _storage(db_path).get_total_cost(
            feature=_v(feature),
            user_id=_v(user_id),
            start_time=_v(start_time),
            end_time=_v(end_time),
        )
    }


@app.get("/api/services")
def api_services(
    db_path: str = Query("cost_data.db"),
    start_time: float | None = Query(None),
    end_time: float | None = Query(None),
    tx_limit: int = Query(1000, ge=1, le=200000),
    service_limit: int = Query(100, ge=1, le=1000),
):
    db_path = _safe_db_path(db_path)
    get_metrics().inc("cost_api_requests_total", 1.0)
    return _storage(db_path).aggregate_api_services(
        start_time=_v(start_time),
        end_time=_v(end_time),
        tx_limit=int(_v(tx_limit) or 1000),
        service_limit=int(_v(service_limit) or 100),
    )
