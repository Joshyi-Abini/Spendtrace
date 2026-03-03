"""
Cost Attribution — Graph-aware cost queries

The ``parent_tx_id`` + ``request_id`` columns on every transaction form a full
call graph.  This module exposes the three queries that graph enables:

1. **Fully-loaded vs direct cost per feature** (``get_feature_cost_breakdown``)
   Answers: "what does our *search* feature *really* cost, including all the
   DynamoDB reads and Bedrock calls it triggers downstream?"

2. **Cost per HTTP request** (``get_request_cost``, ``aggregate_by_request``)
   Answers: "our ``/api/search`` endpoint costs $0.0000181 per call on average."

3. **Request subtree drill-down** (``get_request_subtree``, ``get_transaction_subtree``)
   Answers: "show me the full call tree for request abc-123 with costs at
   each node" — the starting point for any cost debugging session.

All functions accept an optional ``db_path`` or ``storage`` argument.
When omitted they fall back to the global tracker's storage.

Usage::

    from cost_attribution import (
        get_feature_cost_breakdown,
        get_request_cost,
        get_request_subtree,
        get_transaction_subtree,
    )

    # 1 — fully-loaded vs direct per feature
    for row in get_feature_cost_breakdown():
        print(
            f"{row['feature']:<25}"
            f"  direct=${row['direct_cost']:.6f}"
            f"  fully_loaded=${row['fully_loaded_cost']:.6f}"
            f"  children=${row['children_cost']:.6f}"
        )

    # 2 — per-request cost
    for req in get_request_cost(limit=20):
        print(f"{req['endpoint']:<30}  ${req['total_cost']:.6f}  ({req['tx_count']} spans)")

    # 3 — drill into a single request
    tree = get_request_subtree("req-abc-123")
    for node in tree:
        indent = "  " * node["depth"]
        print(f"{indent}{node['feature']:<20}  subtree=${node['subtree_cost']:.6f}")
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------

def _storage(db_path: Optional[str] = None, storage=None):
    """Resolve a storage backend from args or fall back to the global tracker."""
    if storage is not None:
        return storage
    if db_path is not None:
        from .storage.sqlite import SQLiteStorage
        return SQLiteStorage(db_path=db_path)
    # Fall back to whatever the global tracker is using
    try:
        from .core.tracker import get_tracker
        tracker = get_tracker()
        backend = (
            getattr(tracker, "_storage", None)
            or getattr(tracker, "storage", None)
        )
        if backend is not None:
            return backend
    except Exception:
        pass
    raise RuntimeError(
        "No storage backend available. Pass db_path= or storage= to the call."
    )


# ---------------------------------------------------------------------------
# 1. Fully-loaded vs direct cost per feature
# ---------------------------------------------------------------------------

def get_feature_cost_breakdown(
    start_time: Optional[float] = None,
    end_time: Optional[float] = None,
    db_path: Optional[str] = None,
    storage=None,
) -> List[Dict[str, Any]]:
    """Return direct *and* fully-loaded cost for every feature.

    **Why the split matters:** ``aggregate_by_feature()`` only counts what
    each function recorded via ``add_api_call()`` in its own scope.  That's
    correct for debugging individual functions.  But a product manager asking
    "what does *search* cost?" wants the whole picture — the DynamoDB reads
    inside ``search`` *plus* all the Bedrock calls inside ``product_details``
    that ``search`` triggered.  This function provides both numbers.

    Args:
        start_time: Unix timestamp lower bound (inclusive).
        end_time:   Unix timestamp upper bound (inclusive).
        db_path:    Path to SQLite DB (uses global tracker default if omitted).
        storage:    Pre-built ``SQLiteStorage`` instance.

    Returns:
        List of dicts sorted by ``fully_loaded_cost`` descending, each with:

        - ``feature``           — feature name
        - ``direct_cost``       — USD spent by this feature's own transactions
        - ``fully_loaded_cost`` — direct + all descendants triggered
        - ``children_cost``     — ``fully_loaded - direct`` (the delegation cost)
        - ``direct_tx_count``   — number of transactions with this feature tag

    Example::

        feature          direct      fully_loaded   children
        api              $0.000001   $0.0181         $0.0181
        search           $0.005      $0.014          $0.009
        product_details  $0.009      $0.009          $0.000
        cache            $0.0001     $0.0001         $0.000
    """
    return _storage(db_path, storage).get_feature_cost_breakdown(
        start_time=start_time,
        end_time=end_time,
    )


# ---------------------------------------------------------------------------
# 2. Per-request cost
# ---------------------------------------------------------------------------

def get_request_cost(
    start_time: Optional[float] = None,
    end_time: Optional[float] = None,
    limit: int = 1000,
    db_path: Optional[str] = None,
    storage=None,
) -> List[Dict[str, Any]]:
    """Return the total cost of each HTTP request, grouped by ``request_id``.

    Every transaction spawned during a single HTTP request shares the same
    ``request_id`` (set by ``@track_request`` or ``start_request()``).
    Summing those transactions gives the end-to-end cost of one request —
    the number your infrastructure team needs for capacity planning.

    Typical use::

        for req in get_request_cost():
            print(f"{req['endpoint']}  ${req['total_cost']:.6f}/call")

    Args:
        start_time: Unix timestamp lower bound.
        end_time:   Unix timestamp upper bound.
        limit:      Maximum number of requests to return (default 1 000).
        db_path:    Path to SQLite DB.
        storage:    Pre-built ``SQLiteStorage`` instance.

    Returns:
        List of dicts sorted by ``total_cost`` descending, each with:

        - ``request_id``  — the shared request identifier
        - ``endpoint``    — HTTP endpoint (from the root transaction)
        - ``total_cost``  — USD sum of all transactions in the request
        - ``api_cost``    — AWS API cost portion of the total
        - ``tx_count``    — number of spans (transactions) in the request
        - ``error_count`` — number of failed spans
        - ``started_at``  — Unix timestamp of the earliest span
    """
    return _storage(db_path, storage).aggregate_by_request(
        start_time=start_time,
        end_time=end_time,
        limit=limit,
    )


# Alias kept for discoverability
aggregate_by_request = get_request_cost


# ---------------------------------------------------------------------------
# 3. Subtree drill-downs
# ---------------------------------------------------------------------------

def get_request_subtree(
    request_id: str,
    db_path: Optional[str] = None,
    storage=None,
) -> List[Dict[str, Any]]:
    """Return the full annotated call tree for a single HTTP request.

    This is the primary debugging entry point.  Given a ``request_id`` that
    appeared in your logs or a ``get_request_cost()`` result, you get back
    every transaction in that request as a depth-first pre-order list, with
    ``subtree_cost`` already computed so you can see at a glance which branch
    is expensive.

    Args:
        request_id: The shared request identifier (set by ``@track_request``).
        db_path:    Path to SQLite DB.
        storage:    Pre-built ``SQLiteStorage`` instance.

    Returns:
        List of dicts in depth-first pre-order (parent always before its
        children), each with:

        - ``tx_id``         — transaction ID
        - ``parent_tx_id``  — parent transaction ID (None for root)
        - ``depth``         — tree depth (0 = root)
        - ``feature``       — feature label
        - ``function_name`` — Python function name
        - ``total_cost``    — this transaction's own direct cost
        - ``subtree_cost``  — this transaction's cost + all descendants
        - ``api_cost``      — API cost portion
        - ``duration_ms``   — wall-clock time (if tracked)
        - ``error``         — error message if the transaction failed

    Example rendering::

        for node in get_request_subtree("req-abc"):
            indent = "  " * node["depth"]
            print(
                f"{indent}{node['feature']:<20}"
                f"  direct=${node['total_cost']:.6f}"
                f"  subtree=${node['subtree_cost']:.6f}"
            )
        # api                   direct=$0.000001  subtree=$0.018101
        #   search              direct=$0.005000  subtree=$0.014000
        #     product_details   direct=$0.003000  subtree=$0.003000
        #     product_details   direct=$0.003000  subtree=$0.003000
        #     product_details   direct=$0.003000  subtree=$0.003000
        #   cache               direct=$0.000100  subtree=$0.000100
    """
    return _storage(db_path, storage).get_request_subtree(request_id)


def get_transaction_subtree(
    tx_id: str,
    db_path: Optional[str] = None,
    storage=None,
) -> List[Dict[str, Any]]:
    """Return the subtree rooted at a specific transaction.

    Use this when you want to drill into one feature call — e.g., the single
    most expensive ``search`` transaction — to see exactly what it triggered
    and how much each step cost.  Depth is re-numbered from 0 at the
    requested root.

    Args:
        tx_id:    The transaction ID to use as the subtree root.
        db_path:  Path to SQLite DB.
        storage:  Pre-built ``SQLiteStorage`` instance.

    Returns:
        Same shape as :func:`get_request_subtree` but depth starts at 0
        for the requested transaction.
    """
    return _storage(db_path, storage).get_transaction_subtree(tx_id)
