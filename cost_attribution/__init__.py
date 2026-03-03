"""
Cost Attribution

The layer between your Python app and your AWS bill — feature-level cost
breakdown, verified against what AWS actually charged.

Quick start::

    import cost_attribution

    # 1. Auto-instrument all boto3 / LLM calls (once at startup)
    cost_attribution.auto_instrument()

    # 2. Decorate your features
    from cost_attribution import cost_track

    @cost_track(feature="ai_recommendations")
    def recommend(user_id):
        ...

    # 3. Reconcile against your AWS bill
    from cost_attribution import reconcile
    report = reconcile(db_path="cost_data.db", start="2026-02-01", end="2026-03-01")
    print(report.summary())
"""

__version__ = "1.1.0"

from .core.instrumentation import (
    cost_track,
    track_request,
    track,
    set_circuit_breaker,
    set_global_sample_rate,
    get_global_sample_rate,
    set_sampling_policy,
    clear_sampling_policy,
    reload_sampling_from_env,
)
from .core.context import (
    start_request,
    end_request,
    add_api_call,
    add_tag,
    copy_current_context,
    create_task_with_context,
)
from .core.tracker import get_tracker, CostTracker
from .core.models import (
    get_cost_model,
    get_pricing_provider,
    AWSCostModel,
    PricingProvider,
    StaticPricingProvider,
    AWSDynamicPricingProvider,
)
from .storage.sqlite import SQLiteStorage
from .utils.async_logger import get_async_logger, AsyncLogger
from .utils.circuit_breaker import CircuitBreaker
from .utils.metrics import get_metrics
from .utils.logging import configure_structured_logging
from .reconciliation.aws import AWSBillingReconciler, ReconciliationReport
from .reconciliation.api import reconcile
from .auto_instrument import auto_instrument, is_instrumented
from .alerts import set_alert, clear_alerts, get_cost_trend
from .graph import (
    get_feature_cost_breakdown,
    get_request_cost,
    get_request_subtree,
    get_transaction_subtree,
)

__all__ = [
    "cost_track",
    "track_request",
    "track",
    "set_circuit_breaker",
    "set_global_sample_rate",
    "get_global_sample_rate",
    "set_sampling_policy",
    "clear_sampling_policy",
    "reload_sampling_from_env",
    "start_request",
    "end_request",
    "add_api_call",
    "add_tag",
    "copy_current_context",
    "create_task_with_context",
    "get_tracker",
    "CostTracker",
    "get_cost_model",
    "get_pricing_provider",
    "AWSCostModel",
    "PricingProvider",
    "StaticPricingProvider",
    "AWSDynamicPricingProvider",
    "SQLiteStorage",
    "get_async_logger",
    "AsyncLogger",
    "CircuitBreaker",
    "get_metrics",
    "configure_structured_logging",
    "AWSBillingReconciler",
    "ReconciliationReport",
    "reconcile",
    "auto_instrument",
    "is_instrumented",
    "set_alert",
    "clear_alerts",
    "get_cost_trend",
    # Graph-aware queries
    "get_feature_cost_breakdown",
    "get_request_cost",
    "get_request_subtree",
    "get_transaction_subtree",
]
