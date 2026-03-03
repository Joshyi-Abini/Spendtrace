"""
Cost Attribution - Instrumentation

Decorators and utilities for automatic cost tracking.
"""

import functools
import inspect
import os
import random
from typing import Callable, Optional, Any, Dict
from .context import get_context_manager, start_request, end_request
from .tracker import get_tracker, CostRecord
from ..utils.metrics import get_metrics
from ..utils.logging import log_event
from ..utils.async_logger import get_async_logger

_circuit_breaker = None
_global_sample_rate: Optional[float] = None
_sampling_policy: Optional[Callable[[str, Optional[str], Dict[str, Any]], Optional[float]]] = None


def cost_track(
    feature: Optional[str] = None,
    track_memory: bool = True,
    track_cpu: bool = True,
    redact_args: bool = False,
    sample_rate: float = 1.0,
    circuit_breaker: Optional[bool] = None,
    **tags
):
    """
    Decorator to track cost of a function.
    
    Usage:
        @cost_track(feature="search")
        def search_products(query):
            return results
    
    Args:
        feature: Feature name for grouping
        track_memory: Enable memory tracking
        track_cpu: Enable CPU tracking
        redact_args: If True, avoid recording argument values in tags
        sample_rate: Fraction in [0,1] of calls to sample
        circuit_breaker: Per-function override for global circuit breaker usage
        **tags: Additional tags
    """
    bounded_sample_rate = _bounded_rate(sample_rate)

    def decorator(func: Callable) -> Callable:
        
        # Handle async functions
        if inspect.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                effective_sample_rate = _resolve_sample_rate(
                    local_rate=bounded_sample_rate,
                    function_name=func.__name__,
                    feature=feature,
                    tags=tags,
                )
                if effective_sample_rate < 1.0 and random.random() > effective_sample_rate:
                    get_metrics().inc("cost_instrumentation_sampled_out_total", 1.0)
                    return await func(*args, **kwargs)

                if _should_use_breaker(circuit_breaker) and _circuit_breaker and not _circuit_breaker.allow_request():
                    get_metrics().inc("cost_instrumentation_skipped_total", 1.0)
                    return await func(*args, **kwargs)

                tracker = get_tracker()
                ctx_mgr = get_context_manager()
                tx_tags = _build_transaction_tags(func, redact_args, tags, args, kwargs)
                
                # Start transaction
                context = ctx_mgr.start_transaction(
                    function_name=func.__name__,
                    feature=feature,
                    **tx_tags
                )
                
                tracker.start_tracking(context)
                
                error = None
                result = None
                try:
                    result = await func(*args, **kwargs)
                    return result
                except Exception as e:
                    error = e
                    raise
                finally:
                    # Stop tracking
                    record = tracker.stop_tracking(context, error)
                    ctx_mgr.end_transaction()
                    
                    # Store record (async)
                    try:
                        await _store_record_async(record)
                        get_metrics().inc("cost_instrumentation_records_total", 1.0)
                        if _should_use_breaker(circuit_breaker) and _circuit_breaker:
                            _circuit_breaker.record_success()
                    except Exception:
                        get_metrics().inc("cost_instrumentation_store_errors_total", 1.0)
                        if _should_use_breaker(circuit_breaker) and _circuit_breaker:
                            _circuit_breaker.record_failure()
                        log_event("instrumentation_store_error", function=func.__name__)
            
            return async_wrapper
        
        # Sync function
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                effective_sample_rate = _resolve_sample_rate(
                    local_rate=bounded_sample_rate,
                    function_name=func.__name__,
                    feature=feature,
                    tags=tags,
                )
                if effective_sample_rate < 1.0 and random.random() > effective_sample_rate:
                    get_metrics().inc("cost_instrumentation_sampled_out_total", 1.0)
                    return func(*args, **kwargs)

                if _should_use_breaker(circuit_breaker) and _circuit_breaker and not _circuit_breaker.allow_request():
                    get_metrics().inc("cost_instrumentation_skipped_total", 1.0)
                    return func(*args, **kwargs)

                tracker = get_tracker()
                ctx_mgr = get_context_manager()
                tx_tags = _build_transaction_tags(func, redact_args, tags, args, kwargs)
                
                # Start transaction
                context = ctx_mgr.start_transaction(
                    function_name=func.__name__,
                    feature=feature,
                    **tx_tags
                )
                
                tracker.start_tracking(context)
                
                error = None
                result = None
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    error = e
                    raise
                finally:
                    # Stop tracking
                    record = tracker.stop_tracking(context, error)
                    ctx_mgr.end_transaction()
                    
                    # Store record (sync but non-blocking via async logger)
                    try:
                        _store_record_sync(record)
                        get_metrics().inc("cost_instrumentation_records_total", 1.0)
                        if _should_use_breaker(circuit_breaker) and _circuit_breaker:
                            _circuit_breaker.record_success()
                    except Exception:
                        get_metrics().inc("cost_instrumentation_store_errors_total", 1.0)
                        if _should_use_breaker(circuit_breaker) and _circuit_breaker:
                            _circuit_breaker.record_failure()
                        log_event("instrumentation_store_error", function=func.__name__)
            
            return sync_wrapper
    
    return decorator


def track_request(
    user_id: Optional[str] = None,
    feature: Optional[str] = None,
    endpoint: Optional[str] = None,
    **tags
):
    """
    Decorator to track an entire request/operation.
    
    Sets up request context that nested function calls inherit.
    
    Usage:
        @track_request(feature="api")
        def handle_request(request):
            # All nested @cost_track functions inherit this context
            pass
    """
    def decorator(func: Callable) -> Callable:
        
        if inspect.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                # Start request context
                start_request(
                    user_id=user_id or kwargs.get('user_id'),
                    feature=feature,
                    endpoint=endpoint or func.__name__,
                    **tags
                )
                
                try:
                    return await func(*args, **kwargs)
                finally:
                    end_request()
            
            return async_wrapper
        
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                # Start request context
                start_request(
                    user_id=user_id or kwargs.get('user_id'),
                    feature=feature,
                    endpoint=endpoint or func.__name__,
                    **tags
                )
                
                try:
                    return func(*args, **kwargs)
                finally:
                    end_request()
            
            return sync_wrapper
    
    return decorator


def _build_transaction_tags(
    func: Callable,
    redact_args: bool,
    static_tags: Dict[str, Any],
    args: Any,
    kwargs: Dict[str, Any],
) -> Dict[str, Any]:
    tx_tags = dict(static_tags)
    tx_tags["args_redacted"] = redact_args
    if redact_args:
        tx_tags["arg_count"] = len(args)
        tx_tags["kwarg_keys"] = sorted(list(kwargs.keys()))
    else:
        tx_tags["arg_types"] = [type(arg).__name__ for arg in args]
        tx_tags["kwargs"] = {k: _safe_repr(v) for k, v in kwargs.items()}
    tx_tags["function"] = func.__name__
    return tx_tags


def _safe_repr(obj: Any) -> str:
    try:
        return repr(obj)[:256]
    except Exception:
        return "<unrepr>"


def _should_use_breaker(circuit_breaker_override: Optional[bool]) -> bool:
    if circuit_breaker_override is None:
        return bool(_circuit_breaker)
    return bool(circuit_breaker_override)


def _bounded_rate(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _resolve_sample_rate(
    local_rate: float,
    function_name: str,
    feature: Optional[str],
    tags: Dict[str, Any],
) -> float:
    rate = _bounded_rate(local_rate)
    if _global_sample_rate is not None:
        rate = min(rate, _bounded_rate(_global_sample_rate))
    if _sampling_policy:
        try:
            policy_rate = _sampling_policy(function_name, feature, dict(tags))
            if policy_rate is not None:
                rate = min(rate, _bounded_rate(policy_rate))
        except Exception:
            pass
    return rate


def _store_record_sync(record: CostRecord):
    """Store record synchronously (non-blocking)."""
    get_async_logger().log(record)


async def _store_record_async(record: CostRecord):
    """Store record asynchronously."""
    await get_async_logger().log_async(record)


def set_circuit_breaker(breaker):
    """Set a global circuit breaker for instrumentation."""
    global _circuit_breaker
    _circuit_breaker = breaker


def set_global_sample_rate(sample_rate: Optional[float]):
    """Set a fleet-wide sampling rate cap; None disables global cap."""
    global _global_sample_rate
    _global_sample_rate = None if sample_rate is None else _bounded_rate(sample_rate)


def get_global_sample_rate() -> Optional[float]:
    return _global_sample_rate


def set_sampling_policy(policy: Optional[Callable[[str, Optional[str], Dict[str, Any]], Optional[float]]]):
    """
    Set an optional sampling policy hook.

    Hook signature:
        (function_name, feature, tags) -> sample_rate | None
    """
    global _sampling_policy
    _sampling_policy = policy


def clear_sampling_policy():
    global _sampling_policy
    _sampling_policy = None


def reload_sampling_from_env():
    """Load global sample rate from COST_ATTRIBUTION_SAMPLE_RATE env var."""
    raw = os.getenv("COST_ATTRIBUTION_SAMPLE_RATE", "").strip()
    if not raw:
        set_global_sample_rate(None)
        return
    try:
        set_global_sample_rate(float(raw))
    except Exception:
        set_global_sample_rate(None)


reload_sampling_from_env()


# Context manager for explicit tracking

class track:
    """
    Context manager for explicit cost tracking.
    
    Usage:
        with track(feature="search", operation="query"):
            # Do expensive work
            pass
    """
    
    def __init__(self, feature: Optional[str] = None, operation: str = "anonymous", **tags):
        self.feature = feature
        self.operation = operation
        self.tags = tags
        self.context = None
        self.tracker = get_tracker()
    
    def __enter__(self):
        ctx_mgr = get_context_manager()
        self.context = ctx_mgr.start_transaction(
            function_name=self.operation,
            feature=self.feature,
            **self.tags
        )
        self.tracker.start_tracking(self.context)
        return self

    async def __aenter__(self):
        return self.__enter__()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.context:
            record = self.tracker.stop_tracking(self.context, exc_val)
            get_context_manager().end_transaction()
            _store_record_sync(record)
        return False

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.context:
            record = self.tracker.stop_tracking(self.context, exc_val)
            get_context_manager().end_transaction()
            await _store_record_async(record)
        return False
