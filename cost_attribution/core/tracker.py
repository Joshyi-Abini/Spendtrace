"""
Cost Attribution - Cost Tracker

Core engine for tracking function execution costs.
"""

import time
import psutil  # type: ignore[import-untyped]
import tracemalloc
from typing import Dict, Any, Optional
from dataclasses import dataclass, field

from .context import TransactionContext, get_context_manager
from .models import CostModel, get_cost_model


@dataclass
class CostRecord:
    """Record of cost for a single transaction."""
    
    # Identity
    tx_id: str
    timestamp: float
    
    # Context
    function_name: str
    feature: Optional[str] = None
    user_id: Optional[str] = None
    request_id: Optional[str] = None
    endpoint: Optional[str] = None
    parent_tx_id: Optional[str] = None
    
    # Timing
    duration_ms: float = 0.0
    cpu_time_ms: float = 0.0
    
    # Resource usage
    memory_mb: float = 0.0
    allocated_memory_mb: Optional[float] = None
    network_bytes: int = 0
    
    # External calls
    api_calls: Dict[str, Any] = field(default_factory=dict)
    
    # Cost breakdown
    cpu_cost: float = 0.0
    memory_cost: float = 0.0
    api_cost: float = 0.0
    api_cost_breakdown: Dict[str, Any] = field(default_factory=dict)
    total_cost: float = 0.0
    
    # Metadata
    tags: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            'tx_id': self.tx_id,
            'timestamp': self.timestamp,
            'function_name': self.function_name,
            'feature': self.feature,
            'user_id': self.user_id,
            'request_id': self.request_id,
            'endpoint': self.endpoint,
            'parent_tx_id': self.parent_tx_id,
            'duration_ms': self.duration_ms,
            'cpu_time_ms': self.cpu_time_ms,
            'memory_mb': self.memory_mb,
            'allocated_memory_mb': self.allocated_memory_mb,
            'network_bytes': self.network_bytes,
            'api_calls': self.api_calls,
            'cpu_cost': self.cpu_cost,
            'memory_cost': self.memory_cost,
            'api_cost': self.api_cost,
            'api_cost_breakdown': self.api_cost_breakdown,
            'total_cost': self.total_cost,
            'tags': self.tags,
            'error': self.error,
        }


class CostTracker:
    """
    Tracks costs of function execution.
    
    Uses statistical profiling for low overhead.
    """
    
    def __init__(
        self,
        cost_model: Optional[CostModel] = None,
        enable_memory_tracking: bool = True,
        enable_cpu_tracking: bool = True,
        memory_tracking_mode: str = "process",
        default_memory_mb: float = 128.0,
    ):
        """
        Initialize cost tracker.
        
        Args:
            cost_model: Cost model to use (default: AWS)
            enable_memory_tracking: Track memory usage
            enable_cpu_tracking: Track CPU time
        """
        self.cost_model = cost_model or get_cost_model('aws')
        self.enable_memory_tracking = enable_memory_tracking
        self.enable_cpu_tracking = enable_cpu_tracking
        self.default_memory_mb = float(default_memory_mb)

        mode = (memory_tracking_mode or "process").lower()
        if not self.enable_memory_tracking:
            mode = "none"
        if mode not in {"tracemalloc", "process", "none"}:
            mode = "process"
        self.memory_tracking_mode = mode
        
        # Process handle for resource tracking
        self._process = psutil.Process()
        
        # Memory tracking
        self._memory_tracking_started = False
        if self.memory_tracking_mode == "tracemalloc":
            try:
                tracemalloc.start()
                self._memory_tracking_started = True
            except RuntimeError:
                # Already started
                self._memory_tracking_started = tracemalloc.is_tracing()
    
    def start_tracking(self, context: TransactionContext):
        """
        Start tracking for a transaction.
        
        Args:
            context: Transaction context
        """
        # Record start times
        context.start_time = time.perf_counter()
        
        if self.enable_cpu_tracking:
            try:
                cpu_times = self._process.cpu_times()
                context.start_cpu_time = cpu_times.user + cpu_times.system
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                context.start_cpu_time = 0.0
        
        if self.memory_tracking_mode == "tracemalloc" and self._memory_tracking_started:
            try:
                current, peak = tracemalloc.get_traced_memory()
                context.start_memory_bytes = current
            except Exception:
                context.start_memory_bytes = 0
        elif self.memory_tracking_mode == "process":
            context.start_memory_bytes = self._read_process_memory_bytes()
    
    def stop_tracking(self, context: TransactionContext, error: Optional[Exception] = None) -> CostRecord:
        """
        Stop tracking and calculate costs.
        
        Args:
            context: Transaction context
            error: Exception if function failed
        
        Returns:
            CostRecord with calculated costs
        """
        # Calculate duration
        end_time = time.perf_counter()
        duration_ms = (end_time - context.start_time) * 1000
        
        # Calculate CPU time
        cpu_time_ms = 0.0
        if self.enable_cpu_tracking:
            try:
                cpu_times = self._process.cpu_times()
                end_cpu_time = cpu_times.user + cpu_times.system
                cpu_time_ms = (end_cpu_time - context.start_cpu_time) * 1000
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                cpu_time_ms = duration_ms  # Fallback to wall time
        else:
            cpu_time_ms = duration_ms
        
        # Calculate memory usage
        memory_mb = 0.0
        if self.memory_tracking_mode == "tracemalloc" and self._memory_tracking_started:
            try:
                current, peak = tracemalloc.get_traced_memory()
                memory_delta_bytes = max(0, current - context.start_memory_bytes)
                memory_mb = memory_delta_bytes / (1024 * 1024)
            except Exception:
                memory_mb = self._read_process_memory_bytes() / (1024 * 1024)
        elif self.memory_tracking_mode == "process":
            try:
                end_memory = self._read_process_memory_bytes()
                memory_delta_bytes = max(0, end_memory - context.start_memory_bytes)
                memory_mb = memory_delta_bytes / (1024 * 1024)
                if memory_mb == 0.0:
                    # Use absolute process footprint when delta is near-zero.
                    memory_mb = end_memory / (1024 * 1024)
            except Exception:
                memory_mb = self.default_memory_mb
        else:
            memory_mb = self.default_memory_mb

        # Calculate costs
        duration_sec = duration_ms / 1000
        allocated_memory_mb = context.allocated_memory_mb
        costs = self.cost_model.calculate_total_cost(
            cpu_time_ms=cpu_time_ms,
            memory_mb=memory_mb,
            allocated_memory_mb=allocated_memory_mb,
            duration_sec=duration_sec,
            api_calls=context.api_calls,
        )
        
        # Create cost record
        api_cost_breakdown: Dict[str, Any] = {}
        raw_api_breakdown: Any = costs.get("api_cost_breakdown", {})
        if isinstance(raw_api_breakdown, dict):
            api_cost_breakdown = raw_api_breakdown

        record = CostRecord(
            tx_id=context.tx_id,
            timestamp=context.start_time,
            function_name=context.function_name,
            feature=context.feature,
            user_id=context.user_id,
            request_id=context.request_id,
            endpoint=context.endpoint,            # Bug 1 fix: was always None before
            parent_tx_id=context.parent_tx_id,
            duration_ms=duration_ms,
            cpu_time_ms=cpu_time_ms,
            memory_mb=memory_mb,
            allocated_memory_mb=allocated_memory_mb,
            api_calls=context.api_calls.copy(),
            cpu_cost=costs['cpu_cost'],
            memory_cost=costs['memory_cost'],
            api_cost=costs['api_cost'],
            api_cost_breakdown=api_cost_breakdown,
            total_cost=costs['total_cost'],
            tags=context.tags.copy(),
            error=str(error) if error else None,
        )
        
        return record

    def _read_process_memory_bytes(self) -> int:
        """Read process memory with a low-overhead RSS-first strategy."""
        try:
            return int(self._process.memory_info().rss)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            try:
                mem_full = self._process.memory_full_info()
                if hasattr(mem_full, "uss") and mem_full.uss is not None:
                    return int(mem_full.uss)
                return int(mem_full.rss)
            except (psutil.NoSuchProcess, psutil.AccessDenied, AttributeError):
                return int(self.default_memory_mb * 1024 * 1024)
        except AttributeError:
            try:
                mem_full = self._process.memory_full_info()
                if hasattr(mem_full, "uss") and mem_full.uss is not None:
                    return int(mem_full.uss)
                return int(mem_full.rss)
            except (psutil.NoSuchProcess, psutil.AccessDenied, AttributeError):
                return int(self.default_memory_mb * 1024 * 1024)
    
    def track_function(
        self,
        function_name: str,
        feature: Optional[str] = None,
        **tags
    ):
        """
        Context manager for tracking a function.
        
        Usage:
            with tracker.track_function('my_func', feature='search'):
                # Do work
                pass
        """
        return _TrackingContext(self, function_name, feature, tags)


class _TrackingContext:
    """Context manager for cost tracking."""
    
    def __init__(self, tracker: CostTracker, function_name: str, feature: Optional[str], tags: Dict[str, Any]):
        self.tracker = tracker
        self.function_name = function_name
        self.feature = feature
        self.tags = tags
        self.context: Optional[TransactionContext] = None
        self.record: Optional[CostRecord] = None
    
    def __enter__(self) -> 'CostTracker':
        """Enter tracking context."""
        ctx_mgr = get_context_manager()
        tx_context = ctx_mgr.start_transaction(
            function_name=self.function_name,
            feature=self.feature,
            **self.tags
        )
        self.context = tx_context
        self.tracker.start_tracking(tx_context)
        return self.tracker
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit tracking context."""
        if self.context:
            self.record = self.tracker.stop_tracking(self.context, exc_val)
            get_context_manager().end_transaction()
        return False  # Don't suppress exceptions


# Global tracker instance
_global_tracker: Optional[CostTracker] = None


def get_tracker() -> CostTracker:
    """Get or create global tracker instance."""
    global _global_tracker
    if _global_tracker is None:
        _global_tracker = CostTracker()
    return _global_tracker


def set_tracker(tracker: CostTracker):
    """Set global tracker instance."""
    global _global_tracker
    _global_tracker = tracker
