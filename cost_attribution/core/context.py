"""
Cost Attribution - Context Management

Provides thread-local and async-safe context for cost tracking.
Uses contextvars for async compatibility.
"""

from __future__ import annotations

import contextvars
import asyncio
import uuid
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Awaitable
from datetime import datetime, timezone


# Context variables (async-safe)
_current_transaction: contextvars.ContextVar[Optional[TransactionContext]] = contextvars.ContextVar(
    "current_transaction",
    default=None,
)
_request_context: contextvars.ContextVar[Optional[RequestContext]] = contextvars.ContextVar(
    "request_context",
    default=None,
)


@dataclass
class RequestContext:
    """Context for an entire request/operation."""
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    user_id: Optional[str] = None
    feature: Optional[str] = None
    endpoint: Optional[str] = None
    tags: Dict[str, Any] = field(default_factory=dict)
    start_time: float = field(default_factory=lambda: datetime.now(timezone.utc).timestamp())
    _token: Optional[contextvars.Token] = field(default=None, repr=False, compare=False)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'request_id': self.request_id,
            'user_id': self.user_id,
            'feature': self.feature,
            'endpoint': self.endpoint,
            'tags': self.tags,
            'start_time': self.start_time,
        }


@dataclass
class TransactionContext:
    """Context for a single function call (transaction)."""
    tx_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    function_name: str = ""
    feature: Optional[str] = None
    start_time: float = 0.0
    start_cpu_time: float = 0.0
    start_memory_bytes: int = 0
    allocated_memory_mb: Optional[float] = None

    # Request context (inherited from the active RequestContext)
    request_id: Optional[str] = None
    user_id: Optional[str] = None
    endpoint: Optional[str] = None   # Bug 1 fix: wired from RequestContext

    # Parent transaction (for nested calls)
    parent_tx_id: Optional[str] = None

    # Collected data
    api_calls: Dict[str, Any] = field(default_factory=dict)
    tags: Dict[str, Any] = field(default_factory=dict)
    _token: Optional[contextvars.Token] = field(default=None, repr=False, compare=False)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'tx_id': self.tx_id,
            'function_name': self.function_name,
            'feature': self.feature,
            'request_id': self.request_id,
            'user_id': self.user_id,
            'endpoint': self.endpoint,
            'parent_tx_id': self.parent_tx_id,
            'allocated_memory_mb': self.allocated_memory_mb,
            'api_calls': self.api_calls,
            'tags': self.tags,
        }


class ContextManager:
    """
    Manages cost tracking context in thread-safe and async-safe manner.
    
    Uses contextvars for async compatibility.
    """
    
    def __init__(self):
        """Initialize context manager."""
        pass
    
    # Request context
    
    def set_request_context(self, context: RequestContext):
        """Set request context for current execution."""
        token = _request_context.set(context)
        context._token = token
    
    def get_request_context(self) -> Optional[RequestContext]:
        """Get current request context."""
        return _request_context.get()
    
    def clear_request_context(self):
        """Clear request context."""
        _request_context.set(None)
    
    # Transaction context
    
    def set_transaction_context(self, context: TransactionContext):
        """Set transaction context for current execution."""
        token = _current_transaction.set(context)
        context._token = token
    
    def get_transaction_context(self) -> Optional[TransactionContext]:
        """Get current transaction context."""
        return _current_transaction.get()
    
    def clear_transaction_context(self):
        """Clear transaction context."""
        _current_transaction.set(None)
    
    # Helper methods
    
    def start_request(
        self, 
        request_id: Optional[str] = None,
        user_id: Optional[str] = None,
        feature: Optional[str] = None,
        endpoint: Optional[str] = None,
        **tags
    ) -> RequestContext:
        """Start a new request context."""
        context = RequestContext(
            request_id=request_id or str(uuid.uuid4()),
            user_id=user_id,
            feature=feature,
            endpoint=endpoint,
            tags=tags,
        )
        self.set_request_context(context)
        return context
    
    def end_request(self) -> Optional[RequestContext]:
        """End current request context and return it."""
        context = self.get_request_context()
        if context and context._token is not None:
            _request_context.reset(context._token)
            context._token = None
        else:
            self.clear_request_context()
        return context
    
    def start_transaction(
        self,
        function_name: str,
        feature: Optional[str] = None,
        start_time: Optional[float] = None,
        start_cpu_time: Optional[float] = None,
        **tags
    ) -> TransactionContext:
        """Start a new transaction context."""
        # Get request context if exists
        req_ctx = self.get_request_context()
        
        # Get parent transaction if exists
        parent_tx = self.get_transaction_context()

        allocated_memory_raw = tags.get("allocated_memory_mb")
        allocated_memory_mb: Optional[float] = None
        if allocated_memory_raw is not None:
            try:
                allocated_memory_mb = float(allocated_memory_raw)
            except (TypeError, ValueError):
                allocated_memory_mb = None
        
        context = TransactionContext(
            function_name=function_name,
            feature=feature or (req_ctx.feature if req_ctx else None),
            start_time=start_time or datetime.now(timezone.utc).timestamp(),
            start_cpu_time=start_cpu_time or 0.0,
            allocated_memory_mb=allocated_memory_mb,
            request_id=req_ctx.request_id if req_ctx else None,
            user_id=req_ctx.user_id if req_ctx else None,
            endpoint=req_ctx.endpoint if req_ctx else None,   # Bug 1 fix
            parent_tx_id=parent_tx.tx_id if parent_tx else None,
            tags=tags,
        )
        
        self.set_transaction_context(context)
        return context
    
    def end_transaction(self) -> Optional[TransactionContext]:
        """End current transaction context and return it."""
        context = self.get_transaction_context()
        if context and context._token is not None:
            _current_transaction.reset(context._token)
            context._token = None
        else:
            self.clear_transaction_context()
        return context
    
    def add_api_call(
        self,
        service_name: str,
        count: int = 1,
        input_tokens: Optional[int] = None,
        output_tokens: Optional[int] = None,
        **metadata,
    ):
        """Record an API call in current transaction."""
        tx_ctx = self.get_transaction_context()
        if not tx_ctx:
            return

        usage = tx_ctx.api_calls.setdefault(service_name, {"count": 0})
        if not isinstance(usage, dict):
            usage = {"count": int(usage)}
            tx_ctx.api_calls[service_name] = usage

        usage["count"] = int(usage.get("count", 0)) + int(count)
        if input_tokens is not None:
            usage["input_tokens"] = int(usage.get("input_tokens", 0)) + int(input_tokens)
        if output_tokens is not None:
            usage["output_tokens"] = int(usage.get("output_tokens", 0)) + int(output_tokens)
        if metadata:
            meta = usage.setdefault("metadata", {})
            if not isinstance(meta, dict):
                meta = {}
                usage["metadata"] = meta
            meta.update(metadata)
    
    def add_tag(self, key: str, value: Any):
        """Add a tag to current transaction."""
        tx_ctx = self.get_transaction_context()
        if tx_ctx:
            tx_ctx.tags[key] = value


# Global context manager instance
_context_manager = ContextManager()


def get_context_manager() -> ContextManager:
    """Get the global context manager instance."""
    return _context_manager


# Convenience functions

def start_request(**kwargs) -> RequestContext:
    """Start a request context."""
    return _context_manager.start_request(**kwargs)


def end_request() -> Optional[RequestContext]:
    """End request context."""
    return _context_manager.end_request()


def start_transaction(function_name: str, **kwargs) -> TransactionContext:
    """Start a transaction context."""
    return _context_manager.start_transaction(function_name, **kwargs)


def end_transaction() -> Optional[TransactionContext]:
    """End transaction context."""
    return _context_manager.end_transaction()


def add_api_call(
    service_name: str,
    count: int = 1,
    input_tokens: Optional[int] = None,
    output_tokens: Optional[int] = None,
    **metadata,
):
    """Record an API call."""
    _context_manager.add_api_call(
        service_name=service_name,
        count=count,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        **metadata,
    )


def add_tag(key: str, value: Any):
    """Add a tag to current transaction."""
    _context_manager.add_tag(key, value)


def get_current_request() -> Optional[RequestContext]:
    """Get current request context."""
    return _context_manager.get_request_context()


def get_current_transaction() -> Optional[TransactionContext]:
    """Get current transaction context."""
    return _context_manager.get_transaction_context()


def copy_current_context() -> contextvars.Context:
    """Copy the current context for manual asyncio task propagation."""
    return contextvars.copy_context()


def create_task_with_context(coro: Awaitable, *, name: Optional[str] = None) -> asyncio.Task:
    """
    Create an asyncio task that preserves the current contextvars context.

    Useful when spawning tasks manually so request/transaction attribution survives.
    """
    ctx = copy_current_context()
    loop = asyncio.get_running_loop()

    async def _runner():
        return await coro

    task_coro = ctx.run(_runner)
    if name is not None:
        return loop.create_task(task_coro, name=name)
    return loop.create_task(task_coro)
