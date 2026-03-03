import asyncio
import json
import os
import time

from cost_attribution import (
    AsyncLogger,
    SQLiteStorage,
    clear_sampling_policy,
    cost_track,
    get_global_sample_rate,
    reload_sampling_from_env,
    set_circuit_breaker,
    set_global_sample_rate,
    set_sampling_policy,
    track,
)
from cost_attribution.core.context import (
    add_api_call,
    copy_current_context,
    create_task_with_context,
    get_context_manager,
    get_current_request,
)
from cost_attribution.core.models import AWSCostModel
from cost_attribution.core.tracker import CostRecord
from cost_attribution.utils.async_logger import get_async_logger, set_async_logger
from cost_attribution.utils.circuit_breaker import CircuitBreaker


def test_sqlite_storage_roundtrip(tmp_path):
    db = tmp_path / "roundtrip.db"
    storage = SQLiteStorage(str(db))

    record = CostRecord(
        tx_id="tx-1",
        timestamp=time.time(),
        function_name="fn",
        feature="search",
        user_id="u1",
        request_id="r1",
        duration_ms=12.5,
        cpu_time_ms=3.5,
        memory_mb=64.0,
        api_calls={"svc": 1},
        cpu_cost=0.01,
        memory_cost=0.02,
        api_cost=0.03,
        total_cost=0.06,
        tags={"a": "b"},
    )
    storage.store(record)

    rows = storage.query(feature="search")
    assert len(rows) == 1
    assert rows[0]["tx_id"] == "tx-1"


def test_sqlite_retention_policy(tmp_path):
    db = tmp_path / "retention.db"
    storage = SQLiteStorage(str(db))
    storage.set_retention(raw_data_days=2, hourly_rollups_days=7, daily_rollups_days=30)

    policy = storage.get_retention_policy()
    assert policy["raw_data_days"] == 2
    assert policy["hourly_rollups_days"] == 7
    assert policy["daily_rollups_days"] == 30


def test_circuit_breaker_state_machine():
    breaker = CircuitBreaker(error_threshold=2, error_window_sec=60, recovery_timeout_sec=1)
    assert breaker.allow_request() is True
    breaker.record_failure()
    assert breaker.state == CircuitBreaker.CLOSED
    breaker.record_failure()
    assert breaker.state == CircuitBreaker.OPEN
    assert breaker.allow_request() is False


def test_cost_track_redact_args(tmp_path):
    db = tmp_path / "redact.db"
    storage = SQLiteStorage(str(db))
    logger = AsyncLogger(storage_backend=storage, flush_interval=0.1)
    set_async_logger(logger)

    @cost_track(feature="payments", redact_args=True)
    def process_payment(card_number, amount):
        return amount

    process_payment("4111111111111111", 10)
    logger.stop()

    rows = storage.query(feature="payments")
    assert len(rows) == 1
    tags = json.loads(rows[0]["tags"]) if rows[0]["tags"] else {}
    assert tags.get("args_redacted") is True
    assert "4111111111111111" not in json.dumps(tags)

    # Keep global logger in a stopped state for clean test shutdown.
    get_async_logger().stop()


def test_timescaledb_module_importable():
    from cost_attribution.storage.timescaledb import TimescaleDBStorage

    assert TimescaleDBStorage is not None


def test_add_api_call_token_payload(tmp_path):
    db = tmp_path / "tokens.db"
    storage = SQLiteStorage(str(db))
    logger = AsyncLogger(storage_backend=storage, flush_interval=0.1)
    set_async_logger(logger)

    @cost_track(feature="ai")
    def call_model():
        add_api_call(
            "bedrock_claude_3_sonnet",
            count=1,
            input_tokens=1200,
            output_tokens=350,
            model_version="3.5",
        )

    call_model()
    logger.stop()

    rows = storage.query(feature="ai")
    assert len(rows) == 1
    api_calls = json.loads(rows[0]["api_calls"])
    assert api_calls["bedrock_claude_3_sonnet"]["count"] == 1
    assert api_calls["bedrock_claude_3_sonnet"]["input_tokens"] == 1200
    assert api_calls["bedrock_claude_3_sonnet"]["output_tokens"] == 350
    breakdown = json.loads(rows[0]["api_cost_breakdown"])
    assert breakdown["bedrock_claude_3_sonnet"]["count_unit"] == "request"
    assert breakdown["bedrock_claude_3_sonnet"]["input_tokens"] == 1200


def test_lambda_billing_mode_uses_allocated_memory():
    model = AWSCostModel()
    # 1024MB for 1 second at $0.0000166667/GB-s
    costs = model.calculate_total_cost(
        cpu_time_ms=0.0,
        memory_mb=10.0,
        allocated_memory_mb=1024.0,
        duration_sec=1.0,
        api_calls={},
    )
    assert abs(costs["cpu_cost"] - 0.0000166667) < 1e-12
    assert costs["memory_cost"] == 0.0


def test_context_token_reset_restores_parent():
    ctx_mgr = get_context_manager()
    ctx_mgr.start_request(user_id="u1", feature="api")
    tx_parent = ctx_mgr.start_transaction(function_name="parent", feature="api")
    tx_child = ctx_mgr.start_transaction(function_name="child", feature="api")
    assert ctx_mgr.get_transaction_context().tx_id == tx_child.tx_id
    ctx_mgr.end_transaction()
    assert ctx_mgr.get_transaction_context().tx_id == tx_parent.tx_id
    ctx_mgr.end_transaction()
    assert ctx_mgr.get_transaction_context() is None
    ctx_mgr.end_request()
    assert ctx_mgr.get_request_context() is None


def test_copy_current_context_available():
    ctx = copy_current_context()
    assert ctx is not None


def test_create_task_with_context_propagates_request():
    async def main():
        ctx_mgr = get_context_manager()
        ctx_mgr.start_request(user_id="u_ctx", feature="api")
        try:
            async def child():
                req = get_current_request()
                return req.user_id if req else None

            task = create_task_with_context(child())
            return await task
        finally:
            ctx_mgr.end_request()

    result = asyncio.run(main())
    assert result == "u_ctx"


def test_cost_track_sampling_zero_skips_record(tmp_path):
    db = tmp_path / "sampled.db"
    storage = SQLiteStorage(str(db))
    logger = AsyncLogger(storage_backend=storage, flush_interval=0.1)
    set_async_logger(logger)

    @cost_track(feature="sampled", sample_rate=0.0)
    def do_work():
        return 1

    assert do_work() == 1
    logger.stop()
    rows = storage.query(feature="sampled")
    assert len(rows) == 0


def test_cost_track_circuit_breaker_override_false(tmp_path):
    db = tmp_path / "breaker_override.db"
    storage = SQLiteStorage(str(db))
    logger = AsyncLogger(storage_backend=storage, flush_interval=0.1)
    set_async_logger(logger)

    class _DenyBreaker:
        def allow_request(self):
            return False

        def record_success(self):
            pass

        def record_failure(self):
            pass

    set_circuit_breaker(_DenyBreaker())
    try:
        @cost_track(feature="breaker_override", circuit_breaker=False)
        def do_work():
            return 7

        assert do_work() == 7
        logger.stop()
        rows = storage.query(feature="breaker_override")
        assert len(rows) == 1
    finally:
        set_circuit_breaker(None)


def test_track_async_context_manager(tmp_path):
    db = tmp_path / "async_track.db"
    storage = SQLiteStorage(str(db))
    logger = AsyncLogger(storage_backend=storage, flush_interval=0.1)
    set_async_logger(logger)

    async def _run():
        async with track(feature="async", operation="async_op"):
            await asyncio.sleep(0.001)

    asyncio.run(_run())
    logger.stop()
    rows = storage.query(feature="async")
    assert len(rows) == 1


def test_global_sample_rate_zero_skips_all(tmp_path):
    db = tmp_path / "global_sample.db"
    storage = SQLiteStorage(str(db))
    logger = AsyncLogger(storage_backend=storage, flush_interval=0.1)
    set_async_logger(logger)
    set_global_sample_rate(0.0)
    try:
        @cost_track(feature="global_sampled")
        def do_work():
            return 1

        assert do_work() == 1
        logger.stop()
        rows = storage.query(feature="global_sampled")
        assert len(rows) == 0
    finally:
        set_global_sample_rate(None)


def test_sampling_policy_cap(tmp_path):
    db = tmp_path / "policy_sample.db"
    storage = SQLiteStorage(str(db))
    logger = AsyncLogger(storage_backend=storage, flush_interval=0.1)
    set_async_logger(logger)

    def policy(function_name, feature, tags):
        del function_name, tags
        if feature == "policy_feature":
            return 0.0
        return None

    set_sampling_policy(policy)
    try:
        @cost_track(feature="policy_feature", sample_rate=1.0)
        def do_work():
            return 1

        assert do_work() == 1
        logger.stop()
        rows = storage.query(feature="policy_feature")
        assert len(rows) == 0
    finally:
        clear_sampling_policy()


def test_reload_sampling_from_env():
    old = os.environ.get("COST_ATTRIBUTION_SAMPLE_RATE")
    try:
        os.environ["COST_ATTRIBUTION_SAMPLE_RATE"] = "0.25"
        reload_sampling_from_env()
        assert abs((get_global_sample_rate() or 0) - 0.25) < 1e-12
    finally:
        if old is None:
            os.environ.pop("COST_ATTRIBUTION_SAMPLE_RATE", None)
        else:
            os.environ["COST_ATTRIBUTION_SAMPLE_RATE"] = old
        reload_sampling_from_env()
