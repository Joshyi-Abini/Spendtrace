"""
Cost Attribution - Cost Models

Defines cost models for different cloud providers and pricing providers.
"""

from dataclasses import dataclass, field
import json
import threading
import time
from typing import Any, Dict, Optional


DEFAULT_AWS_API_COSTS = {
    # AWS DynamoDB — https://aws.amazon.com/dynamodb/pricing/on-demand/
    "dynamodb_read":  0.00000025,   # $0.25 per million RRUs
    "dynamodb_write": 0.00000125,   # $1.25 per million WRUs
    "dynamodb_query": 0.00000025,   # counted as RRUs

    # Amazon S3 — https://aws.amazon.com/s3/pricing/
    "s3_get":  0.0000004,           # $0.0004 per 1 000 GET requests
    "s3_put":  0.000005,            # $0.005  per 1 000 PUT/POST requests
    "s3_list": 0.000005,            # $0.005  per 1 000 LIST requests

    # Amazon SQS — https://aws.amazon.com/sqs/pricing/
    "sqs_send":    0.0000004,       # $0.40 per million requests
    "sqs_receive": 0.0000004,

    # Amazon SNS — https://aws.amazon.com/sns/pricing/
    "sns_publish": 0.0000005,       # $0.50 per million publishes

    # Amazon API Gateway — https://aws.amazon.com/api-gateway/pricing/
    "api_gateway_request": 0.0000035,  # $3.50 per million calls

    # AWS Lambda — https://aws.amazon.com/lambda/pricing/
    "aws_lambda_request": 0.0000002,   # $0.20 per million requests

    # Amazon Bedrock (Claude) — https://aws.amazon.com/bedrock/pricing/
    # Per-request estimate; use token-aware keys for accurate LLM billing
    "bedrock_claude_3_haiku":  0.00025,
    "bedrock_claude_3_sonnet": 0.003,
    "bedrock_claude_3_opus":   0.015,

    # OpenAI (via direct API, not Bedrock) — https://openai.com/pricing
    "openai_gpt4":       0.03,
    "openai_gpt4_turbo": 0.01,

    # Anthropic direct API — https://www.anthropic.com/pricing
    "anthropic_claude": 0.003,
}

DEFAULT_AWS_LLM_INPUT_COSTS_PER_TOKEN = {
    # USD per input token
    "bedrock_claude_3_haiku": 0.00000025,
    "bedrock_claude_3_sonnet": 0.000003,
    "bedrock_claude_3_opus": 0.000015,
    "openai_gpt4": 0.00003,
    "openai_gpt4_turbo": 0.00001,
    "anthropic_claude": 0.000003,
}

DEFAULT_AWS_LLM_OUTPUT_COSTS_PER_TOKEN = {
    # USD per output token (often higher than input)
    "bedrock_claude_3_haiku": 0.00000125,
    "bedrock_claude_3_sonnet": 0.000015,
    "bedrock_claude_3_opus": 0.000075,
    "openai_gpt4": 0.00006,
    "openai_gpt4_turbo": 0.00003,
    "anthropic_claude": 0.000015,
}

DEFAULT_GCP_API_COSTS = {
    "firestore_read": 0.00000036,
    "firestore_write": 0.0000018,
    "gcs_read": 0.0000004,
    "gcs_write": 0.000005,
    "pubsub_publish": 0.00000004,
    "pubsub_pull": 0.00000004,
    "vertex_ai_prediction": 0.001,
    "openai_gpt4": 0.03,
    "anthropic_claude": 0.003,
}

DEFAULT_AZURE_API_COSTS = {
    "cosmosdb_read": 0.0000003,
    "cosmosdb_write": 0.0000015,
    "blob_read": 0.0000005,
    "blob_write": 0.000005,
    "servicebus_send": 0.00000005,
    "servicebus_receive": 0.00000005,
    "azure_openai_gpt4": 0.03,
    "azure_openai_gpt35": 0.002,
}


@dataclass
class PricingSnapshot:
    cpu_cost_per_ms: float
    memory_cost_per_mb_sec: float
    api_costs: Dict[str, float]
    source: str
    updated_at: float = field(default_factory=time.time)


class PricingProvider:
    """Base class for pricing providers with cached snapshots."""

    def __init__(self, refresh_interval_sec: int = 86400, auto_refresh: bool = False):
        self.refresh_interval_sec = max(60, int(refresh_interval_sec))
        self._snapshot: Optional[PricingSnapshot] = None
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        if auto_refresh:
            self.start_auto_refresh()

    def get_rates(self, force_refresh: bool = False) -> PricingSnapshot:
        with self._lock:
            if force_refresh or self._snapshot is None or self._is_stale(self._snapshot):
                self._snapshot = self._load_rates(self._snapshot)
            return self._snapshot

    def start_auto_refresh(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._refresh_loop, daemon=True)
        self._thread.start()

    def stop_auto_refresh(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2)

    def _refresh_loop(self):
        while not self._stop_event.is_set():
            try:
                self.get_rates(force_refresh=True)
            except Exception:
                # Keep existing snapshot if refresh fails.
                pass
            self._stop_event.wait(self.refresh_interval_sec)

    def _is_stale(self, snapshot: PricingSnapshot) -> bool:
        return (time.time() - snapshot.updated_at) >= self.refresh_interval_sec

    def _load_rates(self, previous: Optional[PricingSnapshot]) -> PricingSnapshot:
        raise NotImplementedError


class StaticPricingProvider(PricingProvider):
    """Static pricing provider with optional periodic re-emit."""

    def __init__(
        self,
        cpu_cost_per_ms: float,
        memory_cost_per_mb_sec: float,
        api_costs: Dict[str, float],
        source: str = "static",
        refresh_interval_sec: int = 86400,
        auto_refresh: bool = False,
    ):
        self._cpu_cost_per_ms = float(cpu_cost_per_ms)
        self._memory_cost_per_mb_sec = float(memory_cost_per_mb_sec)
        self._api_costs = dict(api_costs)
        self._source = source
        super().__init__(refresh_interval_sec=refresh_interval_sec, auto_refresh=auto_refresh)

    def _load_rates(self, previous: Optional[PricingSnapshot]) -> PricingSnapshot:
        del previous
        return PricingSnapshot(
            cpu_cost_per_ms=self._cpu_cost_per_ms,
            memory_cost_per_mb_sec=self._memory_cost_per_mb_sec,
            api_costs=dict(self._api_costs),
            source=self._source,
        )


class AWSDynamicPricingProvider(StaticPricingProvider):
    """
    AWS dynamic pricing provider using boto3 Pricing API with static fallback.

    If boto3 is unavailable or AWS pricing queries fail, static values are used.
    """

    def __init__(
        self,
        region: str = "us-east-1",
        architecture: str = "x86",
        refresh_interval_sec: int = 86400,
        auto_refresh: bool = False,
        fallback_cpu_cost_per_ms: float = 0.0000000166667,
        fallback_memory_cost_per_mb_sec: float = 0.0,
        fallback_api_costs: Optional[Dict[str, float]] = None,
    ):
        self.region = region
        self.architecture = architecture.lower()
        super().__init__(
            cpu_cost_per_ms=fallback_cpu_cost_per_ms,
            memory_cost_per_mb_sec=fallback_memory_cost_per_mb_sec,
            api_costs=fallback_api_costs or DEFAULT_AWS_API_COSTS,
            source="aws_static_fallback",
            refresh_interval_sec=refresh_interval_sec,
            auto_refresh=auto_refresh,
        )

    def _load_rates(self, previous: Optional[PricingSnapshot]) -> PricingSnapshot:
        fallback = super()._load_rates(previous)
        try:
            import boto3  # type: ignore
        except Exception:
            return fallback

        try:
            pricing = boto3.client("pricing", region_name="us-east-1")
            api_costs = dict(fallback.api_costs)
            mem_rate = fallback.memory_cost_per_mb_sec
            cpu_rate = fallback.cpu_cost_per_ms
            updated = False
            location = self._aws_location_name(self.region)
            usage_type = "Lambda-GB-Second-Arm" if self.architecture in {"arm", "arm64", "graviton"} else "Lambda-GB-Second"

            lambda_gb_sec = self._query_price(
                pricing,
                service_code="AWSLambda",
                filters=[
                    {"Type": "TERM_MATCH", "Field": "location", "Value": location},
                    {"Type": "TERM_MATCH", "Field": "group", "Value": "AWS-Lambda-Duration"},
                    {"Type": "TERM_MATCH", "Field": "usagetype", "Value": usage_type},
                ],
            )
            if lambda_gb_sec is not None:
                # Treat lambda compute as primary CPU-time proxy (ms -> sec).
                cpu_rate = lambda_gb_sec / 1000.0
                mem_rate = 0.0
                updated = True

            lambda_request = self._query_price(
                pricing,
                service_code="AWSLambda",
                filters=[
                    {"Type": "TERM_MATCH", "Field": "location", "Value": location},
                    {"Type": "TERM_MATCH", "Field": "group", "Value": "AWS-Lambda-Requests"},
                ],
            )
            if lambda_request is not None:
                api_costs["aws_lambda_request"] = lambda_request
                updated = True

            source = "aws_pricing_api" if updated else fallback.source
            return PricingSnapshot(
                cpu_cost_per_ms=cpu_rate,
                memory_cost_per_mb_sec=mem_rate,
                api_costs=api_costs,
                source=source,
            )
        except Exception:
            return fallback

    @staticmethod
    def _aws_location_name(region: str) -> str:
        mapping = {
            "us-east-1": "US East (N. Virginia)",
            "us-east-2": "US East (Ohio)",
            "us-west-1": "US West (N. California)",
            "us-west-2": "US West (Oregon)",
            "eu-west-1": "EU (Ireland)",
            "eu-west-2": "EU (London)",
            "eu-central-1": "EU (Frankfurt)",
            "ap-southeast-1": "Asia Pacific (Singapore)",
            "ap-southeast-2": "Asia Pacific (Sydney)",
            "ap-northeast-1": "Asia Pacific (Tokyo)",
        }
        return mapping.get(region, "US East (N. Virginia)")

    def _query_price(self, pricing_client, service_code: str, filters: list) -> Optional[float]:
        paginator = pricing_client.get_paginator("get_products")
        page_iter = paginator.paginate(ServiceCode=service_code, Filters=filters, MaxResults=10)
        candidates = []
        for page in page_iter:
            price_list = page.get("PriceList", [])
            for item in price_list:
                data = json.loads(item)
                terms = data.get("terms", {}).get("OnDemand", {})
                for offer in terms.values():
                    for dimension in offer.get("priceDimensions", {}).values():
                        raw = dimension.get("pricePerUnit", {}).get("USD")
                        if raw is None:
                            continue
                        try:
                            value = float(raw)
                            if value >= 0:
                                candidates.append(value)
                        except Exception:
                            continue
        if not candidates:
            return None
        return min(candidates)


@dataclass
class CostModel:
    """Base class for cost models."""

    cpu_cost_per_ms: float = 0.000001
    memory_cost_per_mb_sec: float = 0.0000000017
    api_costs: Dict[str, float] = field(default_factory=dict)
    api_units: Dict[str, str] = field(default_factory=dict)
    llm_input_costs: Dict[str, float] = field(default_factory=dict)
    llm_output_costs: Dict[str, float] = field(default_factory=dict)
    pricing_provider: Optional[PricingProvider] = None
    calibration_factor: float = 1.0
    billing_mode: str = "generic"
    lambda_gb_second_rate: float = 0.0000166667

    def __post_init__(self):
        if self.pricing_provider is None:
            self.pricing_provider = StaticPricingProvider(
                cpu_cost_per_ms=self.cpu_cost_per_ms,
                memory_cost_per_mb_sec=self.memory_cost_per_mb_sec,
                api_costs=self.api_costs,
                source="model_static",
            )

    def _current_rates(self) -> PricingSnapshot:
        snap = self.pricing_provider.get_rates() if self.pricing_provider else PricingSnapshot(
            cpu_cost_per_ms=self.cpu_cost_per_ms,
            memory_cost_per_mb_sec=self.memory_cost_per_mb_sec,
            api_costs=dict(self.api_costs),
            source="model_static",
        )
        merged_api_costs = dict(snap.api_costs)
        merged_api_costs.update(self.api_costs)
        return PricingSnapshot(
            cpu_cost_per_ms=snap.cpu_cost_per_ms,
            memory_cost_per_mb_sec=snap.memory_cost_per_mb_sec,
            api_costs=merged_api_costs,
            source=snap.source,
            updated_at=snap.updated_at,
        )

    def calculate_cpu_cost(self, cpu_time_ms: float) -> float:
        rates = self._current_rates()
        return cpu_time_ms * rates.cpu_cost_per_ms

    def calculate_memory_cost(self, memory_mb: float, duration_sec: float) -> float:
        rates = self._current_rates()
        return memory_mb * duration_sec * rates.memory_cost_per_mb_sec

    def calculate_api_cost_breakdown(self, api_calls: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        rates = self._current_rates()
        breakdown: Dict[str, Dict[str, Any]] = {}
        for service, usage in api_calls.items():
            count = 0.0
            input_tokens = 0.0
            output_tokens = 0.0
            if isinstance(usage, dict):
                count = float(usage.get("count", 0))
                input_tokens = float(usage.get("input_tokens", 0))
                output_tokens = float(usage.get("output_tokens", 0))
            else:
                count = float(usage)

            count_unit_rate = rates.api_costs.get(service, 0.0)
            input_unit_rate = self.llm_input_costs.get(service, 0.0)
            output_unit_rate = self.llm_output_costs.get(service, 0.0)

            count_cost = count_unit_rate * count
            input_cost = input_unit_rate * input_tokens
            output_cost = output_unit_rate * output_tokens

            breakdown[service] = {
                "count": count,
                "count_unit": self.api_units.get(service, "request"),
                "count_unit_rate_usd": count_unit_rate,
                "count_cost_usd": count_cost,
                "input_tokens": input_tokens,
                "input_unit_rate_usd": input_unit_rate,
                "input_cost_usd": input_cost,
                "output_tokens": output_tokens,
                "output_unit_rate_usd": output_unit_rate,
                "output_cost_usd": output_cost,
                "total_cost_usd": count_cost + input_cost + output_cost,
            }
        return breakdown

    def calculate_api_cost(self, api_calls: Dict[str, Any]) -> float:
        return sum(v["total_cost_usd"] for v in self.calculate_api_cost_breakdown(api_calls).values())

    def calculate_total_cost(
        self,
        cpu_time_ms: float,
        memory_mb: float,
        allocated_memory_mb: Optional[float],
        duration_sec: float,
        api_calls: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, float]:
        if self.billing_mode == "lambda_gb_s":
            alloc_mb = float(allocated_memory_mb if allocated_memory_mb is not None else memory_mb)
            gb_seconds = max(0.0, alloc_mb / 1024.0) * max(0.0, duration_sec)
            cpu_cost = gb_seconds * float(self.lambda_gb_second_rate)
            memory_cost = 0.0
        else:
            cpu_cost = self.calculate_cpu_cost(cpu_time_ms)
            memory_cost = self.calculate_memory_cost(memory_mb, duration_sec)
        api_cost_breakdown = self.calculate_api_cost_breakdown(api_calls or {})
        api_cost = sum(v["total_cost_usd"] for v in api_cost_breakdown.values())
        factor = max(0.0, float(self.calibration_factor))
        cpu_cost *= factor
        memory_cost *= factor
        api_cost *= factor
        scaled_api_cost_breakdown = {}
        for service, item in api_cost_breakdown.items():
            scaled = dict(item)
            scaled["count_cost_usd"] = scaled["count_cost_usd"] * factor
            scaled["input_cost_usd"] = scaled["input_cost_usd"] * factor
            scaled["output_cost_usd"] = scaled["output_cost_usd"] * factor
            scaled["total_cost_usd"] = scaled["total_cost_usd"] * factor
            scaled_api_cost_breakdown[service] = scaled
        return {
            "cpu_cost": cpu_cost,
            "memory_cost": memory_cost,
            "api_cost": api_cost,
            "total_cost": cpu_cost + memory_cost + api_cost,
            "api_cost_breakdown": scaled_api_cost_breakdown,
            "billing_mode": self.billing_mode,
        }

    def set_calibration_factor(self, factor: float):
        self.calibration_factor = float(factor)


@dataclass
class AWSCostModel(CostModel):
    """Cost model for AWS-style pricing."""

    cpu_cost_per_ms: float = 0.0000000166667
    memory_cost_per_mb_sec: float = 0.0
    api_costs: Dict[str, float] = field(default_factory=lambda: dict(DEFAULT_AWS_API_COSTS))
    api_units: Dict[str, str] = field(
        default_factory=lambda: {
            "dynamodb_read": "rru",
            "dynamodb_write": "wru",
            "dynamodb_query": "rru",
            "s3_get": "request",
            "s3_put": "request",
            "s3_list": "request",
            "sqs_send": "request",
            "sqs_receive": "request",
            "sns_publish": "request",
            "api_gateway_request": "request",
            "aws_lambda_request": "request",
            "bedrock_claude_3_haiku": "request",
            "bedrock_claude_3_sonnet": "request",
            "bedrock_claude_3_opus": "request",
            "openai_gpt4": "request",
            "openai_gpt4_turbo": "request",
            "anthropic_claude": "request",
        }
    )
    llm_input_costs: Dict[str, float] = field(default_factory=lambda: dict(DEFAULT_AWS_LLM_INPUT_COSTS_PER_TOKEN))
    llm_output_costs: Dict[str, float] = field(default_factory=lambda: dict(DEFAULT_AWS_LLM_OUTPUT_COSTS_PER_TOKEN))
    billing_mode: str = "lambda_gb_s"
    lambda_gb_second_rate: float = 0.0000166667


@dataclass
class GCPCostModel(CostModel):
    """Cost model for GCP-style pricing."""

    cpu_cost_per_ms: float = 0.0000012
    memory_cost_per_mb_sec: float = 0.0000000018
    api_costs: Dict[str, float] = field(default_factory=lambda: dict(DEFAULT_GCP_API_COSTS))


@dataclass
class AzureCostModel(CostModel):
    """Cost model for Azure-style pricing."""

    cpu_cost_per_ms: float = 0.0000011
    memory_cost_per_mb_sec: float = 0.0000000016
    api_costs: Dict[str, float] = field(default_factory=lambda: dict(DEFAULT_AZURE_API_COSTS))


def get_pricing_provider(
    provider: str = "aws",
    dynamic: bool = False,
    refresh_interval_sec: int = 86400,
    auto_refresh: bool = False,
) -> PricingProvider:
    provider = provider.lower()
    if provider == "aws" and dynamic:
        return AWSDynamicPricingProvider(
            refresh_interval_sec=refresh_interval_sec,
            auto_refresh=auto_refresh,
        )
    if provider == "gcp":
        return StaticPricingProvider(
            cpu_cost_per_ms=0.0000012,
            memory_cost_per_mb_sec=0.0000000018,
            api_costs=DEFAULT_GCP_API_COSTS,
            source="gcp_static",
            refresh_interval_sec=refresh_interval_sec,
            auto_refresh=auto_refresh,
        )
    if provider == "azure":
        return StaticPricingProvider(
            cpu_cost_per_ms=0.0000011,
            memory_cost_per_mb_sec=0.0000000016,
            api_costs=DEFAULT_AZURE_API_COSTS,
            source="azure_static",
            refresh_interval_sec=refresh_interval_sec,
            auto_refresh=auto_refresh,
        )
    return StaticPricingProvider(
        cpu_cost_per_ms=0.0000000166667,
        memory_cost_per_mb_sec=0.0,
        api_costs=DEFAULT_AWS_API_COSTS,
        source="aws_static",
        refresh_interval_sec=refresh_interval_sec,
        auto_refresh=auto_refresh,
    )


def get_cost_model(
    provider: str = "aws",
    dynamic_pricing: bool = True,
    refresh_interval_sec: int = 86400,
    auto_refresh: bool = False,
) -> CostModel:
    """
    Get cost model for a cloud provider.

    Dynamic pricing is **on by default**: the AWS Pricing API is queried
    on first use (if boto3 is installed) and the result is cached for
    ``refresh_interval_sec`` seconds.  If the API call fails, static
    fallback rates are used transparently.

    Args:
        provider: Cloud provider name ('aws').
                  GCP and Azure support is coming soon.
        dynamic_pricing: Pull live rates from the AWS Pricing API.
                         Defaults to True; set to False for offline / test use.
        refresh_interval_sec: Rate-cache TTL in seconds (default 24 h).
        auto_refresh: Start a background thread that refreshes rates daily.
    """
    provider_norm = provider.lower()
    pricing_provider = get_pricing_provider(
        provider=provider_norm,
        dynamic=dynamic_pricing,
        refresh_interval_sec=refresh_interval_sec,
        auto_refresh=auto_refresh,
    )
    if provider_norm == "gcp":
        return GCPCostModel(pricing_provider=pricing_provider)
    if provider_norm == "azure":
        return AzureCostModel(pricing_provider=pricing_provider)
    return AWSCostModel(pricing_provider=pricing_provider)
