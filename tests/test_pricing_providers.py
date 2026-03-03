import time

from cost_attribution.core.models import (
    AWSCostModel,
    AWSDynamicPricingProvider,
    PricingProvider,
    PricingSnapshot,
    get_cost_model,
)


class _IncrementProvider(PricingProvider):
    def __init__(self):
        self.calls = 0
        super().__init__(refresh_interval_sec=60, auto_refresh=False)

    def _load_rates(self, previous):
        del previous
        self.calls += 1
        return PricingSnapshot(
            cpu_cost_per_ms=float(self.calls),
            memory_cost_per_mb_sec=0.0,
            api_costs={},
            source="test",
            updated_at=time.time(),
        )


def test_get_cost_model_dynamic_returns_aws_model_with_dynamic_provider():
    model = get_cost_model("aws", dynamic_pricing=True)
    assert isinstance(model, AWSCostModel)
    assert isinstance(model.pricing_provider, AWSDynamicPricingProvider)


def test_dynamic_provider_falls_back_without_boto3():
    provider = AWSDynamicPricingProvider(auto_refresh=False)
    snap = provider.get_rates(force_refresh=True)
    assert snap.source in {"aws_static_fallback", "aws_pricing_api"}
    assert snap.cpu_cost_per_ms > 0


def test_cost_model_uses_provider_rates():
    provider = _IncrementProvider()
    model = AWSCostModel(pricing_provider=provider)

    c1 = model.calculate_cpu_cost(1.0)
    c2 = model.calculate_cpu_cost(1.0)
    assert c1 == 1.0
    assert c2 == 1.0

    c3 = model.pricing_provider.get_rates(force_refresh=True).cpu_cost_per_ms
    assert c3 == 2.0


def test_api_cost_overrides_provider():
    provider = _IncrementProvider()
    model = AWSCostModel(pricing_provider=provider)
    model.api_costs["my_api"] = 0.123
    total = model.calculate_api_cost({"my_api": 2})
    assert abs(total - 0.246) < 1e-12


def test_llm_token_pricing_path():
    model = AWSCostModel()
    cost = model.calculate_api_cost(
        {
            "bedrock_claude_3_sonnet": {
                "count": 1,
                "input_tokens": 1000,
                "output_tokens": 100,
            }
        }
    )
    # count component + input token + output token
    expected = (
        model.api_costs["bedrock_claude_3_sonnet"] * 1
        + model.llm_input_costs["bedrock_claude_3_sonnet"] * 1000
        + model.llm_output_costs["bedrock_claude_3_sonnet"] * 100
    )
    assert abs(cost - expected) < 1e-12
