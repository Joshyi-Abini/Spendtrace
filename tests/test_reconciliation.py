import json
import time

from cost_attribution.core.tracker import CostRecord
from cost_attribution.core.models import AWSCostModel
from cost_attribution.reconciliation.aws import AWSBillingReconciler
from cost_attribution.storage.sqlite import SQLiteStorage


class _FakeCEClient:
    def get_cost_and_usage(self, **kwargs):
        group_by = kwargs.get("GroupBy")
        if group_by:
            return {
                "ResultsByTime": [
                    {
                        "Groups": [
                            {
                                "Keys": ["feature$search"],
                                "Metrics": {"UnblendedCost": {"Amount": "4.0", "Unit": "USD"}},
                            },
                            {
                                "Keys": ["feature$ai_recommendations"],
                                "Metrics": {"UnblendedCost": {"Amount": "8.0", "Unit": "USD"}},
                            },
                        ]
                    }
                ]
            }
        return {
            "ResultsByTime": [
                {
                    "Total": {"UnblendedCost": {"Amount": "12.0", "Unit": "USD"}}
                }
            ]
        }


def _mk_record(tx_id, ts, feature, total):
    return CostRecord(
        tx_id=tx_id,
        timestamp=ts,
        function_name="fn",
        feature=feature,
        duration_ms=1.0,
        cpu_time_ms=1.0,
        memory_mb=1.0,
        api_calls={"svc": {"count": 1}},
        cpu_cost=0.0,
        memory_cost=0.0,
        api_cost=total,
        api_cost_breakdown={
            "svc": {
                "count": 1,
                "count_unit": "request",
                "count_unit_rate_usd": total,
                "count_cost_usd": total,
                "input_tokens": 0,
                "input_unit_rate_usd": 0,
                "input_cost_usd": 0,
                "output_tokens": 0,
                "output_unit_rate_usd": 0,
                "output_cost_usd": 0,
                "total_cost_usd": total,
            }
        },
        total_cost=total,
        tags={},
    )


def test_reconcile_global_and_feature_factors(tmp_path):
    db = tmp_path / "reconcile.db"
    storage = SQLiteStorage(str(db))

    now = time.time()
    storage.store_batch([
        _mk_record("1", now, "search", 2.0),
        _mk_record("2", now, "ai_recommendations", 4.0),
    ])

    reconciler = AWSBillingReconciler(storage_backend=storage, ce_client=_FakeCEClient())

    report = reconciler.reconcile(
        start_date="2000-01-01",
        end_date="2100-01-01",
        tag_key="feature",
    )

    assert report.modeled_total_cost == 6.0
    assert report.actual_total_cost == 12.0
    assert abs(report.global_calibration_factor - 2.0) < 1e-12
    assert abs(report.feature_factors["search"] - 2.0) < 1e-12
    assert abs(report.feature_factors["ai_recommendations"] - 2.0) < 1e-12
    assert report.modeled_api_cost_by_service["svc"] == 6.0
    assert report.service_unit_metadata["svc"]["count_unit"] == "request"


def test_reconcile_save_report(tmp_path):
    storage = SQLiteStorage(str(tmp_path / "db.db"))
    reconciler = AWSBillingReconciler(storage_backend=storage, ce_client=_FakeCEClient())
    report = reconciler.reconcile("2000-01-01", "2100-01-01")

    output = tmp_path / "out" / "report.json"
    path = reconciler.save_report(report, str(output))

    data = json.loads(output.read_text(encoding="utf-8"))
    assert path.endswith("report.json")
    assert data["provider"] == "aws"


def test_apply_global_factor_to_model(tmp_path):
    storage = SQLiteStorage(str(tmp_path / "db2.db"))
    reconciler = AWSBillingReconciler(storage_backend=storage, ce_client=_FakeCEClient())
    report = reconciler.reconcile("2000-01-01", "2100-01-01")

    model = AWSCostModel()
    assert model.calibration_factor == 1.0
    AWSBillingReconciler.apply_global_factor_to_model(report, model)
    assert abs(model.calibration_factor - report.global_calibration_factor) < 1e-12
