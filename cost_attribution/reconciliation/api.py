"""
Cost Attribution — High-level reconcile() API

One call to compare your modelled costs against the actual AWS bill.

Usage:
    from cost_attribution import reconcile

    report = reconcile(
        db_path="cost_data.db",
        start="2026-02-01",
        end="2026-03-01",
    )
    print(report.summary())
"""

from __future__ import annotations

from typing import Optional


def reconcile(
    db_path: str = "cost_attribution.db",
    start: str = "",
    end: str = "",
    tag_key: Optional[str] = None,
    ce_client=None,
) -> "ReconcileReport":
    """
    Compare modelled feature costs against your actual AWS Cost Explorer bill.

    Args:
        db_path:   Path to the SQLite database written by cost_attribution.
        start:     Start date (inclusive) in YYYY-MM-DD format.
        end:       End date (exclusive) in YYYY-MM-DD format.
        tag_key:   Optional AWS cost-allocation tag key for feature-level
                   reconciliation (requires activated tags in AWS Billing).
        ce_client: Optional pre-built boto3 Cost Explorer client (for testing).

    Returns:
        ReconcileReport with .summary() and .to_dict().

    Raises:
        RuntimeError if boto3 is not installed or AWS credentials are missing.
    """
    from ..storage.sqlite import SQLiteStorage
    from .aws import AWSBillingReconciler

    storage = SQLiteStorage(db_path=db_path)
    reconciler = AWSBillingReconciler(storage_backend=storage, ce_client=ce_client)
    report = reconciler.reconcile(
        start_date=start,
        end_date=end,
        tag_key=tag_key,
    )
    return ReconcileReport(report)


class ReconcileReport:
    """
    Thin wrapper around :class:`AWSBillingReconciler` output that provides a
    human-readable ``.summary()`` and preserves full ``.to_dict()`` access.
    """

    def __init__(self, inner):
        self._inner = inner

    # Delegate attribute access to the underlying dataclass
    def __getattr__(self, name):
        return getattr(self._inner, name)

    def to_dict(self) -> dict:
        return self._inner.to_dict()

    def summary(self) -> str:
        d = self._inner.to_dict()
        modelled = d.get("modeled_total_cost", 0.0)
        actual = d.get("actual_total_cost", 0.0)
        delta = actual - modelled
        factor = d.get("global_calibration_factor", 1.0)
        lines = [
            "=" * 60,
            f"  Reconciliation Report  {d.get('start_date')} → {d.get('end_date')}",
            "=" * 60,
            f"  Modelled total : ${modelled:>10.4f}",
            f"  Actual total   : ${actual:>10.4f}",
            f"  Delta          : ${delta:>+10.4f}",
            f"  Calibration Δ  : {factor:.4f}×",
            "",
        ]

        by_feature: dict = d.get("modeled_by_feature", {})
        if by_feature:
            lines.append("  Top features by modelled cost:")
            sorted_features = sorted(by_feature.items(), key=lambda x: x[1], reverse=True)[:10]
            col_w = max((len(f) for f, _ in sorted_features), default=10)
            for feature, cost in sorted_features:
                actual_cost = d.get("actual_by_feature", {}).get(feature, 0.0)
                gap = actual_cost - cost
                lines.append(f"    {feature:<{col_w}}  modelled ${cost:.4f}  actual ${actual_cost:.4f}  gap ${gap:+.4f}")

        lines.append("=" * 60)
        return "\n".join(lines)
