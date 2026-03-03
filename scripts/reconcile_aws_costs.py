"""Run AWS billing reconciliation against modeled costs."""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cost_attribution.reconciliation.aws import AWSBillingReconciler
from cost_attribution.storage.sqlite import SQLiteStorage


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="AWS cost reconciliation")
    parser.add_argument("--db", default="cost_data.db", help="SQLite db path")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD (exclusive)")
    parser.add_argument("--tag-key", default=None, help="AWS cost allocation tag key for feature-level reconciliation")
    parser.add_argument("--out", default="artifacts/calibration/aws_reconciliation.json", help="Output report path")
    args = parser.parse_args(argv)

    storage = SQLiteStorage(args.db)
    reconciler = AWSBillingReconciler(storage_backend=storage)

    report = reconciler.reconcile(start_date=args.start, end_date=args.end, tag_key=args.tag_key)
    out_path = reconciler.save_report(report, args.out)

    print(json.dumps(report.to_dict(), indent=2))
    print(f"saved_report={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
