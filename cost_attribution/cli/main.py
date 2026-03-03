"""CLI for querying cost attribution data."""

import argparse
import json

from ..storage.sqlite import SQLiteStorage
from ..utils.logging import maybe_configure_from_env


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cost-attribution", description="Cost attribution CLI")
    parser.add_argument("--db", default="cost_data.db", help="SQLite database path")

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("total", help="Show total cost")

    by_feature = sub.add_parser("by-feature", help="Show aggregate by feature")
    by_feature.add_argument("--json", action="store_true")

    by_user = sub.add_parser("by-user", help="Show aggregate by user")
    by_user.add_argument("--limit", type=int, default=10)
    by_user.add_argument("--json", action="store_true")

    query = sub.add_parser("query", help="Query transactions")
    query.add_argument("--feature")
    query.add_argument("--user-id")
    query.add_argument("--limit", type=int, default=20)

    return parser


def _print_table(rows, headers):
    if not rows:
        print("No data")
        return
    print(" | ".join(headers))
    print("-" * (sum(len(h) for h in headers) + 3 * (len(headers) - 1)))
    for row in rows:
        print(" | ".join(str(row.get(h, "")) for h in headers))


def main(argv=None) -> int:
    maybe_configure_from_env()
    parser = _build_parser()
    args = parser.parse_args(argv)
    storage = SQLiteStorage(args.db)

    if args.command == "total":
        print(f"{storage.get_total_cost():.6f}")
        return 0

    if args.command == "by-feature":
        rows = storage.aggregate_by_feature()
        if args.json:
            print(json.dumps(rows, indent=2))
        else:
            _print_table(rows, ["feature", "transaction_count", "total_cost", "avg_cost"])
        return 0

    if args.command == "by-user":
        rows = storage.aggregate_by_user(limit=args.limit)
        if args.json:
            print(json.dumps(rows, indent=2))
        else:
            _print_table(rows, ["user_id", "transaction_count", "total_cost", "avg_cost"])
        return 0

    if args.command == "query":
        rows = storage.query(feature=args.feature, user_id=args.user_id, limit=args.limit)
        print(json.dumps(rows, indent=2))
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
