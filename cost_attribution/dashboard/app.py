"""Lightweight dashboard server for cost attribution data."""

import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

from ..storage.sqlite import SQLiteStorage
from ..utils.logging import maybe_configure_from_env

app = FastAPI(title="Cost Attribution Dashboard", version="1.1.0")
maybe_configure_from_env()
_DB_ROOT_RAW = os.getenv("COST_ATTRIBUTION_DB_ROOT", "").strip()
_DB_ROOT = Path(_DB_ROOT_RAW).resolve() if _DB_ROOT_RAW else None
_ALLOWED_DB_SUFFIXES = {".db", ".sqlite", ".sqlite3"}


def _safe_db_path(db_path: str) -> str:
    raw = (db_path or "cost_data.db").strip()
    candidate = Path(raw)
    if _DB_ROOT is not None and not candidate.is_absolute():
        candidate = (_DB_ROOT / candidate).resolve()
    else:
        candidate = candidate.resolve()

    if _DB_ROOT is not None:
        try:
            candidate.relative_to(_DB_ROOT)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"db_path must stay under {_DB_ROOT}") from exc

    if candidate.suffix.lower() not in _ALLOWED_DB_SUFFIXES:
        raise HTTPException(status_code=400, detail="db_path must use .db, .sqlite, or .sqlite3")

    return str(candidate)


@app.get("/", response_class=HTMLResponse)
def index(db_path: str = "cost_data.db"):
    storage = SQLiteStorage(_safe_db_path(db_path))
    feature_rows = storage.aggregate_by_feature()
    total = storage.get_total_cost()
    service_rows = storage.aggregate_api_services(tx_limit=1000, service_limit=15)

    rows_html = "".join(
        f"<tr><td>{r['feature']}</td><td>{r['transaction_count']}</td><td>${r['total_cost']:.6f}</td></tr>"
        for r in feature_rows
    )
    service_rows_html = "".join(
        f"<tr><td>{row['service']}</td><td>{row['count']:.0f} {row['count_unit']}</td>"
        f"<td>{row['input_tokens']:.0f}</td><td>{row['output_tokens']:.0f}</td>"
        f"<td>${row['total_cost_usd']:.6f}</td></tr>"
        for row in service_rows
    )

    return f"""
<!doctype html>
<html>
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>Cost Attribution Dashboard</title>
    <style>
      body {{ font-family: Georgia, serif; margin: 2rem; background: linear-gradient(120deg, #f4f5f2, #eef2ff); }}
      .card {{ background: white; border-radius: 10px; padding: 1rem 1.25rem; box-shadow: 0 6px 20px rgba(0,0,0,0.08); }}
      table {{ width: 100%; border-collapse: collapse; margin-top: 1rem; }}
      th, td {{ text-align: left; padding: 0.5rem; border-bottom: 1px solid #e5e7eb; }}
    </style>
  </head>
  <body>
    <div class=\"card\">
      <h1>Cost Attribution Dashboard</h1>
      <p>Total cost: <strong>${total:.6f}</strong></p>
      <h2>Cost by Feature</h2>
      <table>
        <thead><tr><th>Feature</th><th>Transactions</th><th>Total Cost</th></tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
      <h2>API Service Units (Recent 1000 tx)</h2>
      <table>
        <thead><tr><th>Service</th><th>Count + Unit</th><th>Input Tokens</th><th>Output Tokens</th><th>Total Cost</th></tr></thead>
        <tbody>{service_rows_html}</tbody>
      </table>
    </div>
  </body>
</html>
"""
