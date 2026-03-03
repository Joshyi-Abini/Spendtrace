from cost_attribution.api.app import app as api_app, api_services, health, metrics
from cost_attribution.cli.main import main as cli_main
from cost_attribution.dashboard.app import app as dashboard_app
from cost_attribution.storage.sqlite import SQLiteStorage
from cost_attribution.core.tracker import CostRecord
import time


def test_api_app_loads():
    assert api_app.title == "Cost Attribution API"


def test_dashboard_app_loads():
    assert dashboard_app.title == "Cost Attribution Dashboard"


def test_metrics_text_endpoint_content():
    health()
    output = metrics()
    assert "cost_api_requests_total" in output


def test_api_services_endpoint(tmp_path):
    db = tmp_path / "svc.db"
    storage = SQLiteStorage(str(db))
    storage.store(
        CostRecord(
            tx_id="svc1",
            timestamp=time.time(),
            function_name="fn",
            feature="api",
            duration_ms=1.0,
            cpu_time_ms=1.0,
            memory_mb=1.0,
            api_calls={"svc": {"count": 2}},
            cpu_cost=0.0,
            memory_cost=0.0,
            api_cost=2.0,
            api_cost_breakdown={
                "svc": {
                    "count": 2,
                    "count_unit": "request",
                    "count_unit_rate_usd": 1.0,
                    "count_cost_usd": 2.0,
                    "input_tokens": 0,
                    "input_unit_rate_usd": 0,
                    "input_cost_usd": 0,
                    "output_tokens": 0,
                    "output_unit_rate_usd": 0,
                    "output_cost_usd": 0,
                    "total_cost_usd": 2.0,
                }
            },
            total_cost=2.0,
            tags={},
        )
    )
    rows = api_services(db_path=str(db), tx_limit=100, service_limit=10)
    assert len(rows) == 1
    assert rows[0]["service"] == "svc"
    assert rows[0]["count_unit"] == "request"


def test_cli_total_command(tmp_path, capsys):
    db = tmp_path / "cli.db"
    rc = cli_main(["--db", str(db), "total"])
    captured = capsys.readouterr()
    assert rc == 0
    assert captured.out.strip() == "0.000000"
