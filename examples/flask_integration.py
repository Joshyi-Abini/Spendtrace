"""Minimal Flask integration example."""

import sys
from pathlib import Path

from flask import Flask, request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cost_attribution import AsyncLogger, SQLiteStorage, add_api_call, cost_track, track_request
from cost_attribution.utils.async_logger import set_async_logger

app = Flask(__name__)
storage = SQLiteStorage("example_flask.db")
set_async_logger(AsyncLogger(storage_backend=storage))


@track_request(feature="api", endpoint="/search")
@cost_track(feature="api.search")
def _search_impl(query: str):
    add_api_call("api_gateway_request", 1)
    return {"query": query, "results": ["r1", "r2"]}


@app.get("/search")
def search():
    query = request.args.get("query", "")
    return _search_impl(query)


if __name__ == "__main__":
    app.run(debug=True)
