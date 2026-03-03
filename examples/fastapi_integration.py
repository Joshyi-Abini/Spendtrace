"""FastAPI integration example with request-context middleware."""

import sys
from pathlib import Path

from fastapi import FastAPI, Request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cost_attribution import AsyncLogger, SQLiteStorage, add_api_call, cost_track, end_request, start_request
from cost_attribution.utils.async_logger import set_async_logger

app = FastAPI(title="Cost Attribution Example")
storage = SQLiteStorage("example_fastapi.db")
set_async_logger(AsyncLogger(storage_backend=storage))


@app.middleware("http")
async def cost_context_middleware(request: Request, call_next):
    start_request(
        request_id=request.headers.get("X-Request-ID"),
        endpoint=request.url.path,
        feature="api",
    )
    try:
        return await call_next(request)
    finally:
        end_request()


@cost_track(feature="api.search")
def _search_impl(query: str):
    add_api_call("api_gateway_request", 1)
    return {"query": query, "results": ["r1", "r2"]}


@app.get("/search")
def search(query: str):
    return _search_impl(query)
