"""Demo upstream with configurable response delay, a health endpoint used by
the gateway's pre-switch probe, and a settable business result value on /work
so side-by-side comparisons can demonstrate both match and mismatch."""
from __future__ import annotations

import asyncio
import os
import time
from typing import Any

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

NAME = os.environ.get("UPSTREAM_NAME", "upstream")
DEFAULT_DELAY_MS = int(os.environ.get("DEFAULT_DELAY_MS", "0"))
MAX_DELAY_MS = 60_000

app = FastAPI(title=f"upstream-{NAME}")
_state = {
    "health_fail": os.environ.get("HEALTH_FAIL", "0") == "1",
    # Business result value returned by /work; compared by the gateway's
    # shadow records (envelope fields upstream/ts/delay_ms are ignored there).
    "result": os.environ.get("WORK_RESULT", "ok"),
}


@app.get("/health")
def health():
    if _state["health_fail"]:
        return JSONResponse(
            status_code=503, content={"status": "failing", "upstream": NAME}
        )
    return {"status": "ok", "upstream": NAME}


class HealthMode(BaseModel):
    fail: bool


@app.post("/admin/health_mode")
def set_health_mode(mode: HealthMode):
    """Demo hook: make /health fail or recover at runtime."""
    _state["health_fail"] = mode.fail
    return {"upstream": NAME, "health_fail": _state["health_fail"]}


class WorkResult(BaseModel):
    result: Any  # any JSON value


@app.get("/admin/work_result")
def get_work_result():
    return {"upstream": NAME, "result": _state["result"]}


@app.post("/admin/work_result")
def set_work_result(cfg: WorkResult):
    """Demo hook: set the business result value returned by /work, e.g. to
    make a shadow candidate disagree with the primary on purpose."""
    _state["result"] = cfg.result
    return {"upstream": NAME, "result": _state["result"]}


@app.api_route("/work", methods=["GET", "POST"])
async def work(delay_ms: int | None = Query(default=None)):
    delay = DEFAULT_DELAY_MS if delay_ms is None else max(0, min(delay_ms, MAX_DELAY_MS))
    await asyncio.sleep(delay / 1000)
    return {
        "upstream": NAME,
        "delay_ms": delay,
        "ts": time.time(),
        "result": _state["result"],
    }


@app.get("/")
def root():
    return {"upstream": NAME, "default_delay_ms": DEFAULT_DELAY_MS}
