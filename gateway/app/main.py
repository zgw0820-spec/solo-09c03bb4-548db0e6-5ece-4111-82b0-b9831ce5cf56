"""Edge gateway: proxies requests to the currently routed upstream and
exposes the operations API + web console for on-duty target switching.

Shadow (side-by-side) comparison: while enabled, every newly admitted
GET /work request is additionally mirrored to the configured candidate
upstream. The client only ever waits for the primary target; the candidate
call runs as a background task and its outcome is recorded against the
round/targets bound at admission time.
"""
from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from . import db

STATIC_DIR = Path(__file__).parent / "static"


def parse_upstreams() -> dict[str, str]:
    raw = os.environ.get(
        "UPSTREAMS", "alpha=http://127.0.0.1:9001;beta=http://127.0.0.1:9002"
    )
    out: dict[str, str] = {}
    for part in raw.split(";"):
        part = part.strip()
        if not part:
            continue
        name, _, url = part.partition("=")
        out[name.strip()] = url.strip().rstrip("/")
    return out


UPSTREAMS = parse_upstreams()
INITIAL_TARGET = os.environ.get("INITIAL_TARGET") or next(iter(UPSTREAMS))


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.boot_info = db.init_db(UPSTREAMS, INITIAL_TARGET)
    app.state.http = httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=3.0))
    app.state.shadow_tasks = set()
    yield
    # Cancel in-flight shadow mirrors so their records stay unfinished and are
    # reported as interrupted after a restart; never write late results here.
    tasks = [t for t in app.state.shadow_tasks if not t.done()]
    for t in tasks:
        t.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    await app.state.http.aclose()


app = FastAPI(title="edge-gateway", lifespan=lifespan)


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
def health():
    return {"ok": True}


@app.get("/api/state")
def state():
    s = db.get_state()
    s["boot"] = app.state.boot_info
    return s


async def shadow_mirror(client: httpx.AsyncClient, record_id: str, url: str,
                        path: str, params: dict) -> None:
    """Fire-and-forget mirror of an admitted request to the shadow candidate.
    The result is written to the record created at admission (same id), so a
    late result still lands on its original round and targets even if the
    route was switched or comparison was reopened meanwhile."""
    try:
        resp = await client.get(f"{url}{path}", params=params)
        db.shadow_side_done(record_id, "candidate", resp.status_code,
                            resp.text[:2000], None)
    except asyncio.CancelledError:
        raise  # shutting down: leave the record unfinished -> interrupted on boot
    except httpx.HTTPError as exc:
        db.shadow_side_done(record_id, "candidate", None, None, str(exc))


class CallRequest(BaseModel):
    delay_ms: int | None = Field(default=None, ge=0, le=60_000)
    path: str = "/work"


@app.post("/api/call")
async def call(req: CallRequest):
    """Admit a request and proxy it to the upstream that is current *at
    admission time*. The full upstream response is fetched from that original
    target even if a switch commits while the request is in flight. When
    shadow comparison is enabled, the request is also mirrored to the
    candidate in the background; the client response never waits for it."""
    path = req.path if req.path.startswith("/") else "/work"
    if "://" in path or ".." in path:
        return JSONResponse(status_code=400, content={"ok": False, "error": "bad_path"})
    admission = db.admit(path)
    shadow = admission["shadow"]
    target_url = db.upstream_url(admission["target"])
    params = {} if req.delay_ms is None else {"delay_ms": req.delay_ms}
    if shadow is not None:
        task = asyncio.create_task(
            shadow_mirror(app.state.http, admission["id"],
                          shadow["candidate_url"], path, params)
        )
        app.state.shadow_tasks.add(task)
        task.add_done_callback(app.state.shadow_tasks.discard)
    started = time.monotonic()
    try:
        resp = await app.state.http.get(f"{target_url}{path}", params=params)
        if shadow is not None:
            db.shadow_side_done(admission["id"], "primary",
                                resp.status_code, resp.text[:2000], None)
        try:
            body = resp.json()
        except ValueError:
            body = {"text": resp.text[:500]}
        return {
            "ok": resp.status_code == 200,
            "status": resp.status_code,
            "request_id": admission["id"],
            "target": admission["target"],
            "route_version": admission["version"],
            "duration_ms": int((time.monotonic() - started) * 1000),
            "upstream": body,
            "shadow": (
                {"round": shadow["round"], "candidate": shadow["candidate"]}
                if shadow else None
            ),
        }
    except httpx.HTTPError as exc:
        if shadow is not None:
            db.shadow_side_done(admission["id"], "primary", None, None, str(exc))
        return JSONResponse(
            status_code=502,
            content={
                "ok": False,
                "error": f"upstream_error: {exc}",
                "request_id": admission["id"],
                "target": admission["target"],
                "route_version": admission["version"],
                "shadow": (
                    {"round": shadow["round"], "candidate": shadow["candidate"]}
                    if shadow else None
                ),
            },
        )
    finally:
        db.finish(admission["id"])


class ProbeRequest(BaseModel):
    target: str
    base_version: int


@app.post("/api/switch/probe")
async def probe(req: ProbeRequest):
    """Probe a candidate target. Never mutates the route: while probing, new
    requests keep flowing to the old target, and a failed probe leaves the
    original route untouched."""
    route = db.get_route()
    url = db.upstream_url(req.target)
    if url is None:
        return JSONResponse(
            status_code=404, content={"ok": False, "error": "unknown_target"}
        )
    if req.target == route["current_target"]:
        return {"ok": False, "error": "target_is_current", "current_version": route["version"]}
    ok, detail = False, {}
    try:
        health = await app.state.http.get(f"{url}/health", timeout=2.5)
        work = await app.state.http.get(
            f"{url}/work", params={"delay_ms": 0}, timeout=5.0
        )
        ok = health.status_code == 200 and work.status_code == 200
        detail = {"health_status": health.status_code, "work_status": work.status_code}
    except httpx.HTTPError as exc:
        detail = {"error": str(exc)}
    rec = db.record_probe(req.target, req.base_version, ok, detail)
    return {
        "ok": ok,
        "probe_id": rec["id"],
        "target": req.target,
        "base_version": req.base_version,
        "current_version": route["version"],
        "version_matches": route["version"] == req.base_version,
        "detail": detail,
    }


class CommitRequest(BaseModel):
    target: str
    base_version: int
    probe_id: str


@app.post("/api/switch/commit")
def commit(req: CommitRequest):
    """Confirm a switch. Version-guarded: once this commits, newly admitted
    requests immediately go to the new target without waiting for old
    in-flight requests; a stale commit is rejected with 409."""
    res = db.commit_switch(req.target, req.base_version, req.probe_id)
    if not res["ok"]:
        code = 409 if res["error"] == "version_conflict" else 400
        return JSONResponse(status_code=code, content=res)
    return res


class ShadowEnableRequest(BaseModel):
    candidate: str


@app.post("/api/shadow/enable")
def shadow_enable(req: ShadowEnableRequest):
    """Enable side-by-side comparison against a candidate upstream. Every
    enable (including re-enabling the same candidate) starts a new comparison
    round; requests admitted afterwards bind that round. Rejected when the
    candidate is unknown or already the current target."""
    route = db.get_route()
    if db.upstream_url(req.candidate) is None:
        return JSONResponse(
            status_code=404, content={"ok": False, "error": "unknown_target"}
        )
    if req.candidate == route["current_target"]:
        return JSONResponse(
            status_code=400, content={"ok": False, "error": "candidate_is_current"}
        )
    cfg = db.set_shadow(True, req.candidate)
    return {"ok": True, "shadow": cfg}


@app.post("/api/shadow/disable")
def shadow_disable():
    """Stop mirroring new requests. Records already admitted keep their round
    and still absorb late candidate results."""
    cfg = db.set_shadow(False)
    return {"ok": True, "shadow": cfg}
