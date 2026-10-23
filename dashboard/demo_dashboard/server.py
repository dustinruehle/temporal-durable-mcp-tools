"""Demo control panel: starts agent runs, flips the tool mode, injects heritage faults,
sends approvals, and shows every layer at once.

It reads each layer from its own source of truth: the platform's run state, the MCP
server's stats, the heritage services' stats, and Temporal (describe and the `progress`
Query) for the durable tool calls.

Run: uv run python -m demo_dashboard.server   (port 8095)
"""
from __future__ import annotations

import asyncio
import os
import uuid
from pathlib import Path

import httpx
import uvicorn
from durable_tools.config import connect, target
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from temporalio.client import Client, WorkflowUpdateFailedError
from temporalio.service import RPCError

HERE = Path(__file__).resolve().parent
PY = os.getenv("HERITAGE_PY_URL", "http://localhost:9101")
JAVA = os.getenv("HERITAGE_JAVA_URL", "http://localhost:9103")
MCP_ADMIN = os.getenv("MCP_ADMIN_URL", "http://localhost:9200")
PLATFORM = os.getenv("PLATFORM_URL", "http://localhost:9300")
UI = os.getenv("TEMPORAL_UI_URL", "http://localhost:8233")

app = FastAPI(title="demo-dashboard")
_client: Client | None = None
MODES: dict[str, str] = {}  # request_id -> tool mode at the time of the call


async def temporal() -> Client:
    global _client
    if _client is None:
        _client = await connect()
    return _client


async def _get(http: httpx.AsyncClient, url: str):
    try:
        r = await http.get(url, timeout=2)
        return r.json() if r.status_code == 200 else {"error": r.status_code}
    except httpx.HTTPError:
        return None


async def _post(url: str, body: dict | None = None):
    async with httpx.AsyncClient(timeout=5) as http:
        try:
            r = await http.post(url, json=body or {})
        except httpx.HTTPError as e:
            raise HTTPException(502, f"{url}: {e.__class__.__name__}") from e
    if r.status_code >= 400:
        raise HTTPException(r.status_code, r.text)
    return r.json()


async def _workflow(rid: str) -> dict | None:
    """Temporal's view of one tool call: status, pending Activity attempts, and the progress Query."""
    try:
        client = await temporal()
        h = client.get_workflow_handle(f"fulfill-{rid}")
        d = await asyncio.wait_for(h.describe(), timeout=2)
    except (TimeoutError, RPCError, OSError):
        return None
    out = {"workflow_id": f"fulfill-{rid}", "run_id": d.run_id, "status": d.status.name if d.status else None,
           "pending": [], "progress": None, "worker_online": None,
           "ui": f"{UI}/namespaces/{target()[1]}/workflows/fulfill-{rid}/{d.run_id}/history"}
    raw = d.raw_description
    for pa in raw.pending_activities:
        out["pending"].append({"activity": pa.activity_type.name, "attempt": pa.attempt,
                               "last_failure": pa.last_failure.message if pa.HasField("last_failure") else ""})
    if out["status"] == "RUNNING":
        try:
            out["progress"] = await asyncio.wait_for(h.query("progress"), timeout=1.5)
            out["worker_online"] = True
        except (TimeoutError, RPCError):
            out["worker_online"] = False  # no Worker to answer: the tool Worker is down
    else:
        try:
            out["progress"] = await asyncio.wait_for(h.query("progress"), timeout=1.5)
        except (TimeoutError, RPCError):
            pass
    return out


@app.get("/")
async def index():
    return FileResponse(HERE / "index.html")


@app.get("/api/state")
async def state():
    async with httpx.AsyncClient() as http:
        mode, mcp, py, java, runs, py_faults = await asyncio.gather(
            _get(http, f"{MCP_ADMIN}/admin/mode"), _get(http, f"{MCP_ADMIN}/admin/stats"),
            _get(http, f"{PY}/admin/stats"), _get(http, f"{JAVA}/admin/stats"),
            _get(http, f"{PLATFORM}/runs?limit=200"), _get(http, f"{PY}/admin/faults"))
    all_runs = (runs or {}).get("runs", [])
    single = [r for r in all_runs if not r["request_id"].startswith("burst-")][:6]
    burst = [r for r in all_runs if r["request_id"].startswith("burst-")]
    wfs = await asyncio.gather(*[_workflow(r["request_id"]) for r in single])
    for r, w in zip(single, wfs):
        r["temporal"] = w
        r["mode"] = MODES.get(r["request_id"])
    burst_summary = None
    if burst:
        latest = burst[0]["request_id"].rsplit("-", 1)[0]
        batch = [r for r in burst if r["request_id"].startswith(latest + "-")]
        ok = sum(1 for r in batch if r["status"] == "completed" and (r.get("tool_result") or {}).get("outcome") == "fulfilled")
        burst_summary = {"batch": latest, "mode": MODES.get(batch[0]["request_id"]), "total": len(batch), "fulfilled": ok,
                         "failed": sum(1 for r in batch if r["status"] == "failed"),
                         "running": sum(1 for r in batch if r["status"] == "running"),
                         "errors": sorted({(r.get("tool_error") or "")[:90] for r in batch if r.get("tool_error")})[:3]}
    return {"mode": (mode or {}).get("mode"), "mcp": mcp, "heritage_py": py, "heritage_java": java,
            "py_faults": py_faults, "runs": single, "burst": burst_summary,
            "online": {"mcp": mode is not None, "heritage_py": py is not None, "heritage_java": java is not None,
                       "platform": runs is not None}}


@app.post("/api/mode")
async def set_mode(body: dict):
    return await _post(f"{MCP_ADMIN}/admin/mode", {"mode": body.get("mode")})


@app.post("/api/run")
async def run(body: dict):
    case_id = body.get("case_id", "approve")
    rid = body.get("request_id") or f"{case_id}-{uuid.uuid4().hex[:4]}"
    async with httpx.AsyncClient() as http:
        mode = (await _get(http, f"{MCP_ADMIN}/admin/mode") or {}).get("mode")
    MODES[rid] = mode
    return await _post(f"{PLATFORM}/runs", {"case_id": case_id, "request_id": rid})


@app.post("/api/burst")
async def burst(body: dict):
    n = int(body.get("n", 30))
    batch = f"burst-{uuid.uuid4().hex[:4]}"
    async with httpx.AsyncClient() as http:
        mode = (await _get(http, f"{MCP_ADMIN}/admin/mode") or {}).get("mode")
    rids = [f"{batch}-{i:02d}" for i in range(1, n + 1)]
    for rid in rids:
        MODES[rid] = mode
    await asyncio.gather(*[_post(f"{PLATFORM}/runs", {"case_id": "approve", "request_id": rid}) for rid in rids])
    return {"batch": batch, "n": n, "mode": mode}


@app.post("/api/approve")
async def approve(body: dict):
    client = await temporal()
    h = client.get_workflow_handle(f"fulfill-{body['request_id']}")
    try:
        return await h.execute_update("decide_approval", {"approver": body.get("approver", "reviewer"),
                                                          "decision": body.get("decision", "approve"),
                                                          "note": body.get("note", "")})
    except WorkflowUpdateFailedError as e:
        raise HTTPException(409, str(e.cause or e)) from e


@app.post("/api/fault")
async def fault(body: dict):
    base = JAVA if body.get("target") == "java" else PY
    return await _post(f"{base}/admin/faults", {k: body[k] for k in ("service", "mode", "count", "delay_ms") if k in body})


@app.post("/api/reset")
async def reset():
    results = {}
    for name, url in (("heritage_py", f"{PY}/admin/reset"), ("heritage_java", f"{JAVA}/admin/reset"),
                      ("platform", f"{PLATFORM}/admin/reset"), ("mcp", f"{MCP_ADMIN}/admin/reset")):
        try:
            results[name] = await _post(url)
        except HTTPException as e:
            results[name] = {"error": e.detail}
    MODES.clear()
    return results


def main() -> None:
    port = int(os.getenv("DASHBOARD_PORT", "8095"))
    print(f"dashboard on http://localhost:{port}", flush=True)
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")


if __name__ == "__main__":
    main()
