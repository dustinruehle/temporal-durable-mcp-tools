"""HTTP front door for the agent platform: start a run, read its state.

Run: uv run python -m agent_platform.app   (port 9300)
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException

from agent_platform.graph import NODES, build_graph, mcp_client, model_mode

load_dotenv()
app = FastAPI(title="agent-platform")
RUNS: dict[str, dict] = {}
_tool = None


async def get_tool():
    global _tool
    if _tool is None:
        tools = await mcp_client().get_tools()
        _tool = next(t for t in tools if t.name == "fulfill_request")
    return _tool


GRAPH = build_graph(get_tool)


async def _execute(run: dict) -> None:
    config = {"configurable": {"thread_id": run["run_id"]}}
    order = list(NODES)
    run["nodes"][order[0]] = "running"
    try:
        async for update in GRAPH.astream({"request_id": run["request_id"]}, config, stream_mode="updates"):
            for node, values in update.items():
                values = values or {}
                run.update({k: v for k, v in values.items() if k in ("decision", "reason", "tool_result", "tool_error")})
                run["nodes"][node] = "failed" if values.get("tool_error") else "done"
                i = order.index(node)
                if i + 1 < len(order):
                    run["nodes"][order[i + 1]] = "running"
        run["status"] = "failed" if run.get("tool_error") else "completed"
    except Exception as e:  # noqa: BLE001
        run["status"], run["error"] = "failed", f"{e.__class__.__name__}: {e}"
        for n, s in run["nodes"].items():
            if s == "running":
                run["nodes"][n] = "failed"
    run["finished_at"] = time.time()


@app.post("/runs")
async def start_run(body: dict):
    case_id = str(body.get("case_id") or "approve")
    request_id = str(body.get("request_id") or case_id)
    if case_id not in ("approve", "refer", "deny"):
        raise HTTPException(400, "case_id must be approve, refer or deny")
    run = {"run_id": uuid.uuid4().hex[:8], "request_id": request_id, "case_id": case_id, "status": "running",
           "nodes": {n: "pending" for n in NODES}, "decision": None, "reason": None, "tool_result": None,
           "tool_error": None, "error": None, "started_at": time.time(), "finished_at": None}
    RUNS[run["run_id"]] = run
    asyncio.create_task(_execute(run))
    return {"run_id": run["run_id"], "request_id": request_id}


@app.get("/runs/{run_id}")
async def get_run(run_id: str):
    if run_id not in RUNS:
        raise HTTPException(404, "unknown run")
    return RUNS[run_id]


@app.get("/runs")
async def list_runs(limit: int = 50):
    return {"runs": sorted(RUNS.values(), key=lambda r: r["started_at"], reverse=True)[:limit]}


@app.post("/admin/reset")
async def reset():
    RUNS.clear()
    return {"ok": True}


@app.get("/healthz")
async def healthz():
    return {"ok": True, "model_mode": model_mode()}


def main() -> None:
    port = int(os.getenv("PLATFORM_PORT", "9300"))
    print(f"agent-platform (plain LangGraph, model mode={model_mode()}) on http://localhost:{port}; "
          f"tools from {os.getenv('MCP_URL', 'http://localhost:9200/mcp')}", flush=True)
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")


if __name__ == "__main__":
    main()
