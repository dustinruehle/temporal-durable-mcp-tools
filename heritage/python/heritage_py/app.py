"""Mock heritage services: records, policy and notify.

These stand in for existing systems of record that a tool has to call. They behave like
live RPC services: they answer directly, they have no queue, and the policy service
rejects work above a small concurrency limit with a 429. Fault switches let the demo
make any of them slow or fail for the next few calls. See docs/CONTRACTS.md.

Run: uv run python -m heritage_py.app   (port 9101)
"""
from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

ROOT = Path(__file__).resolve().parents[3]
CASES = ROOT / "datasets" / "cases"
POLICY_LIMIT = int(os.getenv("POLICY_MAX_IN_FLIGHT", "3"))
LATENCY_S = {"records": 0.3, "policy": 1.0, "notify": 0.1}
SERVICES = ("records", "policy", "notify")


def _new_stats() -> dict:
    return {"calls": 0, "ok": 0, "errors": 0, "rejected_429": 0, "in_flight": 0, "max_in_flight": 0}


STATS = {s: _new_stats() for s in SERVICES}
FAULTS: dict[str, dict] = {}


def case_for(request_id: str) -> str:
    """Request ids are `<case>` or `<case>-<suffix>`; `burst-...` uses the approve case."""
    prefix = request_id.split("-", 1)[0]
    return "approve" if prefix == "burst" else prefix


def load_case(request_id: str) -> dict | None:
    case_id = case_for(request_id)
    path = CASES / f"{case_id}.json"
    if not path.is_file() or "/" in case_id or ".." in case_id:
        return None
    case = json.loads(path.read_text())
    case["request_id"] = request_id
    return case


@asynccontextmanager
async def call(service: str):
    """Count the call, apply faults and latency, and keep in-flight stats."""
    st = STATS[service]
    st["calls"] += 1
    st["in_flight"] += 1
    st["max_in_flight"] = max(st["max_in_flight"], st["in_flight"])
    try:
        fault = FAULTS.get(service) or {}
        delay = LATENCY_S[service]
        if fault.get("mode") == "slow":
            delay += fault.get("delay_ms", 0) / 1000
            fault["count"] -= 1
            if fault["count"] <= 0:
                FAULTS.pop(service, None)
        await asyncio.sleep(delay)
        if fault.get("mode") == "fail_next" and fault.get("count", 0) > 0:
            fault["count"] -= 1
            if fault["count"] == 0:
                FAULTS.pop(service, None)
            raise ServiceError(503, f"{service} service unavailable")
        yield
    finally:
        st["in_flight"] -= 1


class ServiceError(Exception):
    def __init__(self, status: int, message: str):
        self.status, self.message = status, message


app = FastAPI(title="heritage-py")


@app.exception_handler(ServiceError)
async def _service_error(_: Request, exc: ServiceError):
    return JSONResponse({"error": exc.message}, status_code=exc.status)


def _ok(service: str, body: dict) -> dict:
    STATS[service]["ok"] += 1
    return body


def _err(service: str, status: int, message: str) -> JSONResponse:
    STATS[service]["errors"] += 1
    return JSONResponse({"error": message}, status_code=status)


@app.post("/records/fetch")
async def records_fetch(body: dict):
    try:
        async with call("records"):
            case = load_case(str(body.get("request_id", "")))
            if case is None:
                return _err("records", 404, "unknown request")
            return _ok("records", {
                "request_id": case["request_id"], "requester": case["requester"], "amount": case["amount"],
                "criteria_met": case["criteria_met"], "criteria_total": len(case["criteria"]),
                "documents": case["documents"], "summary": case["summary"],
            })
    except ServiceError as e:
        return _err("records", e.status, e.message)


@app.post("/policy/check")
async def policy_check(body: dict):
    st = STATS["policy"]
    if st["in_flight"] >= POLICY_LIMIT:
        # A live service with no queue in front of it: too much concurrent work is turned away.
        st["calls"] += 1
        st["rejected_429"] += 1
        st["errors"] += 1
        return JSONResponse({"error": f"too many requests: policy service allows {POLICY_LIMIT} in flight"}, status_code=429)
    try:
        async with call("policy"):
            case = load_case(str(body.get("request_id", "")))
            if case is None:
                return _err("policy", 404, "unknown request")
            met, total = len(case["criteria_met"]), len(case["criteria"])
            return _ok("policy", {"request_id": case["request_id"], "eligible": met == total,
                                  "criteria_met": met, "criteria_total": total})
    except ServiceError as e:
        return _err("policy", e.status, e.message)


@app.post("/notify/send")
async def notify_send(body: dict):
    try:
        async with call("notify"):
            return _ok("notify", {"request_id": body.get("request_id"), "sent": True})
    except ServiceError as e:
        return _err("notify", e.status, e.message)


@app.get("/admin/stats")
async def admin_stats():
    return STATS


@app.post("/admin/faults")
async def admin_faults(body: dict):
    service, mode = body.get("service"), body.get("mode")
    if service not in SERVICES or mode not in ("fail_next", "slow", "clear"):
        return JSONResponse({"error": "service must be records|policy|notify; mode fail_next|slow|clear"}, status_code=400)
    if mode == "clear":
        FAULTS.pop(service, None)
    else:
        default_count = 1 if mode == "fail_next" else 1_000_000  # slow: every call until cleared, unless count is set
        FAULTS[service] = {"mode": mode, "count": int(body.get("count", default_count)), "delay_ms": int(body.get("delay_ms", 0))}
    return {"ok": True, "faults": FAULTS}


@app.get("/admin/faults")
async def admin_faults_get():
    return FAULTS


@app.post("/admin/reset")
async def admin_reset():
    FAULTS.clear()
    for s in SERVICES:
        STATS[s] = _new_stats()
    return {"ok": True}


@app.get("/healthz")
async def healthz():
    return {"ok": True}


def main() -> None:
    port = int(os.getenv("HERITAGE_PY_PORT", "9101"))
    print(f"heritage-py: records, policy (max {POLICY_LIMIT} in flight), notify on http://localhost:{port}", flush=True)
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")


if __name__ == "__main__":
    main()
