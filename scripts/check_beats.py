"""Run every demo beat through the dashboard API, the same calls the buttons make, and check what each layer reports.

Usage (with the stack running, for example after `make up`):

    uv run python scripts/check_beats.py              # all beats
    uv run python scripts/check_beats.py b1 b3 b4     # some beats

Beats: b1 b2 b2k b3 b3r b3d b4d b4 b5 reattach dedup. Beat b2k stops and restarts the tool Worker started by
`make up` (it reads .run/tool-worker.pid). The temporal CLI calls use Temporal Cloud when TEMPORAL_API_KEY is set.
Each check resets the demo first, so don't run this while you present.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DASH = os.getenv("DASHBOARD_URL", "http://localhost:8095")
results: list[tuple[str, bool]] = []


def tflags() -> list[str]:
    if not os.getenv("TEMPORAL_API_KEY"):
        return []
    return ["--address", os.environ["TEMPORAL_ADDRESS"], "--namespace", os.environ["TEMPORAL_NAMESPACE"],
            "--api-key", os.environ["TEMPORAL_API_KEY"], "--tls"]


def api(path: str, body: dict | None = None, timeout: int = 30):
    req = urllib.request.Request(DASH + path, method="POST" if body is not None else "GET",
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read() or b"{}")


def state() -> dict:
    return api("/api/state")


def row(rid: str) -> dict | None:
    return next((r for r in state()["runs"] if r["request_id"] == rid), None)


def wait(rid: str, pred, secs: int = 60):
    t = time.time()
    r = None
    while time.time() - t < secs:
        r = row(rid)
        if r and pred(r):
            return r, round(time.time() - t, 1)
        time.sleep(0.5)
    return r, None


def done(r: dict) -> bool:
    return r["status"] in ("completed", "failed")


def steps(r: dict) -> dict:
    return (((r or {}).get("temporal") or {}).get("progress") or {}).get("steps", {})


def waiting(r: dict) -> bool:
    return bool((((r or {}).get("temporal") or {}).get("progress") or {}).get("awaiting_approval"))


def prep(mode: str) -> None:
    api("/api/reset", {})
    api("/api/mode", {"mode": mode})
    for s in ("records", "policy", "notify"):
        api("/api/fault", {"target": "py", "service": s, "mode": "clear"})
    for s in ("hold", "commit", "release"):
        api("/api/fault", {"target": "java", "service": s, "mode": "clear"})


def ledger() -> dict:
    return state()["heritage_java"]


def executions(rid: str) -> int:
    """How many Workflow executions exist for this request. Visibility (list) is eventually consistent, so a run
    that just closed can take a few seconds to show up, especially on Temporal Cloud: retry for up to 30 s."""
    n = 0
    for _ in range(15):
        out = subprocess.run(["temporal", "workflow", "list", "--query", f"WorkflowId='fulfill-{rid}'", "-o", "json"]
                             + tflags(), capture_output=True, text=True, check=False).stdout
        n = len(json.loads(out or "[]"))
        if n:
            return n
        time.sleep(2)
    return n


def record(name: str, ok: bool, detail: str) -> None:
    results.append((name, ok))
    print(f"{'PASS' if ok else 'FAIL'} {name} | {detail}", flush=True)


def b1():
    prep("direct")
    api("/api/fault", {"target": "java", "service": "commit", "mode": "fail_next", "count": 2})
    rid = api("/api/run", {"case_id": "approve"})["request_id"]
    r, s = wait(rid, done)
    lg = ledger()
    record("Beat 1 direct: a flaky ledger fails the whole tool call and leaves a hold open",
           r["status"] == "failed" and "commit" in (r.get("tool_error") or "") and lg["open_holds"] == 1,
           f"{rid} {r['status']} in {s}s, open holds {lg['open_holds']}")


def b2():
    prep("temporal")
    api("/api/fault", {"target": "java", "service": "commit", "mode": "fail_next", "count": 2})
    rid = api("/api/run", {"case_id": "approve"})["request_id"]
    retry_seen, t = False, time.time()
    while time.time() - t < 90:
        r = row(rid)
        if r and r.get("temporal") and any(p["attempt"] > 1 for p in r["temporal"]["pending"]):
            retry_seen = True
        if r and done(r):
            break
        time.sleep(0.4)
    lg = ledger()
    record("Beat 2 temporal: the same fault is retried and the tool call succeeds",
           r["status"] == "completed" and r["tool_result"]["outcome"] == "fulfilled" and lg["open_holds"] == 0
           and lg["commit"]["errors"] == 2,
           f"{rid} {(r['tool_result'] or {}).get('outcome')} in {round(time.time() - t, 1)}s, retry visible {retry_seen}, "
           f"commit errors {lg['commit']['errors']}, open holds {lg['open_holds']}")


def b2k():
    pidf = REPO / ".run" / "tool-worker.pid"
    if not pidf.exists():
        record("Beat 2k", False, "no .run/tool-worker.pid: start the stack with `make up`")
        return
    prep("temporal")
    api("/api/fault", {"target": "py", "service": "policy", "mode": "slow", "count": 1, "delay_ms": 8000})
    rid = api("/api/run", {"case_id": "approve"})["request_id"]
    wait(rid, lambda r: steps(r).get("policy") == "running", 30)
    time.sleep(1)
    os.killpg(int(pidf.read_text()), signal.SIGINT)  # what `make stop-tool-worker` does
    time.sleep(4)
    down = row(rid)
    offline = (down.get("temporal") or {}).get("worker_online") is False and down["temporal"]["status"] == "RUNNING"
    t0 = time.time()
    log = open(REPO / "logs" / "tool-worker.log", "a")  # noqa: SIM115 (the restarted Worker keeps writing to it)
    p = subprocess.Popen(["uv", "run", "python", "-m", "durable_tools.worker"], cwd=REPO, start_new_session=True,
                         stdout=log, stderr=subprocess.STDOUT)
    pidf.write_text(str(p.pid))
    r, _ = wait(rid, done, 120)
    calls = state()["heritage_py"]["records"]["calls"]
    record("Beat 2k temporal: the tool Worker stops mid-call; after restart the call finishes and records isn't re-run",
           offline and r["status"] == "completed" and r["tool_result"]["outcome"] == "fulfilled" and calls == 1,
           f"{rid} worker offline while down {offline}, finished {round(time.time() - t0, 1)}s after restart, "
           f"records calls {calls}")


def b3(decision: str = "approve"):
    prep("temporal")
    rid = api("/api/run", {"case_id": "refer"})["request_id"]
    r, _ = wait(rid, waiting, 45)
    paused = waiting(r) and r["status"] == "running"
    api("/api/approve", {"request_id": rid, "decision": decision})
    r, s = wait(rid, done, 45)
    lg = ledger()
    want = "fulfilled" if decision == "approve" else "rejected"
    record(f"Beat 3 temporal: a person decides in the middle of one tool call ({decision})",
           paused and (r["tool_result"] or {}).get("outcome") == want and lg["open_holds"] == 0,
           f"{rid} paused {paused}, then {(r['tool_result'] or {}).get('outcome')} in {s}s, open holds {lg['open_holds']}")


def b3d():
    prep("direct")
    rid = api("/api/run", {"case_id": "refer"})["request_id"]
    r, _ = wait(rid, done)
    lg = ledger()
    record("Beat 3d direct: the approval forces the tool to end early and leaves a hold open",
           r["status"] == "failed" and "approval required" in (r.get("tool_error") or "") and lg["open_holds"] == 1,
           f"{rid} open holds {lg['open_holds']}")


def burst(mode: str):
    prep(mode)
    api("/api/burst", {"n": 30})
    t, b = time.time(), None
    while time.time() - t < 180:
        b = state()["burst"]
        if b and b["running"] == 0 and b["total"] == 30:
            break
        time.sleep(1)
    return b, state()["heritage_py"]["policy"], round(time.time() - t, 1)


def b4d():
    b, pol, s = burst("direct")
    record("Beat 4d direct: a burst of 30 gets 429s from the policy service",
           b["failed"] > 0 and pol["rejected_429"] > 0 and pol["max_in_flight"] <= 3,
           f"{b['fulfilled']} fulfilled, {b['failed']} failed in {s}s, 429s {pol['rejected_429']}, "
           f"max in flight {pol['max_in_flight']}")


def b4():
    b, pol, s = burst("temporal")
    record("Beat 4 temporal: the burst is queued, all 30 fulfilled, never more than 3 in flight",
           b["fulfilled"] == 30 and pol["rejected_429"] == 0 and pol["max_in_flight"] <= 3,
           f"{b['fulfilled']} fulfilled in {s}s, 429s {pol['rejected_429']}, max in flight {pol['max_in_flight']}")


def b5():
    prep("temporal")
    api("/api/fault", {"target": "java", "service": "commit", "mode": "reject_next", "count": 1})
    rid = api("/api/run", {"case_id": "approve"})["request_id"]
    r, _ = wait(rid, done)
    lg, tr = ledger(), (r["tool_result"] or {})
    record("Beat 5 temporal: the ledger rejects the commit and the saga releases the hold",
           tr.get("outcome") == "failed" and tr["steps"]["hold"] == "compensated" and lg["open_holds"] == 0
           and lg["commit"]["calls"] == 1,
           f"{rid} {tr.get('outcome')}, hold {tr.get('steps', {}).get('hold')}, commit calls {lg['commit']['calls']}")


def reattach():
    prep("temporal")
    rid = api("/api/run", {"case_id": "refer"})["request_id"]
    wait(rid, waiting, 45)
    api("/api/run", {"case_id": "refer", "request_id": rid})  # Call again while it waits
    time.sleep(3)
    api("/api/approve", {"request_id": rid, "decision": "approve"})
    t = time.time()
    while time.time() - t < 45:
        runs = [x for x in state()["runs"] if x["request_id"] == rid]
        if runs and all(done(x) for x in runs):
            break
        time.sleep(0.5)
    n, lg = executions(rid), ledger()
    record("Reattach: Call again while it waits attaches to the same Workflow",
           len(runs) == 2 and all((x["tool_result"] or {}).get("outcome") == "fulfilled" for x in runs)
           and n == 1 and lg["hold"]["calls"] == 1,
           f"{rid} agent runs {len(runs)}, Workflow executions {n}, hold calls {lg['hold']['calls']}")


def dedup():
    prep("temporal")
    rid = api("/api/run", {"case_id": "approve"})["request_id"]
    wait(rid, done)
    api("/api/run", {"case_id": "approve", "request_id": rid})  # the same call after it finished
    time.sleep(5)
    runs = [x for x in state()["runs"] if x["request_id"] == rid]
    n, lg = executions(rid), ledger()
    record("Dedup: the same call after completion returns the first result and does no work",
           len(runs) == 2 and all((x["tool_result"] or {}).get("outcome") == "fulfilled" for x in runs)
           and n == 1 and lg["hold"]["calls"] == 1 and lg["commit"]["calls"] == 1,
           f"{rid} agent runs {len(runs)}, Workflow executions {n}, hold calls {lg['hold']['calls']}, "
           f"commit calls {lg['commit']['calls']}")


BEATS = {"b1": b1, "b2": b2, "b2k": b2k, "b3": b3, "b3r": lambda: b3("reject"), "b3d": b3d,
         "b4d": b4d, "b4": b4, "b5": b5, "reattach": reattach, "dedup": dedup}

if __name__ == "__main__":
    for name in sys.argv[1:] or list(BEATS):
        try:
            BEATS[name]()
        except Exception as e:  # noqa: BLE001 (a crashed check is a failed check)
            record(name, False, repr(e)[:300])
    print(f"{sum(ok for _, ok in results)} of {len(results)} checks pass")
    api("/api/reset", {})
    sys.exit(0 if all(ok for _, ok in results) else 1)
