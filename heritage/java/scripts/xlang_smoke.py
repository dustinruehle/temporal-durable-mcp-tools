"""Cross-language smoke test for the Java ledger Activities.

A small Python Workflow (on its own Task Queue) calls the Java Activities `PlaceHold`, `CommitHold` and
`ReleaseHold` by type name on the `heritage-ledger` Task Queue. It checks three paths:

1. Happy path: PlaceHold, then CommitHold.
2. Retry: the ledger fails the next 2 hold calls with 503; the Activity retries and succeeds.
3. Rejection: the ledger rejects the next commit with 422; the Workflow gets a non-retryable
   ApplicationError of type LedgerRejected after one attempt, then releases the hold.

Needs the Java app running (`./gradlew run`) and the `temporalio` package. Resets the ledger first.

    TEMPORAL_ADDRESS=localhost:7233 HERITAGE_JAVA_URL=http://localhost:9103 python scripts/xlang_smoke.py
"""

import asyncio
import json
import os
import sys
import urllib.request
import uuid
from datetime import timedelta

from temporalio import workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.worker import Worker

TASK_QUEUE = "xlang-smoke"
LEDGER_TASK_QUEUE = "heritage-ledger"
LEDGER_URL = os.environ.get("HERITAGE_JAVA_URL", "http://localhost:9103").rstrip("/")

RETRY = RetryPolicy(initial_interval=timedelta(milliseconds=200), backoff_coefficient=1.5, maximum_attempts=5)


@workflow.defn
class LedgerSmokeWorkflow:
    @workflow.run
    async def run(self, args: dict) -> dict:
        opts = dict(
            task_queue=LEDGER_TASK_QUEUE,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RETRY,
        )
        hold = await workflow.execute_activity(
            "PlaceHold", {"request_id": args["request_id"], "amount": args["amount"]}, **opts
        )
        result = {"hold": hold}
        try:
            result["commit"] = await workflow.execute_activity("CommitHold", {"hold_id": hold["hold_id"]}, **opts)
        except ActivityError as err:
            cause = err.cause
            if not isinstance(cause, ApplicationError):
                raise
            result["commit_error"] = {
                "type": cause.type,
                "non_retryable": cause.non_retryable,
                "message": cause.message,
                "details": list(cause.details),
            }
            # Compensation: release the hold we placed.
            result["release"] = await workflow.execute_activity(
                "ReleaseHold", {"hold_id": hold["hold_id"]}, **opts
            )
        return result


def ledger(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(LEDGER_URL + path, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


async def run_case(client: Client, request_id: str, amount: float) -> dict:
    return await client.execute_workflow(
        LedgerSmokeWorkflow.run,
        {"request_id": request_id, "amount": amount},
        id=f"xlang-smoke-{request_id}-{uuid.uuid4().hex[:6]}",
        task_queue=TASK_QUEUE,
        execution_timeout=timedelta(minutes=2),
    )


async def main() -> int:
    address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    namespace = os.environ.get("TEMPORAL_NAMESPACE", "default")
    api_key = os.environ.get("TEMPORAL_API_KEY") or None
    client = await Client.connect(address, namespace=namespace, api_key=api_key, tls=bool(api_key))

    ledger("POST", "/admin/reset")
    failures = []

    def check(name: str, ok: bool, detail) -> None:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {json.dumps(detail)}")
        if not ok:
            failures.append(name)

    async with Worker(client, task_queue=TASK_QUEUE, workflows=[LedgerSmokeWorkflow]):
        # 1. Happy path.
        r1 = await run_case(client, "t1", 10)
        check("PlaceHold + CommitHold", r1["hold"]["status"] == "held" and r1["commit"]["status"] == "committed", r1)

        # 2. Two 503s on hold: retried by Temporal, then succeeds.
        ledger("POST", "/admin/faults", {"service": "hold", "mode": "fail_next", "count": 2})
        r2 = await run_case(client, "t2", 20)
        hold_stats = ledger("GET", "/admin/stats")["hold"]
        check(
            "PlaceHold retried through 2x 503",
            r2.get("commit", {}).get("status") == "committed" and hold_stats["errors"] == 2,
            {"result": r2, "hold_stats": hold_stats},
        )

        # 3. 422 on commit: non-retryable LedgerRejected, then ReleaseHold compensates.
        ledger("POST", "/admin/faults", {"service": "commit", "mode": "reject_next", "count": 1})
        before = ledger("GET", "/admin/stats")["commit"]["calls"]
        r3 = await run_case(client, "t3", 30)
        commit_calls = ledger("GET", "/admin/stats")["commit"]["calls"] - before
        err = r3.get("commit_error", {})
        check(
            "CommitHold 422 -> LedgerRejected (non-retryable, 1 attempt), ReleaseHold ran",
            err.get("type") == "LedgerRejected"
            and err.get("non_retryable") is True
            and commit_calls == 1
            and r3.get("release", {}).get("status") == "released",
            {"result": r3, "commit_attempts": commit_calls},
        )

    stats = ledger("GET", "/admin/stats")
    print("ledger stats:", json.dumps({k: stats[k] for k in ("open_holds", "committed", "released")}))
    print("RESULT:", "FAIL " + ", ".join(failures) if failures else "all checks passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
