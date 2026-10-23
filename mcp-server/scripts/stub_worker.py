"""Throwaway stand-in Worker for testing the MCP server in temporal mode.

Defines a Workflow named FulfillRequest on Task Queue durable-tools that waits
2 seconds (or N seconds for request_id "sleep-N") and returns a fixed result. It answers the progress Query. A request_id
that starts with "fail" makes the Workflow fail, to test the error path.

Run: python scripts/stub_worker.py   (TEMPORAL_ADDRESS defaults to localhost:7233)
"""

import asyncio
import os
from datetime import timedelta

from temporalio import workflow
from temporalio.client import Client
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

STEPS = ["records", "policy", "hold", "approval", "commit", "notify"]


@workflow.defn(name="FulfillRequest")
class StubFulfillRequest:
    def __init__(self) -> None:
        self._request_id = ""
        self._stage = "records"
        self._steps = {s: "pending" for s in STEPS}

    @workflow.run
    async def run(self, req: dict) -> dict:
        self._request_id = req["request_id"]
        self._steps["records"] = "running"
        # "sleep-<N>" waits N seconds instead of 2, to test long tool calls.
        seconds = 2
        if self._request_id.startswith("sleep-"):
            seconds = int(self._request_id.split("-")[1])
        await workflow.sleep(timedelta(seconds=seconds))
        if self._request_id.startswith("fail"):
            self._steps["records"] = "failed"
            raise ApplicationError("records service returned 404 for this case", type="CaseNotFound", non_retryable=True)
        self._steps = {s: "done" for s in STEPS}
        self._stage = "done"
        return {
            "request_id": self._request_id,
            "outcome": "fulfilled",
            "hold_id": f"H-{self._request_id}-1",
            "steps": dict(self._steps),
            "approved_by": None,
            "message": f"stub fulfilled {self._request_id} ({req.get('decision')}, {req.get('amount')}, timeout {req.get('approval_timeout_s')}s)",
        }

    @workflow.query
    def progress(self) -> dict:
        return {
            "request_id": self._request_id,
            "stage": self._stage,
            "steps": dict(self._steps),
            "awaiting_approval": False,
            "approval_deadline": None,
            "hold_id": None,
            "outcome": "fulfilled" if self._stage == "done" else None,
            "attempts": {},
        }


async def main() -> None:
    client = await Client.connect(os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"))
    worker = Worker(client, task_queue="durable-tools", workflows=[StubFulfillRequest])
    print("stub worker polling durable-tools", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
