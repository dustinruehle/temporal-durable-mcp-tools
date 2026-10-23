"""FulfillRequest: one composite tool call, made durable.

The agent platform calls one MCP tool, `fulfill_request`. Behind it, this Workflow calls
the heritage services in order: records, policy, a ledger hold (Java), an optional human
approval, the ledger commit (Java) and a notification. What the Workflow gives the tool,
with no extra code in the tool:

- Each step is an Activity with its own timeout and retry policy, so a flaky service
  costs a retry of that step, not a failed tool call.
- Progress lives in the Workflow's Event History. If the Worker dies mid-call, another
  Worker replays the history and continues at the step it was on.
- A person can approve in the middle of the tool. The Workflow waits on an Update with a
  timer, holding no compute, for as long as the approval window allows.
- Saga compensation: once a hold is placed, its release is registered. A rejection, a
  timeout or a hard commit failure releases it, so no hold is left behind.
- The policy call runs on its own Task Queue, where the Worker runs at most a few at once:
  a queue in front of a service that has none.
"""
from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from durable_tools.activities import check_policy, fetch_records, send_notification
    from durable_tools.config import TASK_QUEUE_LEDGER, TASK_QUEUE_POLICY

STEPS = ("records", "policy", "hold", "approval", "commit", "notify")
APPROVAL_LIMIT = 5000

# Retry with backoff for as long as the step's window allows. Typed business errors stop at once.
SERVICE_RETRY = RetryPolicy(initial_interval=timedelta(seconds=1), backoff_coefficient=2.0,
                            maximum_interval=timedelta(seconds=10),
                            non_retryable_error_types=["HeritageRejected"])
LEDGER_RETRY = RetryPolicy(initial_interval=timedelta(seconds=1), backoff_coefficient=2.0,
                           maximum_interval=timedelta(seconds=10),
                           non_retryable_error_types=["LedgerRejected", "LedgerNotFound", "LedgerConflict"])
# start_to_close bounds one attempt: if a Worker dies mid-call, the attempt times out and runs again on
# another Worker. schedule_to_close bounds the step overall, retries included.
STEP_TIMEOUTS = {"start_to_close_timeout": timedelta(seconds=10), "schedule_to_close_timeout": timedelta(minutes=10)}


@workflow.defn(name="FulfillRequest")
class FulfillRequest:
    def __init__(self) -> None:
        self._request_id = ""
        self._stage = "starting"
        self._steps = {s: "pending" for s in STEPS}
        self._awaiting = False
        self._deadline: str | None = None
        self._hold_id: str | None = None
        self._outcome: str | None = None
        self._approval: dict | None = None

    # ------------------------------------------------------------------ run
    @workflow.run
    async def run(self, req: dict) -> dict:
        self._request_id = rid = req["request_id"]
        decision = req.get("decision", "approve")
        amount = float(req.get("amount", 0))

        if decision == "deny":
            for s in ("records", "policy", "hold", "approval", "commit"):
                self._steps[s] = "skipped"
            await self._notify(rid, "denied", req.get("reason", ""))
            return self._finish("denied", f"Request {rid} denied; requester notified.")

        await self._run_step("records", workflow.execute_activity(
            fetch_records, {"request_id": rid}, retry_policy=SERVICE_RETRY, **STEP_TIMEOUTS))
        policy = await self._run_step("policy", workflow.execute_activity(
            check_policy, {"request_id": rid, "amount": amount}, task_queue=TASK_QUEUE_POLICY,
            retry_policy=SERVICE_RETRY, **STEP_TIMEOUTS))

        hold = await self._run_step("hold", workflow.execute_activity(
            "PlaceHold", {"request_id": rid, "amount": amount}, task_queue=TASK_QUEUE_LEDGER,
            retry_policy=LEDGER_RETRY, result_type=dict, **STEP_TIMEOUTS))
        self._hold_id = hold["hold_id"]
        # Saga: from here on, every exit that doesn't commit releases the hold.

        needs_person = decision == "refer" or amount > APPROVAL_LIMIT or not policy.get("eligible", False)
        if needs_person:
            result = await self._wait_for_approval(req)
            if result is not None:
                return result
        else:
            self._steps["approval"] = "skipped"

        try:
            await self._run_step("commit", workflow.execute_activity(
                "CommitHold", {"hold_id": self._hold_id}, task_queue=TASK_QUEUE_LEDGER,
                retry_policy=LEDGER_RETRY, result_type=dict, **STEP_TIMEOUTS))
        except ActivityError as e:
            await self._compensate()
            await self._notify(rid, "failed", "The ledger rejected the commit; the hold was released.")
            return self._finish("failed", f"Commit failed ({_cause(e)}); hold {self._hold_id} released.")

        await self._notify(rid, "fulfilled", req.get("reason", ""))
        who = (self._approval or {}).get("approver")
        return self._finish("fulfilled", f"Request {rid} fulfilled; hold {self._hold_id} committed."
                            + (f" Approved by {who}." if who else ""))

    # ------------------------------------------------------------------ approval
    async def _wait_for_approval(self, req: dict) -> dict | None:
        """Wait for a person, holding no compute. Returns a result if the request ends here."""
        timeout = timedelta(seconds=int(req.get("approval_timeout_s", 300)))
        self._steps["approval"] = "running"
        self._stage = "awaiting_approval"
        self._awaiting = True
        self._deadline = (workflow.now() + timeout).isoformat()
        try:
            await workflow.wait_condition(lambda: self._approval is not None, timeout=timeout)
        except TimeoutError:
            self._awaiting = False
            self._steps["approval"] = "failed"
            await self._compensate()
            await self._notify(self._request_id, "timed_out", "No approval in time; the hold was released.")
            return self._finish("timed_out", f"No approval within {int(timeout.total_seconds())} s; hold released.")
        self._awaiting = False
        if self._approval["decision"] != "approve":
            self._steps["approval"] = "failed"
            await self._compensate()
            await self._notify(self._request_id, "rejected", self._approval.get("note", ""))
            return self._finish("rejected", f"Rejected by {self._approval['approver']}; hold released.")
        self._steps["approval"] = "done"
        return None

    @workflow.update
    async def decide_approval(self, decision: dict) -> dict:
        """A person's decision. Returns the progress the caller sees right after it lands."""
        self._approval = {"approver": str(decision.get("approver") or "reviewer"),
                          "decision": decision["decision"], "note": str(decision.get("note") or "")}
        await workflow.wait_condition(lambda: not self._awaiting)
        return self.progress()

    @decide_approval.validator
    def _validate_approval(self, decision: dict) -> None:
        # Runs before the Update is accepted: a bad or late decision never enters history.
        if not self._awaiting or self._approval is not None:
            raise ValueError("no approval is pending for this request")
        if decision.get("decision") not in ("approve", "reject"):
            raise ValueError('decision must be "approve" or "reject"')

    @workflow.query
    def progress(self) -> dict:
        return {"request_id": self._request_id, "stage": self._stage, "steps": dict(self._steps),
                "awaiting_approval": self._awaiting, "approval_deadline": self._deadline,
                "hold_id": self._hold_id, "outcome": self._outcome,
                "approved_by": (self._approval or {}).get("approver")}

    # ------------------------------------------------------------------ helpers
    async def _run_step(self, name: str, call):
        self._steps[name] = "running"
        self._stage = name
        try:
            result = await call
        except ActivityError:
            self._steps[name] = "failed"
            raise
        self._steps[name] = "done"
        return result

    async def _compensate(self) -> None:
        if not self._hold_id:
            return
        try:
            await workflow.execute_activity("ReleaseHold", {"hold_id": self._hold_id}, task_queue=TASK_QUEUE_LEDGER,
                                            retry_policy=LEDGER_RETRY, result_type=dict, **STEP_TIMEOUTS)
        except ActivityError as e:
            # The ledger no longer knows the hold (for example, it was reset): nothing is left to release.
            # Any other compensation failure fails the Workflow, so it shows up instead of being hidden.
            if not (isinstance(e.cause, ApplicationError) and e.cause.type == "LedgerNotFound"):
                raise
            workflow.logger.warning("hold %s not found at release; nothing to compensate", self._hold_id)
        self._steps["hold"] = "compensated"
        if self._steps["commit"] == "pending":
            self._steps["commit"] = "skipped"

    async def _notify(self, rid: str, outcome: str, message: str) -> None:
        await self._run_step("notify", workflow.execute_activity(
            send_notification, {"request_id": rid, "outcome": outcome, "message": message},
            retry_policy=SERVICE_RETRY, **STEP_TIMEOUTS))

    def _finish(self, outcome: str, message: str) -> dict:
        self._outcome = outcome
        self._stage = "completed"
        return {"request_id": self._request_id, "outcome": outcome, "hold_id": self._hold_id,
                "steps": dict(self._steps), "approved_by": (self._approval or {}).get("approver"), "message": message}


def _cause(e: ActivityError) -> str:
    c = e.cause
    if isinstance(c, ApplicationError):
        return f"{c.type}: {c.message}"
    return str(c or e)
