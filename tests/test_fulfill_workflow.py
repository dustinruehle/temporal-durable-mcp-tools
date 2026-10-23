"""FulfillRequest Workflow tests with Temporal's time-skipping test server.

The Java ledger Activities are replaced by Python stand-ins with the same Activity type
names on the same Task Queue, which is exactly how the real Workflow finds them.
"""
from __future__ import annotations

import uuid

import pytest
from durable_tools.config import TASK_QUEUE_LEDGER, TASK_QUEUE_POLICY, TASK_QUEUE_TOOLS
from durable_tools.workflow import FulfillRequest
from temporalio import activity
from temporalio.client import WorkflowUpdateFailedError
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker


class Ledger:
    def __init__(self, commit_failures: int = 0, reject_commit: bool = False):
        self.holds: dict[str, str] = {}
        self.commit_failures = commit_failures
        self.reject_commit = reject_commit
        self.commit_attempts = 0

    def activities(self):
        @activity.defn(name="PlaceHold")
        async def place_hold(req: dict) -> dict:
            hid = f"H-{req['request_id']}-1"
            self.holds.setdefault(hid, "held")
            return {"hold_id": hid, "status": "held"}

        @activity.defn(name="CommitHold")
        async def commit_hold(req: dict) -> dict:
            self.commit_attempts += 1
            if self.reject_commit:
                raise ApplicationError("ledger rejected the commit", type="LedgerRejected", non_retryable=True)
            if self.commit_attempts <= self.commit_failures:
                raise ApplicationError("ledger unavailable", type="LedgerUnavailable")
            self.holds[req["hold_id"]] = "committed"
            return {"hold_id": req["hold_id"], "status": "committed"}

        @activity.defn(name="ReleaseHold")
        async def release_hold(req: dict) -> dict:
            self.holds[req["hold_id"]] = "released"
            return {"hold_id": req["hold_id"], "status": "released"}

        return [place_hold, commit_hold, release_hold]


@activity.defn(name="fetch_records")
async def fake_records(req: dict) -> dict:
    return {"request_id": req["request_id"], "amount": 1200}


@activity.defn(name="check_policy")
async def fake_policy(req: dict) -> dict:
    return {"request_id": req["request_id"], "eligible": req["request_id"] != "refer", "criteria_met": 2, "criteria_total": 3}


@activity.defn(name="send_notification")
async def fake_notify(req: dict) -> dict:
    return {"request_id": req["request_id"], "sent": True}


async def _run(env: WorkflowEnvironment, ledger: Ledger, req: dict, approve: dict | None = None) -> dict:
    async with Worker(env.client, task_queue=TASK_QUEUE_TOOLS, workflows=[FulfillRequest],
                      activities=[fake_records, fake_notify]), \
            Worker(env.client, task_queue=TASK_QUEUE_POLICY, activities=[fake_policy]), \
            Worker(env.client, task_queue=TASK_QUEUE_LEDGER, activities=ledger.activities()):
        handle = await env.client.start_workflow(FulfillRequest.run, req, id=f"fulfill-{uuid.uuid4().hex[:8]}",
                                                 task_queue=TASK_QUEUE_TOOLS)
        if approve is not None:
            # Wait until the Workflow is paused for a person, then decide.
            for _ in range(50):
                if (await handle.query(FulfillRequest.progress))["awaiting_approval"]:
                    break
                await env.sleep(0.2)
            await handle.execute_update(FulfillRequest.decide_approval, approve)
        return await handle.result()


@pytest.fixture
async def env():
    async with await WorkflowEnvironment.start_time_skipping() as e:
        yield e


async def test_happy_path_commits(env):
    ledger = Ledger()
    r = await _run(env, ledger, {"request_id": "approve", "decision": "approve", "amount": 1200})
    assert r["outcome"] == "fulfilled"
    assert r["steps"]["approval"] == "skipped"
    assert ledger.holds == {"H-approve-1": "committed"}


async def test_flaky_commit_is_retried_not_failed(env):
    ledger = Ledger(commit_failures=2)
    r = await _run(env, ledger, {"request_id": "approve", "decision": "approve", "amount": 1200})
    assert r["outcome"] == "fulfilled"
    assert ledger.commit_attempts == 3


async def test_approval_in_the_middle_of_the_tool(env):
    ledger = Ledger()
    r = await _run(env, ledger, {"request_id": "refer", "decision": "refer", "amount": 8000},
                   approve={"approver": "reviewer-1", "decision": "approve"})
    assert r["outcome"] == "fulfilled" and r["approved_by"] == "reviewer-1"
    assert ledger.holds["H-refer-1"] == "committed"


async def test_rejection_releases_the_hold(env):
    ledger = Ledger()
    r = await _run(env, ledger, {"request_id": "refer", "decision": "refer", "amount": 8000},
                   approve={"approver": "reviewer-1", "decision": "reject"})
    assert r["outcome"] == "rejected"
    assert ledger.holds["H-refer-1"] == "released"


async def test_approval_timeout_releases_the_hold(env):
    ledger = Ledger()
    r = await _run(env, ledger, {"request_id": "refer", "decision": "refer", "amount": 8000, "approval_timeout_s": 60})
    assert r["outcome"] == "timed_out"
    assert ledger.holds["H-refer-1"] == "released"


async def test_hard_commit_failure_compensates(env):
    ledger = Ledger(reject_commit=True)
    r = await _run(env, ledger, {"request_id": "approve", "decision": "approve", "amount": 1200})
    assert r["outcome"] == "failed"
    assert r["steps"]["hold"] == "compensated"
    assert ledger.holds["H-approve-1"] == "released"


async def test_deny_only_notifies(env):
    ledger = Ledger()
    r = await _run(env, ledger, {"request_id": "deny", "decision": "deny", "amount": 300})
    assert r["outcome"] == "denied" and ledger.holds == {}


async def test_validator_rejects_a_decision_when_nothing_is_pending(env):
    ledger = Ledger()
    async with Worker(env.client, task_queue=TASK_QUEUE_TOOLS, workflows=[FulfillRequest],
                      activities=[fake_records, fake_notify]), \
            Worker(env.client, task_queue=TASK_QUEUE_POLICY, activities=[fake_policy]), \
            Worker(env.client, task_queue=TASK_QUEUE_LEDGER, activities=ledger.activities()):
        handle = await env.client.start_workflow(FulfillRequest.run, {"request_id": "refer", "decision": "refer", "amount": 8000},
                                                 id="fulfill-validator", task_queue=TASK_QUEUE_TOOLS)
        for _ in range(50):
            if (await handle.query(FulfillRequest.progress))["awaiting_approval"]:
                break
            await env.sleep(0.2)
        with pytest.raises(WorkflowUpdateFailedError):
            await handle.execute_update(FulfillRequest.decide_approval, {"approver": "x", "decision": "maybe"})
        await handle.execute_update(FulfillRequest.decide_approval, {"approver": "x", "decision": "approve"})
        assert (await handle.result())["outcome"] == "fulfilled"


async def test_release_of_a_hold_the_ledger_no_longer_knows_still_completes(env):
    ledger = Ledger()
    acts = ledger.activities()

    @activity.defn(name="ReleaseHold")
    async def release_gone(req: dict) -> dict:
        raise ApplicationError("unknown hold", type="LedgerNotFound", non_retryable=True)

    acts = [a for a in acts if a.__temporal_activity_definition.name != "ReleaseHold"] + [release_gone]
    async with Worker(env.client, task_queue=TASK_QUEUE_TOOLS, workflows=[FulfillRequest],
                      activities=[fake_records, fake_notify]), \
            Worker(env.client, task_queue=TASK_QUEUE_POLICY, activities=[fake_policy]), \
            Worker(env.client, task_queue=TASK_QUEUE_LEDGER, activities=acts):
        handle = await env.client.start_workflow(FulfillRequest.run, {"request_id": "refer", "decision": "refer", "amount": 8000},
                                                 id="fulfill-gone", task_queue=TASK_QUEUE_TOOLS)
        for _ in range(50):
            if (await handle.query(FulfillRequest.progress))["awaiting_approval"]:
                break
            await env.sleep(0.2)
        await handle.execute_update(FulfillRequest.decide_approval, {"approver": "x", "decision": "reject"})
        r = await handle.result()
        assert r["outcome"] == "rejected" and r["steps"]["hold"] == "compensated"
