"""Python Activities: one call to one heritage service each.

An Activity is the unit Temporal retries. Each one makes a single RPC and turns the
service's answer into either a result or a typed error. Temporal decides what happens
next from the error type: retry with backoff (the service was down or busy) or stop
(the request itself is wrong). The Workflow never sees a raw HTTP status.
"""
from __future__ import annotations

import httpx
from temporalio import activity
from temporalio.exceptions import ApplicationError

from durable_tools.config import heritage_py_url


async def _post(path: str, body: dict) -> dict:
    async with httpx.AsyncClient(base_url=heritage_py_url(), timeout=30) as http:
        try:
            r = await http.post(path, json=body)
        except httpx.TransportError as e:
            raise ApplicationError(f"{path}: {e.__class__.__name__}", type="HeritageUnavailable") from e
    if r.status_code == 200:
        return r.json()
    message = f"{path} returned {r.status_code}: {r.json().get('error', r.text) if r.content else ''}"
    if r.status_code == 429:
        raise ApplicationError(message, type="HeritageBusy")
    if r.status_code >= 500:
        raise ApplicationError(message, type="HeritageUnavailable")
    # 4xx other than 429: the request is wrong. Retrying won't help.
    raise ApplicationError(message, type="HeritageRejected", non_retryable=True)


@activity.defn
async def fetch_records(req: dict) -> dict:
    """Records service: the request and its documents."""
    return await _post("/records/fetch", {"request_id": req["request_id"]})


@activity.defn
async def check_policy(req: dict) -> dict:
    """Policy service: allows only a few calls in flight. Runs on the heritage-policy Task Queue."""
    return await _post("/policy/check", {"request_id": req["request_id"], "amount": req.get("amount", 0)})


@activity.defn
async def send_notification(req: dict) -> dict:
    """Notification service: tells the requester the outcome."""
    return await _post("/notify/send", {"request_id": req["request_id"], "outcome": req["outcome"],
                                        "message": req.get("message", "")})
