"""The Python tool Worker: two Workers in one process.

- `durable-tools`: the FulfillRequest Workflow plus the records and notify Activities.
- `heritage-policy`: only the policy Activity, with max_concurrent_activities set low.
  This is the queue in front of the policy service. Tool calls can arrive in a burst;
  the Task Queue holds the extra work, and the service never sees more than this many
  calls at once.

The Java ledger Activities run in their own Worker (heritage/java) on `heritage-ledger`.

Run: uv run python -m durable_tools.worker
"""
from __future__ import annotations

import asyncio
import logging

from temporalio.worker import Worker

from durable_tools.activities import check_policy, fetch_records, send_notification
from durable_tools.config import POLICY_MAX_CONCURRENT, TASK_QUEUE_POLICY, TASK_QUEUE_TOOLS, connect, target
from durable_tools.workflow import FulfillRequest

log = logging.getLogger("durable_tools.worker")


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    client = await connect()
    tools = Worker(client, task_queue=TASK_QUEUE_TOOLS, workflows=[FulfillRequest],
                   activities=[fetch_records, send_notification])
    policy = Worker(client, task_queue=TASK_QUEUE_POLICY, activities=[check_policy],
                    max_concurrent_activities=POLICY_MAX_CONCURRENT)
    address, namespace = target()
    log.info("Tool Worker polling '%s' and '%s' (policy: at most %d at once) on %s (namespace=%s)",
             TASK_QUEUE_TOOLS, TASK_QUEUE_POLICY, POLICY_MAX_CONCURRENT, address, namespace)
    await asyncio.gather(tools.run(), policy.run())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
