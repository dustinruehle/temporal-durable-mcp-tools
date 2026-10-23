"""Connection settings shared by the Python Temporal Client and Workers."""
from __future__ import annotations

import os

from dotenv import load_dotenv
from temporalio.client import Client

load_dotenv()

TASK_QUEUE_TOOLS = "durable-tools"
TASK_QUEUE_POLICY = "heritage-policy"
TASK_QUEUE_LEDGER = "heritage-ledger"
# The queue in front of the policy service: at most this many policy calls run at once.
POLICY_MAX_CONCURRENT = int(os.getenv("POLICY_MAX_CONCURRENT", "3"))


def heritage_py_url() -> str:
    return os.getenv("HERITAGE_PY_URL", "http://localhost:9101")


def target() -> tuple[str, str]:
    return os.getenv("TEMPORAL_ADDRESS", "localhost:7233"), os.getenv("TEMPORAL_NAMESPACE", "default")


async def connect() -> Client:
    """Local dev server by default. With TEMPORAL_API_KEY set, connect to Temporal Cloud over TLS."""
    address, namespace = target()
    api_key = os.getenv("TEMPORAL_API_KEY") or None
    return await Client.connect(address, namespace=namespace, api_key=api_key, tls=bool(api_key))
