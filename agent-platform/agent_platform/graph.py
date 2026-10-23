"""The agent platform: plain LangGraph, no Temporal.

This stands in for an agent platform that already exists: a LangGraph StateGraph with an
intake agent, an evaluation agent and a reviewer agent. Nothing here imports Temporal.
The only thing the platform knows about fulfillment is one MCP tool, `fulfill_request`,
reached through the LangChain MCP adapters. Whether that tool is durable is decided
behind the tool interface, not here.

Models: mock by default (deterministic, no keys). Set PLATFORM_MODEL_MODE=real and
ANTHROPIC_API_KEY to use Claude (PLATFORM_MODEL, default claude-sonnet-4-6).
"""
from __future__ import annotations

import json
import os
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from typing import Any, TypedDict

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph

ROOT = Path(__file__).resolve().parents[2]
CASES = ROOT / "datasets" / "cases"
NODES = ("intake", "evaluate", "review", "fulfill")


class RunState(TypedDict, total=False):
    request_id: str
    case: dict
    intake_summary: str
    recommendation: str
    rationale: str
    decision: str
    reason: str
    tool_result: dict
    tool_error: str


def model_mode() -> str:
    return os.getenv("PLATFORM_MODEL_MODE", "mock").strip().lower()


def _chat_model():
    from langchain_anthropic import ChatAnthropic

    return ChatAnthropic(model=os.getenv("PLATFORM_MODEL", "claude-sonnet-4-6"), max_tokens=400, temperature=0)


def case_for(request_id: str) -> str:
    """Request ids are `<case>` or `<case>-<suffix>`; `burst-...` uses the approve case."""
    prefix = request_id.split("-", 1)[0]
    return "approve" if prefix == "burst" else prefix


def load_case(request_id: str) -> dict:
    case_id = case_for(request_id)
    path = CASES / f"{case_id}.json"
    if case_id not in ("approve", "refer", "deny") or not path.is_file():
        raise ValueError(f"unknown case for request {request_id}")
    case = json.loads(path.read_text())
    case["request_id"] = request_id
    return case


async def _ask_json(system: str, user: str, keys: tuple[str, ...]) -> dict:
    msg = await _chat_model().ainvoke([SystemMessage(system), HumanMessage(user)])
    text = msg.content if isinstance(msg.content, str) else "".join(
        b.get("text", "") for b in msg.content if isinstance(b, dict))
    start, end = text.find("{"), text.rfind("}")
    data = json.loads(text[start:end + 1]) if start >= 0 else {}
    return {k: data.get(k, "") for k in keys}


# ---------------------------------------------------------------- agents (graph nodes)
async def intake(state: RunState) -> dict:
    case = load_case(state["request_id"])
    if model_mode() != "real":
        return {"case": case, "intake_summary": f"{case['requester']} asks for {case['amount']:.0f}. {case['summary']}"}
    out = await _ask_json(
        "You are an intake agent. Summarize the request in one sentence. Reply as JSON: {\"summary\": \"...\"}",
        json.dumps({k: case[k] for k in ("requester", "amount", "summary", "documents")}), ("summary",))
    return {"case": case, "intake_summary": out["summary"] or case["summary"]}


async def evaluate(state: RunState) -> dict:
    case = state["case"]
    met, total = len(case["criteria_met"]), len(case["criteria"])
    if model_mode() != "real":
        rec = case["expected"]
        return {"recommendation": rec, "rationale": f"{met} of {total} criteria met; recommendation: {rec}."}
    out = await _ask_json(
        "You are an evaluation agent. Rule: approve only if every criterion is met and the amount is 5000 or less; "
        "deny if this duplicates a paid request or most criteria are unmet; otherwise refer to a person. "
        "Reply as JSON: {\"recommendation\": \"approve|refer|deny\", \"rationale\": \"one sentence\"}",
        json.dumps({"amount": case["amount"], "criteria": case["criteria"], "criteria_met": case["criteria_met"],
                    "summary": case["summary"]}), ("recommendation", "rationale"))
    rec = out["recommendation"] if out["recommendation"] in ("approve", "refer", "deny") else "refer"
    return {"recommendation": rec, "rationale": out["rationale"]}


async def review(state: RunState) -> dict:
    if model_mode() != "real":
        return {"decision": state["recommendation"], "reason": state["rationale"]}
    out = await _ask_json(
        "You are a reviewer agent. Confirm or change the recommendation and give a short reason for the requester. "
        "Reply as JSON: {\"decision\": \"approve|refer|deny\", \"reason\": \"one sentence\"}",
        json.dumps({"summary": state["intake_summary"], "recommendation": state["recommendation"],
                    "rationale": state["rationale"]}), ("decision", "reason"))
    decision = out["decision"] if out["decision"] in ("approve", "refer", "deny") else state["recommendation"]
    return {"decision": decision, "reason": out["reason"] or state["rationale"]}


def make_fulfill(get_tool: Callable[[], Any]):
    async def fulfill(state: RunState) -> dict:
        """Call the composite MCP tool. One tool call, however many systems sit behind it."""
        tool = await get_tool()
        args = {"request_id": state["request_id"], "decision": state["decision"],
                "amount": float(state["case"]["amount"]), "reason": state["reason"]}
        try:
            result = await tool.ainvoke(args)
        except Exception as e:  # noqa: BLE001 (a failed tool call is reported to the platform, not raised)
            return {"tool_error": str(e)}
        text = result if isinstance(result, str) else _text(result)
        try:
            return {"tool_result": json.loads(text)}
        except (TypeError, ValueError):
            return {"tool_error": text}
    return fulfill


def _text(result: Any) -> str:
    if isinstance(result, list):
        return "".join(b.get("text", "") if isinstance(b, dict) else str(b) for b in result)
    return str(result)


# ---------------------------------------------------------------- wiring
def mcp_client() -> MultiServerMCPClient:
    # A person can approve in the middle of the tool, so a tool call may stay open for minutes.
    return MultiServerMCPClient({"tools": {
        "transport": "streamable_http", "url": os.getenv("MCP_URL", "http://localhost:9200/mcp"),
        "timeout": timedelta(seconds=30), "sse_read_timeout": timedelta(minutes=30),
        "session_kwargs": {"read_timeout_seconds": timedelta(minutes=30)},
    }})


def build_graph(get_tool: Callable[[], Any]):
    g = StateGraph(RunState)
    g.add_node("intake", intake)
    g.add_node("evaluate", evaluate)
    g.add_node("review", review)
    g.add_node("fulfill", make_fulfill(get_tool))
    g.add_edge(START, "intake")
    g.add_edge("intake", "evaluate")
    g.add_edge("evaluate", "review")
    g.add_edge("review", "fulfill")
    g.add_edge("fulfill", END)
    # LangGraph's own in-memory checkpointer: the platform as it runs today. If this process
    # dies, its runs are gone. The durability story in this repo lives behind the tool interface.
    return g.compile(checkpointer=InMemorySaver())
