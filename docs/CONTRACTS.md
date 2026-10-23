# Contracts between the components

Every component in this repo talks to the others only through the interfaces below.
Change them here first.

## Processes and ports

| Process | Language | Port | Directory | Role |
|---|---|---|---|---|
| Temporal dev server | CLI | 7233 (gRPC), 8233 (Web UI) | none | `temporal server start-dev` |
| `heritage-py` | Python (FastAPI) | 9101 | `heritage/python` | Mock heritage services: records, policy, notify |
| `heritage-java` | Java 21 | 9103 | `heritage/java` | Mock heritage ledger (HTTP) **and** a Temporal Worker for the ledger Activities |
| `tool-worker` | Python | none | `tools/python` | Temporal Worker: the `FulfillRequest` Workflow and the Python Activities |
| `mcp-server` | TypeScript (Node 22) | 9200 | `mcp-server` | MCP tool interface (Streamable HTTP at `/mcp`); a Temporal Client |
| `agent-platform` | Python | 9300 | `agent-platform` | Plain LangGraph agents (no Temporal); calls MCP tools |
| `dashboard` | Python (FastAPI) | 8095 | `dashboard` | Demo control panel |

Environment (all optional): `TEMPORAL_ADDRESS` (default `localhost:7233`), `TEMPORAL_NAMESPACE` (default `default`),
`TEMPORAL_API_KEY` (if set: TLS on, API key auth, for Temporal Cloud). `HERITAGE_PY_URL` (default `http://localhost:9101`),
`HERITAGE_JAVA_URL` (default `http://localhost:9103`), `MCP_URL` (default `http://localhost:9200/mcp`),
`PLATFORM_URL` (default `http://localhost:9300`).

## Task Queues and Temporal names

| Name | Kind | Owner | Notes |
|---|---|---|---|
| `durable-tools` | Task Queue | `tool-worker` | Workflow `FulfillRequest`; Activities `fetch_records`, `send_notification` |
| `heritage-policy` | Task Queue | `tool-worker` (second Worker in the same process) | Activity `check_policy`. This Worker sets `max_concurrent_activities=3`: the queue in front of the policy service |
| `heritage-ledger` | Task Queue | `heritage-java` | Activities `PlaceHold`, `CommitHold`, `ReleaseHold` (Java) |
| `FulfillRequest` | Workflow type | Python | Workflow Id: `fulfill-<request_id>` |

The Python Workflow calls the Java Activities by type name on `heritage-ledger`. Payloads use the default JSON
data converter in every SDK. Field names are snake_case JSON in every language.

### Java ledger Activities (Activity type names are exact)

| Activity type | Input (one JSON object) | Output (one JSON object) | Errors |
|---|---|---|---|
| `PlaceHold` | `{"request_id": str, "amount": number}` | `{"hold_id": str, "status": "held"}` | Retryable on 503 or connection errors |
| `CommitHold` | `{"hold_id": str}` | `{"hold_id": str, "status": "committed"}` | 503: retryable. 422: non-retryable `ApplicationFailure` with type `LedgerRejected` |
| `ReleaseHold` | `{"hold_id": str}` | `{"hold_id": str, "status": "released"}` | Retryable; idempotent |

Each Java Activity makes an HTTP call to the ledger service (same process, `http://localhost:9103`), so the
Temporal path and the direct path hit the same heritage API.

## Heritage HTTP APIs

All bodies are JSON. Errors are `{"error": str}` with the status code shown.

### `heritage-py` (9101)

| Method and path | Request | Response | Behavior |
|---|---|---|---|
| `POST /records/fetch` | `{"request_id"}` | `{"request_id", "requester", "amount", "criteria_met": [..], "criteria_total": int, "documents": [..]}` | About 300 ms. 404 if the case is unknown |
| `POST /policy/check` | `{"request_id", "amount"}` | `{"request_id", "eligible": bool, "criteria_met": int, "criteria_total": int}` | About 1 s. **At most 3 in flight; the 4th concurrent call gets 429** |
| `POST /notify/send` | `{"request_id", "outcome", "message"}` | `{"request_id", "sent": true}` | About 100 ms |
| `GET /admin/stats` | | `{"records": S, "policy": S, "notify": S}` where `S = {"calls", "ok", "errors", "rejected_429", "in_flight", "max_in_flight"}` | |
| `POST /admin/faults` | `{"service": "records"\|"policy"\|"notify", "mode": "fail_next"\|"slow"\|"clear", "count": int, "delay_ms": int}` | `{"ok": true, "faults": {...}}` | `fail_next`: the next `count` calls return 503. `slow`: add `delay_ms` to each of the next `count` calls (every call until cleared if `count` is omitted) |
| `POST /admin/reset` | | `{"ok": true}` | Clears faults and stats |

Request ids are `<case>` or `<case>-<suffix>` (for example `refer-7f3a`), where `<case>` is `approve`, `refer` or `deny`; `burst-...` uses the `approve` case data.
Cases live in `datasets/cases/<id>.json`.

### `heritage-java` ledger (9103)

| Method and path | Request | Response | Behavior |
|---|---|---|---|
| `POST /ledger/hold` | `{"request_id", "amount"}` | `{"hold_id", "status": "held"}` | Idempotent per `request_id`: returns the existing open hold. `hold_id` = `H-<request_id>-<n>` |
| `POST /ledger/commit` | `{"hold_id"}` | `{"hold_id", "status": "committed"}` | 404 unknown hold. 409 if released |
| `POST /ledger/release` | `{"hold_id"}` | `{"hold_id", "status": "released"}` | Idempotent. 409 if committed |
| `GET /ledger/holds` | | `{"holds": [{"hold_id", "request_id", "amount", "status", "created_at"}]}` | |
| `GET /admin/stats` | | `{"hold": S, "commit": S, "release": S, "open_holds": int, "committed": int, "released": int}` | `open_holds` = holds still `held`: in the direct path these are orphans |
| `POST /admin/faults` | `{"service": "hold"\|"commit"\|"release", "mode": "fail_next"\|"reject_next"\|"slow"\|"clear", "count", "delay_ms"}` | `{"ok": true, "faults": {...}}` | `fail_next`: 503. `reject_next`: 422 (business rejection) |
| `POST /admin/reset` | | `{"ok": true}` | Clears holds, faults and stats |

## `FulfillRequest` Workflow (Python, `durable-tools`)

Input:

```json
{"request_id": "refer", "decision": "approve|refer|deny", "amount": 8000, "reason": "text", "approval_timeout_s": 300}
```

Steps, in order: `records` (fetch_records), `policy` (check_policy on `heritage-policy`), `hold` (PlaceHold, Java),
`approval` (only when `decision == "refer"` or `amount > 5000`), `commit` (CommitHold, Java), `notify` (send_notification).
A `deny` decision skips hold, approval and commit, and only notifies.
Compensation: after `hold` succeeds, `ReleaseHold` is registered; it runs if approval is rejected or times out, or
if `commit` fails non-retryably.

- **Update** `decide_approval(input: {"approver": str, "decision": "approve"|"reject", "note": str})` returns the progress
  object. The validator rejects the call unless the Workflow is waiting for approval and `decision` is valid.
- **Query** `progress()` returns
  `{"request_id", "stage", "steps": {"records"|"policy"|"hold"|"approval"|"commit"|"notify": "pending"|"running"|"done"|"skipped"|"failed"|"compensated"}, "awaiting_approval": bool, "approval_deadline": iso8601|null, "hold_id": str|null, "outcome": str|null, "attempts": {"<step>": int}}`.
- **Result**: `{"request_id", "outcome": "fulfilled"|"denied"|"rejected"|"timed_out"|"failed", "hold_id", "steps": {...}, "approved_by": str|null, "message": str}`.

## MCP tool interface (TypeScript, 9200, Streamable HTTP at `/mcp`)

Tools:

| Tool | Arguments | Result |
|---|---|---|
| `fulfill_request` | `{"request_id": str, "decision": "approve"\|"refer"\|"deny", "amount": number, "reason": str}` | Text content: the JSON result. `isError: true` on failure |
| `get_fulfillment_status` | `{"request_id": str}` | Text content: the `progress()` Query result (temporal mode), or the last known status (direct mode) |

Mode switch (no agent change): `GET /admin/mode` returns `{"mode": "direct"|"temporal"}`; `POST /admin/mode {"mode"}` sets it.
Default `temporal`. `GET /admin/stats` returns `{"mode", "calls", "ok", "errors", "in_flight"}`.

- **temporal mode:** start `FulfillRequest` on `durable-tools` with Workflow Id `fulfill-<request_id>` and
  conflict policy *use existing*, so a repeated call attaches to the run in progress. Await the result.
- **direct mode (today's way):** the same steps as plain HTTP calls in order, no retries. If approval is
  needed, it returns `isError: true` with "approval required: this tool must end here and a second tool call must
  finish later", leaving the hold open. Any failed call fails the whole tool call; earlier side effects stay.

## Agent platform (Python, 9300)

Plain LangGraph `StateGraph`: `intake`, `evaluate`, `review`, `fulfill`. No Temporal imports. `fulfill` calls the
`fulfill_request` MCP tool through `langchain-mcp-adapters`.

- `POST /runs {"case_id", "request_id"?}` returns `{"run_id"}` and runs in the background.
- `GET /runs/{run_id}` returns `{"run_id", "request_id", "status": "running"|"completed"|"failed", "nodes": {name: "pending"|"running"|"done"|"failed"}, "decision", "reason", "tool_result", "error"}`.
- `GET /runs` lists recent runs.
