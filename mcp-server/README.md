# mcp-server

The tool interface that the agent platform calls. It exposes MCP tools over Streamable HTTP at
`http://localhost:9200/mcp` and runs each tool call in one of two modes. You switch the mode at runtime; the agent
does not change.

- `temporal` (default): the call starts the `FulfillRequest` Workflow on Task Queue `durable-tools` and waits for its
  result. A separate Python Worker runs the Workflow. A repeated call with the same `request_id` attaches to the run
  already in progress, or, after that run closed, returns its result.
- `direct`: the call makes plain HTTP calls to the heritage services, in order, with no retries. A failure part way
  fails the whole call and leaves earlier side effects in place.

The interfaces are defined in [`../docs/CONTRACTS.md`](../docs/CONTRACTS.md).

## Run

Node 22 or later.

```sh
npm install
npm start            # tsx src/server.ts
npm run build        # tsc typecheck and compile to dist/
```

On start it prints `MCP tool server on http://localhost:9200/mcp (mode=temporal)`, then one line per tool call with
the mode, `request_id` and outcome.

## Tools

| Tool | Arguments | Result |
|---|---|---|
| `fulfill_request` | `request_id` (string), `decision` (`approve`, `refer` or `deny`), `amount` (number), `reason` (string) | JSON result as text. `isError: true` on failure |
| `get_fulfillment_status` | `request_id` (string) | The `progress` Query result (temporal mode) or the last known status (direct mode) |

In temporal mode, `fulfill_request` can stay open for minutes while a person approves. The server does not time out
long requests, the response stream sends a keep-alive comment every 15 seconds, and clients that send a
`progressToken` get a progress notification with the Workflow stage every 5 seconds. Set your client timeout above
the approval timeout (for example, `timeout` with `resetTimeoutOnProgress` in the TypeScript SDK client).

In direct mode, `fulfill_request` calls `POST /records/fetch`, `POST /policy/check`, `POST /ledger/hold`, then
`POST /ledger/commit` and `POST /notify/send`. A `deny` decision only notifies, as the Workflow does. When
`decision` is `refer`, `amount` is above 5000, or the policy check says the request isn't eligible, the call returns
an error that starts with
`approval required: this tool must end here and a second tool call must finish later` and leaves the hold open.

## Admin endpoints

| Method and path | Body or result |
|---|---|
| `GET /admin/mode` | `{"mode": "direct" \| "temporal"}` |
| `POST /admin/mode` | Body `{"mode": "direct" \| "temporal"}`. Applies to new calls; calls in flight keep their mode |
| `GET /admin/stats` | `{"mode", "calls", "ok", "errors", "in_flight", "status_calls"}`. Counters cover `fulfill_request` |
| `POST /admin/reset` | Clears the counters and the direct-mode status |
| `GET /healthz` | `{"ok": true, "mode"}` |

## Environment

| Variable | Default | Use |
|---|---|---|
| `MCP_PORT` | `9200` | Listen port |
| `MCP_HOST` | `127.0.0.1` | Listen address. On a localhost address, the server rejects requests with another `Host` header |
| `TOOL_MODE` | `temporal` | Starting mode |
| `TEMPORAL_ADDRESS` | `localhost:7233` | Temporal Service address |
| `TEMPORAL_NAMESPACE` | `default` | Namespace |
| `TEMPORAL_API_KEY` | unset | When set, connects with TLS and this API key (Temporal Cloud). Never logged |
| `APPROVAL_TIMEOUT_S` | `300` | Passed to the Workflow as `approval_timeout_s` |
| `HERITAGE_PY_URL` | `http://localhost:9101` | Records, policy and notify services (direct mode) |
| `HERITAGE_JAVA_URL` | `http://localhost:9103` | Ledger service (direct mode) |
| `DIRECT_HTTP_TIMEOUT_MS` | `30000` | Per-call timeout in direct mode. A timeout fails the call |
| `PROGRESS_INTERVAL_MS` | `5000` | Progress notification interval in temporal mode |
| `SSE_KEEPALIVE_MS` | `15000` | Keep-alive comment interval on open response streams |

## Test scripts

These are throwaway stand-ins for testing this component on its own.

- `scripts/stub_worker.py`: a stand-in `FulfillRequest` Workflow that returns a fixed result after 2 seconds. A
  `request_id` of `fail-...` fails it; `sleep-N` waits N seconds.
- `scripts/fake_heritage.ts`: canned heritage responses on ports 9601 and 9603, with `POST /admin/faults` for 503s.
- `scripts/smoke.ts`: a real MCP client. `npm run smoke -- temporal` or `npm run smoke -- direct`.

```sh
temporal server start-dev --port 7298 --ui-port 8298 --headless
TEMPORAL_ADDRESS=localhost:7298 python scripts/stub_worker.py
npm run fake-heritage
TEMPORAL_ADDRESS=localhost:7298 HERITAGE_PY_URL=http://localhost:9601 HERITAGE_JAVA_URL=http://localhost:9603 npm start
npm run smoke -- temporal
npm run smoke -- direct
```
