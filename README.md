# Durable MCP tools: Temporal behind the tool interface

Your agents already reason well. What breaks in production is the work they ask for: one tool call that touches
several existing systems, in order, sometimes with a person in the middle. Built as one plain tool, a failure in any
one system fails the whole call. Split into thin tools, the agent has to call them in the right order, and the
heritage systems leak into the agent: their call order, their failures and their approvals end up in its prompt and
its context.

This repo adds a durable execution layer, [Temporal](https://docs.temporal.io/temporal), behind the MCP tool
interface. The agent platform stays as it is (plain LangGraph) and still makes one tool call. Behind the tool, a
[Temporal Workflow](https://docs.temporal.io/workflows) runs the work across the systems. That layer gives you:

- **Reliability.** Each step retries on its own, a crash resumes at the step it was on, and when a step fails for
  good, the work already done is undone (here, the ledger hold is released).
- **Scale.** Workers pull work from Task Queues, so you add Workers to handle more. A burst waits in the queue instead
  of overloading a service that has no queue of its own.
- **One clean contract for the agent.** The agent sees one tool. How the heritage systems are called, retried, queued
  and approved stays behind it, including a person who approves in the middle of the call.

![The agent platform makes one MCP tool call. Behind the tool interface, Temporal runs the work across the heritage systems: it retries each step, resumes after a crash, queues the work, undoes partial work and waits for a person. The heritage systems keep their existing APIs.](docs/images/overview.svg)

The demo runs the same business request two ways, with one switch and no change to the agent:

| | **Direct (today)** | **Temporal behind the tool** |
|---|---|---|
| A heritage service fails twice | The whole tool call fails. A ledger hold is left behind. | That step retries with backoff. The tool call succeeds. |
| The Worker running the tool's steps dies | The call is lost, with partial side effects. | When a Worker is back, it replays the history and continues at the step it was on. |
| A person must approve mid-way | The tool has to end early and a second tool call must finish later. | One tool call waits for the person, holding no compute, with a deadline. |
| 30 tool calls arrive at once | The service allows 3 in flight; the rest get 429 errors. | The Task Queue holds the extra work. The service never sees more than 3. |
| The last step is rejected | Earlier steps stay done. Someone cleans up by hand. | A [saga](https://docs.temporal.io/design-patterns/saga-pattern) releases the hold and reports why. |
| The agent repeats the same call | It runs again. After the first call commits, a repeat places and commits a second hold. | The request id is the Workflow Id: the repeat attaches to the run, or gets its result. |

The stack is polyglot on purpose: the agents and the Workflow are **Python**, the MCP tool server is **TypeScript**,
and one heritage system (the ledger) is **Java**, with its Activities running in a Java Worker. One Workflow
orchestrates all of it.

**What changes for you, and what doesn't**

- **Agents:** no change. They still make one MCP tool call. Nothing in the agent platform imports Temporal.
- **Heritage services:** no change. Activities call them over their existing APIs. Each team can adopt at its own pace;
  the Java Worker here shows a team that runs its own Activities in its own language.
- **The tool:** its body moves into a Workflow, and the tool starts it. Your tools are TypeScript? The Workflow can be
  TypeScript too, next to the tool; this repo uses Python for it to show the languages working together.
- **What it adds:** a Temporal Service to run or use (self-hosted or Temporal Cloud), Workers to deploy, and a few calls to
  the Temporal Service per step, which adds some latency to each step. In exchange, the retries, waits, queuing, recovery and undo above stop being
  code you write per tool.

## Contents

- [Why this matters](#why-this-matters)
- [What's in the demo](#whats-in-the-demo)
- [Run it](#run-it)
- [Walk through the demo](#walk-through-the-demo)
- [How it works](#how-it-works)
- [Temporal concepts and patterns used](#temporal-concepts-and-patterns-used)
- [What you don't have to build](#what-you-dont-have-to-build)
- [Adopt it one tool at a time](#adopt-it-one-tool-at-a-time)
- [Production notes and limits](#production-notes-and-limits)
- [Further reading](#further-reading)
- [Reference: layout, configuration and troubleshooting](#reference-layout-configuration-and-troubleshooting)

## Why this matters

### The thin tool or thick tool question

A composite tool framework hosts tools that agents call over MCP. Most useful tools call more than one system.
Teams then argue about how to shape them:

- **Thin tools:** one tool per system call. The agent calls five tools in the right order. Durability becomes the
  agent's job: the order, the retries, what to do when call four fails after calls one to three changed something.
  The agent's prompt and context fill up with plumbing that has nothing to do with its goal.
- **Thick tools:** one tool calls the five systems in order. The agent's view is clean. But if the tool isn't
  durable, any failure fails the whole tool call, and the systems it already changed stay changed.

There's a second problem that pushes teams back to thin tools: **a person in the middle**. If step three needs an
approval, a plain tool can't wait for it. It has to end, and a second tool call has to pick the work up later. Now
someone has to keep the state between the two calls, expire it, and clean up if the second call never comes.

### What durable execution changes

[Durable Execution](https://docs.temporal.io/temporal) means the tool's progress is recorded as it runs, in the
Workflow's [Event History](https://docs.temporal.io/workflow-execution/event). That one property removes the
trade-off:

- The tool can be thick, because a failure costs one retried step, not the whole call.
- The tool can wait for a person for minutes or days, because waiting is a recorded state, not a running process.
- The tool can survive the process that runs it, because any Worker can replay the history and continue.
- The calls to existing systems can be shaped: queued, rate limited, retried with backoff, or compensated.

The agent doesn't change. It still makes one MCP tool call. Temporal sits behind the tool interface, between the
tool and the systems it calls.

### Why behind the tool interface, not in the agent

You can make agents themselves durable too (Temporal has integrations for agent frameworks, including the
[LangGraph plugin](https://docs.temporal.io/develop/python/integrations/langgraph), in Public Preview). This repo
deliberately doesn't. It shows the smallest change that pays off first: **keep the agent platform as it is and make
the tools durable**. The agent platform here imports nothing from Temporal.

## What's in the demo

![What runs in the demo. T7, the dashboard, starts an agent run on T6, the agent platform. T6 makes one MCP tool call to T5, the MCP tool interface, which starts a Workflow on T1, the Temporal Service. T4, the Python tool Worker, and the Java Worker in T3 poll Temporal and call the heritage services in T2 and T3. In direct mode, T5 calls the heritage services itself.](docs/images/runtime.svg)

Each box is one process, and its **T number is the terminal it runs in** (see
[one terminal per process](#or-one-terminal-per-process)). Read the lines this way:

- **Solid lines are the request path.** The dashboard starts an agent run, as a user or an upstream app would. The
  agent makes one MCP tool call. The MCP server starts a Workflow, and the Workers run its steps against the heritage
  services. The Workers connect out to Temporal; nothing connects in to them.
- **Dotted lines are direct mode,** today's way, for contrast: the MCP server calls the services itself.
- **Dashed lines are demo controls only:** the direct or Temporal switch, the fault switches and stats on the
  heritage services, and the Approve and Reject buttons (a Temporal Update). In production, whatever calls your
  agents takes the dashboard's place, plus a reviewer screen that sends the approval.

The agent is the only MCP client. The dashboard never makes a tool call itself.

| Terminal | Process | Language | What it stands for | Code |
|---|---|---|---|---|
| T1 | Temporal Service | Temporal CLI dev server, or Temporal Cloud | The durable execution layer: Event History and Task Queues. On Temporal Cloud you don't run T1. | none |
| T2 | heritage-py | Python (FastAPI) | Heritage systems of record: records, policy, notify. Live RPC services with no queue; policy rejects more than 3 in flight. | [`heritage/python/`](heritage/python/heritage_py/app.py) |
| T3 | heritage-java | Java 21, [Temporal Java SDK](https://docs.temporal.io/develop/java) | A heritage system owned by another team, in another language: the ledger API, plus the Java Worker for its Activities. | [`heritage/java/`](heritage/java/) |
| T4 | Tool Worker | Python, [Temporal Python SDK](https://docs.temporal.io/develop/python) | The thick tool, made durable: the `FulfillRequest` Workflow and one Activity per heritage call. A second Worker on `heritage-policy` runs at most 3 policy calls at once. | [`tools/python/durable_tools/`](tools/python/durable_tools/workflow.py) |
| T5 | MCP tool interface | TypeScript, [MCP SDK](https://github.com/modelcontextprotocol/typescript-sdk) | Your composite tool framework. Exposes `fulfill_request`. A runtime switch picks direct or Temporal. | [`mcp-server/`](mcp-server/) |
| T6 | Agent platform | Python, [LangGraph](https://langchain-ai.github.io/langgraph/) | Your existing agent platform: intake, evaluate and review agents, then one tool call. No Temporal. | [`agent-platform/`](agent-platform/agent_platform/graph.py) |
| T7 | Dashboard | Python, HTML | The demo driver: starts agent runs, holds the controls, and shows every layer's state. | [`dashboard/`](dashboard/demo_dashboard/) |

All data is synthetic: three requests in [`datasets/cases/`](datasets/cases/) (`approve`, `refer`, `deny`).

## Run it

### Prerequisites

- [Temporal CLI](https://docs.temporal.io/cli) 1.3 or later (`temporal server start-dev` runs a local Temporal Service)
- Python 3.11 or later and [uv](https://docs.astral.sh/uv/)
- Node.js 22 or later
- Java 21 (the Gradle wrapper is included)
- `curl` and `nc`, which the start script uses (both ship with macOS and most Linux distributions)

### Start everything

```bash
make setup     # Python, Node and Java dependencies
make up        # starts a dev server if none is running, then every process in the background
```

Open the dashboard at <http://localhost:8095> and the Temporal Web UI at <http://localhost:8233> (on Temporal
Cloud, <https://cloud.temporal.io>). `make status` shows what answers; logs are in `logs/`. `make down` stops what
`make up` started.

### Before you present

1. Start everything (`make up`, or one terminal per process below) and run `make status`. Every process must answer.
2. Run the beat checks once: `uv run python scripts/check_beats.py`. It takes about 2 minutes and must end with
   `11 of 11 checks pass`. It resets the demo when it's done.
3. Open two browser tabs: the dashboard and the Temporal Web UI (your Namespace's Workflows list).
4. If you plan to show beat 2k, keep the tool Worker's terminal (T4) visible, or have `make stop-tool-worker` ready.

### Or one terminal per process

Use this when you want to stop a process by hand. Beat 2k stops the tool Worker. Start them in this order; the
terminal numbers match the boxes in the [diagram](#whats-in-the-demo).

| Terminal | Command | Box in the diagram | Ready when you see |
|---|---|---|---|
| T1 | `make server` | Temporal Service (skip it on Temporal Cloud) | the dev server's startup banner |
| T2 | `make heritage-py` | heritage-py | `heritage-py: records, policy (max 3 in flight), notify on http://localhost:9101` |
| T3 | `make heritage-java` | heritage-java (ledger and Java Worker) | `Java ledger Worker polling 'heritage-ledger'` |
| T4 | `make tool-worker` | Tool Worker | `Tool Worker polling 'durable-tools' and 'heritage-policy'` |
| T5 | `make mcp-server` | MCP tool interface | `MCP tool server on http://localhost:9200/mcp` |
| T6 | `make platform` | Agent platform | `agent-platform (plain LangGraph ...)` |
| T7 | `make dashboard` | Dashboard | `dashboard on http://localhost:8095` |

Then open the dashboard at <http://localhost:8095>.

The agents use deterministic mock models by default, so no keys are needed. To use Claude, set
`PLATFORM_MODEL_MODE=real` and `ANTHROPIC_API_KEY` in `.env` (copy [`.env.example`](.env.example)).
To use Temporal Cloud instead of the dev server, set `TEMPORAL_ADDRESS` (your region endpoint, for example
`us-east-1.aws.api.temporal.io:7233`), `TEMPORAL_NAMESPACE` (`<namespace>.<account>`), `TEMPORAL_API_KEY` and
`TEMPORAL_UI_URL=https://cloud.temporal.io` in `.env`. `make` and `make up` pass `.env` to every process.

### Run the tests

```bash
make test      # Workflow tests (time-skipping test server), TypeScript build, Java tests
```

To check every beat end to end against the running stack, the same calls the dashboard buttons make:

```bash
uv run python scripts/check_beats.py      # resets the demo between checks; don't run it while you present
```

## Walk through the demo

Each beat is one button on the dashboard. The button resets the demo, sets the tool mode and a fault switch on a
heritage service, and starts an agent run, so every beat starts clean and you can run them in any order. Watch
three places: the **Tool calls** table (each layer's state), the
**Heritage services** table (what the existing systems saw), and **Ledger holds** (open holds with no tool call
running are work left behind).

| Beat | What it shows | About |
|---|---|---|
| [1](#beat-1-todays-way-a-flaky-ledger) | Today's way: one flaky system fails the whole tool call and leaves a hold behind | 1 min |
| [2](#beat-2-the-same-failure-temporal-behind-the-tool) | The same failure with Temporal behind the tool: one step retries, the call succeeds | 1.5 min |
| [2k](#beat-2k-kill-the-tool-worker-mid-call) | The tool Worker dies mid-call and the call finishes after a restart | 2 min |
| [3](#beat-3-a-person-in-the-middle-of-the-tool) | A person approves in the middle of one tool call | 2 min |
| [3d](#beat-3d-the-same-approval-todays-way) | The same approval today: the tool has to end early | 1 min |
| [4](#beat-4-a-burst-of-30-tool-calls) | 30 calls at once: 429s today, a queue with Temporal | 2.5 min |
| [5](#beat-5-the-ledger-rejects-the-commit) | A business rejection: no retry, and the saga releases the hold | 1.5 min |
| [Web UI](#the-web-ui) | The history of one tool call | 1.5 min |

### Beat 1: Today's way, a flaky ledger

**Do:** click **1 · Today's way**. Direct mode; the ledger's commit fails twice with 503.

**See:** the agent's one tool call fails at the commit step. The ledger shows **1 open hold**: the hold step
succeeded, the commit didn't, and nothing undoes it.

**Why it matters:** one flaky system fails the whole tool call. Then comes the part that hurts later: the systems
the tool already changed stay changed.

### Beat 2: The same failure, Temporal behind the tool

**Do:** click **2 · Same failure, Temporal behind the tool**. Same fault, Temporal mode. The agent code is the same.

**See:** the commit step shows attempt 2, then 3, with the last failure ("ledger commit unavailable"). Then it
succeeds. The tool call
returns `fulfilled`. No open holds.

**Why it matters:** each step is an [Activity](https://docs.temporal.io/activities) with its own
[Retry Policy](https://docs.temporal.io/encyclopedia/retry-policies). A flaky system costs a retry of one step, not
the whole tool call. The tool's code has no retry loop.

### Beat 2k: Kill the tool Worker mid-call

**Do:** click **2k**. It resets stats first. The policy service is slow for this one call (about 8 seconds), so you
have time. While the
policy step shows `running`, stop the tool Worker:

- With one terminal per process: press Ctrl-C in T4 (the tool Worker), then run `make tool-worker` again.
- With `make up`: run `make stop-tool-worker`, then `make up` (it starts only what's missing).

**See:** while the Worker is down, the dashboard shows the Workflow **Running** and the tool Worker **offline**. The
agent's tool call stays open. When the Worker is back, the policy step runs again (attempt 2) and the call finishes.
The records step, already done, isn't called again: the records service's call count stays at 1.

**Why it matters:** the tool's progress lives in Temporal, not in the process. That's what lets you deploy, scale or
lose a Worker in the middle of work. A Worker that's stopped with Ctrl-C reports its in-flight attempt, so the retry
starts as soon as a Worker returns. A Worker that dies without warning is detected by the attempt's
[timeout](https://docs.temporal.io/encyclopedia/detecting-activity-failures) (10 seconds here).

### Beat 3: A person in the middle of the tool

**Do:** click **3 · A person in the middle of the tool**. The `refer` request is above the auto-approval limit.

**See:** the tool call is open and the row says **Waiting for a person**, with the time left before the approval
window closes (300 seconds by default, `APPROVAL_TIMEOUT_S`). No Worker is busy: the wait is a durable timer and a
condition recorded in Event History. (The MCP server and the agent keep the HTTP call open; see
[Production notes](#production-notes-and-limits).) While it waits, click **Call again**: a second agent run makes the
same tool call, attaches to the same Workflow, and the ledger still shows one hold. Then click **Approve** (or
**Reject**). Both agent runs get the same result.

**Why it matters:** one tool call, not two. The Workflow waits on a durable
[timer and condition](https://docs.temporal.io/develop/python/workflows/timers), and the decision arrives as an
[Update](https://docs.temporal.io/develop/python/workflows/message-passing#updates). Its validator rejects a
decision when nothing is pending, before it's recorded. A reject releases the hold. If nobody answers in time,
the Workflow releases the hold and returns `timed_out`. This is the
[Approval pattern](https://docs.temporal.io/design-patterns/approval).

### Beat 3d: The same approval, today's way

**Do:** click **3d**.

**See:** the tool call ends early with "approval required: this tool must end here and a second tool call must
finish later". The ledger shows an **open hold** that nothing owns.

**Why it matters:** without durable waits, a human step splits a tool in two, and someone has to own the state in
between.

### Beat 4: A burst of 30 tool calls

**Do:** click **4d** (direct), note the 429s, then click **4** (Temporal). Each button resets the stats first,
then starts 30 agent runs at once.

**See:** in direct mode, the policy service, which allows 3 calls in flight, turns most calls away with **429**
and many tool calls fail. In Temporal mode, all 30 finish. The policy service's **max in flight stays at 3** and
it returns no 429s.

**Why it matters:** a live RPC service has no queue. Here the policy calls run on their own
[Task Queue](https://docs.temporal.io/task-queue), and the Worker on that queue runs at most 3 at once
(`max_concurrent_activities`). Extra work waits in the queue on the Temporal Service. Nothing is dropped,
and you set the limit in one place, on the Worker. This is the concurrency form of the
[Downstream Rate Limiting pattern](https://docs.temporal.io/design-patterns/downstream-rate-limiting); see also the
[guide to protecting downstream APIs](https://docs.temporal.io/guides/rate-limit-downstream-apis).

### Beat 5: The ledger rejects the commit

**Do:** click **5 · The ledger rejects the commit**. The ledger returns 422: a business rejection, not an outage.

**See:** no retry. The Java Activity raises a non-retryable `LedgerRejected` error, the Workflow runs
`ReleaseHold` (also Java), notifies the requester, and returns `failed` with the reason. No open holds.

**Why it matters:** retries are for outages, not for "no". Error types decide which is which, and the
[saga](https://docs.temporal.io/design-patterns/saga-pattern) undoes what the tool already did.

### The Web UI

Every Temporal-mode row has a **Web UI** link. The Workflow's history shows each Activity with its inputs, results, attempts and
failures, the approval Update, and which Worker (Python or Java) ran each step. That's the audit trail and the
debugger, with no extra code.

## How it works

### The tool call, end to end

1. The agent platform's `fulfill` node calls the MCP tool `fulfill_request` through the
   [LangChain MCP adapters](https://github.com/langchain-ai/langchain-mcp-adapters). See
   [`graph.py`](agent-platform/agent_platform/graph.py).
2. The MCP server, in Temporal mode, starts the `FulfillRequest` Workflow with Workflow Id `fulfill-<request_id>`
   and waits for the result. The request id is the idempotency key. With the
   [Workflow Id conflict policy](https://docs.temporal.io/workflow-execution/workflowid-runid#workflow-id-conflict-policy)
   *use existing*, a repeat call while the run is open attaches to it. With the
   [reuse policy](https://docs.temporal.io/workflow-execution/workflowid-runid#workflow-id-reuse-policy) *reject
   duplicate*, a repeat call after the run closed gets that run's result instead of doing the work again (within the
   Namespace's retention period). If the MCP server restarts mid-call, the Workflow keeps running; calling the tool
   again with the same request id attaches to it. See [`mcp-server/src/temporal.ts`](mcp-server/src/temporal.ts).
3. The Workflow calls the heritage services through Activities, in order. See
   [`workflow.py`](tools/python/durable_tools/workflow.py):
   - `records`: `fetch_records` (Python) on `durable-tools`.
   - `policy`: `check_policy` (Python) on `heritage-policy`, where at most 3 run at once.
   - `hold`: `PlaceHold` (Java) on `heritage-ledger`. From here on, every exit that doesn't commit releases the hold.
   - `approval`: only for `refer`, amounts over 5000, or a policy result of not eligible. Waits for the
     `decide_approval` Update or a timeout.
   - `commit`: `CommitHold` (Java). A 503 retries; a 422 is final and triggers the release.
   - `notify`: `send_notification` (Python).
4. The result goes back through the MCP server to the agent as the tool result.

A `deny` decision only notifies the requester: no hold, no approval, no commit.

### One Workflow across three languages

The Python Workflow calls the Java Activities by name on their own Task Queue:

```python
hold = await workflow.execute_activity(
    "PlaceHold", {"request_id": rid, "amount": amount},
    task_queue="heritage-ledger", retry_policy=LEDGER_RETRY, result_type=dict, **STEP_TIMEOUTS)
```

The Java Worker registers those Activity types and polls `heritage-ledger`. Payloads are JSON in every SDK, so the
contract is the field names ([`docs/CONTRACTS.md`](docs/CONTRACTS.md)). The TypeScript MCP server starts the
Python Workflow by type name the same way. No team imports another team's code. For calls across team
[Namespaces](https://docs.temporal.io/namespaces) with a service contract, see [Nexus](#adopt-it-one-tool-at-a-time).

### Errors decide what happens next

Each Activity turns a heritage response into a result or a typed error:

| Response | Error type | What Temporal does |
|---|---|---|
| 5xx, connection error | `HeritageUnavailable`, `LedgerUnavailable` | Retry with backoff (1 s, 2 s, 4 s, 8 s, then every 10 s), for up to 10 minutes per step |
| 429 | `HeritageBusy` | Retry with backoff |
| 422 from the ledger | `LedgerRejected` (non-retryable) | Stop; the Workflow compensates |
| Other 4xx | `HeritageRejected`, `LedgerNotFound`, `LedgerConflict`, `LedgerBadRequest` (non-retryable) | Stop |

### The demo switch

The MCP server's `direct` mode runs the same steps as plain HTTP calls, in the same order, with no retries and no
waiting. It shows today's behavior next to the durable one, with the same agent and the same
services. Only the line behind the tool interface changes.

## Temporal concepts and patterns used

| Concept or pattern | Where in this repo | Docs |
|---|---|---|
| Durable Execution, Event History | Every tool call's progress | [What is Temporal](https://docs.temporal.io/temporal) · [Events and Event History](https://docs.temporal.io/workflow-execution/event) |
| Workflow | `FulfillRequest`: the thick tool | [Workflows](https://docs.temporal.io/workflows) |
| Activity, Retry Policy, timeouts | One Activity per heritage call | [Activities](https://docs.temporal.io/activities) · [Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies) · [Activity timeouts](https://docs.temporal.io/encyclopedia/detecting-activity-failures) |
| Non-retryable errors | `LedgerRejected`, `HeritageRejected` | [Failures](https://docs.temporal.io/references/failures) |
| Saga (compensation) | `ReleaseHold` after a reject, timeout or hard commit failure | [Saga pattern](https://docs.temporal.io/design-patterns/saga-pattern) |
| Human in the loop: Update with a validator, durable timer | The approval step | [Approval pattern](https://docs.temporal.io/design-patterns/approval) · [Message passing (Python)](https://docs.temporal.io/develop/python/workflows/message-passing) |
| Query | The dashboard's `progress` view | [Message passing (Python)](https://docs.temporal.io/develop/python/workflows/message-passing) |
| Task Queues and Worker concurrency | `heritage-policy` with `max_concurrent_activities=3` | [Task Queues](https://docs.temporal.io/task-queue) · [Downstream Rate Limiting](https://docs.temporal.io/design-patterns/downstream-rate-limiting) |
| Polyglot Workers | Python Workflow, Java Activities, TypeScript Client | [Temporal SDKs](https://docs.temporal.io/encyclopedia/architecture/temporal-sdks) |
| Workflow Id as an idempotency key | `fulfill-<request_id>`, conflict policy *use existing*, reuse policy *reject duplicate* | [Workflow Id conflict and reuse policies](https://docs.temporal.io/workflow-execution/workflowid-runid#workflow-id-conflict-policy) |
| Durable MCP tools | The MCP server starts a Workflow per tool call | [Durable MCP server (cookbook)](https://docs.temporal.io/ai/cookbook/hello-world-durable-mcp-server) |

## What you don't have to build

With a plain tool, each of these is code you write, test and run yourself, in every tool that needs it:

| You'd otherwise build | Here it's |
|---|---|
| Retry loops with backoff and jitter, per downstream call | A Retry Policy on each Activity |
| A table that records which steps finished, so a retry doesn't repeat them | The Workflow's Event History |
| Crash recovery: noticing a dead process and resuming its work | Any Worker replays the history and continues |
| A pending-approvals store, a poller, expiry, and cleanup when nobody answers | `wait_condition` with a timeout, and an Update |
| Two-call tools with state handed between them | One tool call that waits |
| Undo logic for partial work | Compensations registered as the saga runs |
| A queue and worker pool in front of a service that has none | A Task Queue and a Worker concurrency limit |
| Deduplication when a caller retries the same request | The Workflow Id and its conflict policy |
| Status endpoints and audit logs for each tool | A Query, and the Web UI over the Event History (kept for the Namespace's retention period) |

## Adopt it one tool at a time

- **Keep the agent platform.** Nothing in [`agent-platform/`](agent-platform/) knows about Temporal. Agents keep
  calling MCP tools.
- **Start with the tool that hurts.** Pick one composite tool that calls several systems or needs an approval.
  Implement its body as a Workflow and have the tool start it. Other tools stay as they are.
- **Use the language each team already uses.** The tool interface here is TypeScript, the Workflow is Python and the
  ledger team's Activities are Java. Temporal has [official SDKs](https://docs.temporal.io/encyclopedia/architecture/temporal-sdks)
  for many languages, including Go, Java, Python, TypeScript and .NET.
- **Cross team boundaries with Nexus.** When another team owns a system, they can expose it as a
  [Nexus](https://docs.temporal.io/nexus) service from their own Namespace, with an explicit contract, instead of
  sharing code or Task Queues. Nexus is available in the Go, Java, Python, TypeScript, .NET and Rust SDKs; see
  [Evaluate Nexus](https://docs.temporal.io/evaluate/features/nexus).
- **Run it where your data must stay.** The same code runs on a self-hosted Temporal Service or on
  [Temporal Cloud](https://docs.temporal.io/cloud). Workers always run on your infrastructure and connect out to the
  Temporal Service; your heritage systems are only ever called from your Workers.
- **Make the agents durable later, if you want.** The [LangGraph plugin](https://docs.temporal.io/develop/python/integrations/langgraph)
  (Public Preview) runs graph nodes as Activities. That's a separate, optional step.

## Production notes and limits

- **The heritage services are mocks.** They simulate latency, 503s, 422s and a 3-in-flight limit. Real services need
  idempotent writes or idempotency keys, because an Activity can run more than once (at-least-once execution).
  The mock ledger's hold call is idempotent per request id for this reason.
- **Concurrency limits here are per Worker.** `max_concurrent_activities=3` caps one Worker. With several Workers on
  the queue, the total is the sum: keep the sum at or below what the service allows. If the service limits requests
  per second rather than calls in flight, use `max_task_queue_activities_per_second`, which the Temporal Service
  enforces across all Workers ([guide](https://docs.temporal.io/guides/rate-limit-downstream-apis)).
- **The tool call blocks while it waits for a person.** The MCP client here is configured with a 30-minute read
  timeout. For waits longer than a client connection, return the Workflow Id and let the agent check status (the
  `get_fulfillment_status` tool), or let a person's decision resume the agent later. The Workflow itself can wait for
  days either way.
- **If the Temporal Service is unreachable,** the tool returns `temporal_unavailable` and starts no work. Running
  Workflows pause and continue when it's back.
- **Compensation can fail too.** If `ReleaseHold` fails for a reason other than "hold not found", the Workflow
  fails and shows the error in the Web UI, so a stuck undo is visible instead of hidden.
- **What's stored in Temporal.** Activity inputs and results are recorded in Event History. To encrypt them, use a
  [Payload Codec](https://docs.temporal.io/payload-codec). Keep large documents out of payloads and pass
  references (see [Workflow Execution limits](https://docs.temporal.io/workflow-execution/limits)).
- **Versioning.** Changing a running Workflow's code needs care. See
  [Versioning](https://docs.temporal.io/develop/python/workflows/versioning) before you change `FulfillRequest` with runs in
  flight.

## Further reading

Docs and tutorials:

- [Durable MCP weather server (cookbook)](https://docs.temporal.io/ai/cookbook/hello-world-durable-mcp-server): MCP tools as thin wrappers that start Workflows.
- [Building durable MCP tools with Temporal (tutorial, two parts)](https://learn.temporal.io/tutorials/ai/building-mcp-tools-with-temporal/): part 2 adds human in the loop.
- [Rate-limit downstream APIs with separate Task Queues (guide)](https://docs.temporal.io/guides/rate-limit-downstream-apis).
- [Temporal for AI](https://docs.temporal.io/ai): patterns for agents and tools.
- [Develop with AI](https://docs.temporal.io/with-ai): the Temporal Developer skill and docs MCP server for coding agents.

Blog posts:

- [Durable MCP: how to give agentic systems superpowers](https://temporal.io/blog/durable-mcp-how-to-give-agentic-systems-superpowers) (Joshua Smith, July 9, 2025): MCP tools as Workflows.
- [Building long-running interactive MCP tools with Temporal](https://temporal.io/blog/building-long-running-interactive-mcp-tools-temporal) (Joshua Smith, August 21, 2025): tools that wait for people.
- [From AI hype to durable reality: why agentic flows need distributed systems](https://temporal.io/blog/from-ai-hype-to-durable-reality-why-agentic-flows-need-distributed-systems) (Kevin Martin, June 11, 2025): durable tools and back-pressure.
- [The hero's journey to AI durability with Temporal](https://temporal.io/blog/the-heros-journey-to-ai-durability-with-temporal) (Tim Imkin, January 14, 2026): durable tools with MCP.

## Reference: layout, configuration and troubleshooting

### Layout

```
agent-platform/agent_platform/   plain LangGraph agents and their HTTP front door (9300)
mcp-server/                      MCP tool interface, TypeScript (9200)
tools/python/durable_tools/      FulfillRequest Workflow, Python Activities, Worker
heritage/python/heritage_py/     mock records, policy and notify services (9101)
heritage/java/                   mock ledger service (9103) and the Java Worker
dashboard/demo_dashboard/        demo control panel (8095)
datasets/cases/                  synthetic requests
docs/CONTRACTS.md                every interface between components
tests/                           Workflow tests on the time-skipping test server
```

### Configuration

All settings are environment variables with working defaults; see [`.env.example`](.env.example).
Ports: Temporal 7233 (Web UI 8233), heritage-py 9101, heritage-java 9103, MCP 9200, platform 9300, dashboard 8095.

### Troubleshooting

- **A beat button shows an error toast:** run `make status`. Every process must answer.
- **Tool calls stay open in Temporal mode:** the tool Worker or the Java Worker isn't polling. Check
  `logs/tool-worker.log` and `logs/heritage-java.log`, or the Workflow's pending Activities in the Web UI.
- **Numbers from the last beat are still on screen:** every beat button resets the demo first. **Reset** clears it
  without starting a beat.
- **A tool call returns `failed` after a reset:** a reset clears the ledger, so a commit that was still waiting finds
  no hold. Every beat button resets, so finish an open approval before you start the next beat.
- **Port in use:** another process holds one of the ports above. Stop it, or change the port variables.
