# heritage-java

One Java 21 process with two parts:

- A mock heritage ledger: an HTTP API on port 9103 with in-memory holds, fault switches and call stats.
- A Temporal Worker on the Task Queue `heritage-ledger` that hosts the ledger Activities. Each Activity calls
  the HTTP API above, so a Workflow and a direct caller hit the same ledger.

The contract is in [`docs/CONTRACTS.md`](../../docs/CONTRACTS.md).

## Run

You need Java 21 and a Temporal Service (for example `temporal server start-dev`).

```bash
./gradlew -q installDist && build/install/heritage-java/bin/heritage-java
```

`make heritage-java` from the repo root does the same. Use the installed launcher rather than `./gradlew run`:
Ctrl-C reaches the app directly, so it shuts down the Worker cleanly.

You see two startup lines:

```
Heritage ledger HTTP API listening on http://localhost:9103 (latency ~150 ms per call)
Java ledger Worker polling 'heritage-ledger' on localhost:7233 (namespace=default)
```

If Temporal is not up yet, the ledger API still starts and the Worker waits for it. Press Ctrl-C to stop.

Run the tests with `./gradlew test`.

| Variable | Default | Use |
|---|---|---|
| `TEMPORAL_ADDRESS` | `localhost:7233` | Temporal frontend |
| `TEMPORAL_NAMESPACE` | `default` | Namespace |
| `TEMPORAL_API_KEY` | unset | If set: TLS on and API key auth, for Temporal Cloud |
| `HERITAGE_JAVA_URL` | `http://localhost:9103` | Ledger URL the Activities call |
| `HERITAGE_JAVA_PORT` | `9103` | Port for the ledger HTTP API |
| `LEDGER_LATENCY_MS` | `150` | Simulated latency per ledger call |

## Activities

Task Queue `heritage-ledger`. Each Activity takes one JSON object and returns one JSON object.

| Activity type | Input | Output |
|---|---|---|
| `PlaceHold` | `{"request_id", "amount"}` | `{"hold_id", "status": "held"}` |
| `CommitHold` | `{"hold_id"}` | `{"hold_id", "status": "committed"}` |
| `ReleaseHold` | `{"hold_id"}` | `{"hold_id", "status": "released"}` |

HTTP results map to Temporal failures:

| Ledger response | Failure type | Retryable |
|---|---|---|
| 503, other 5xx, connection error | `LedgerUnavailable` | Yes |
| 422 | `LedgerRejected` | No |
| 404 | `LedgerNotFound` | No |
| 409 | `LedgerConflict` | No |
| Other 4xx | `LedgerBadRequest` | No |

Call them from a Python Workflow by type name:

```python
hold = await workflow.execute_activity(
    "PlaceHold", {"request_id": "t1", "amount": 10},
    task_queue="heritage-ledger", start_to_close_timeout=timedelta(seconds=30),
)
```

## Ledger API

| Method and path | Body | Notes |
|---|---|---|
| `POST /ledger/hold` | `{"request_id", "amount"}` | Returns the open hold for that `request_id` if there is one. `hold_id` is `H-<request_id>-<n>` |
| `POST /ledger/commit` | `{"hold_id"}` | 404 unknown hold, 409 if released. Committing twice returns 200 |
| `POST /ledger/release` | `{"hold_id"}` | 404 unknown hold, 409 if committed. Releasing twice returns 200 |
| `GET /ledger/holds` | | All holds |
| `GET /admin/stats` | | Per-endpoint `{calls, ok, errors, rejected_429, in_flight, max_in_flight}`, plus `open_holds`, `committed`, `released` |
| `POST /admin/faults` | `{"service", "mode", "count", "delay_ms"}` | Fault switches, below. `GET` shows the current switches |
| `POST /admin/reset` | | Clears holds, faults and stats |

## Fault switches

`service` is `hold`, `commit` or `release`.

| `mode` | Effect |
|---|---|
| `fail_next` | The next `count` calls (default 1) return 503 |
| `reject_next` | The next `count` calls (default 1) return 422, a business rejection |
| `slow` | Adds `delay_ms` to every call until cleared |
| `clear` | Clears all switches for that service |

```bash
curl -s -XPOST localhost:9103/admin/faults -d '{"service":"commit","mode":"reject_next","count":1}'
```

A faulted call changes no ledger state.

## Cross-language smoke test

`scripts/xlang_smoke.py` runs a small Python Workflow that calls `PlaceHold` and `CommitHold`, shows a
503 being retried, and shows a 422 surfacing as a non-retryable `LedgerRejected` failure followed by
`ReleaseHold`. It resets the ledger first. It needs the `temporalio` package and the app running:

```bash
TEMPORAL_ADDRESS=localhost:7233 python scripts/xlang_smoke.py
```
