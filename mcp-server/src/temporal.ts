// Temporal mode: each tool call starts (or attaches to) a FulfillRequest Workflow.
// The Workflow is defined and run by a separate Worker; it is referenced by type name only.
import {
  Client,
  Connection,
  WorkflowExecutionAlreadyStartedError,
  WorkflowFailedError,
  WorkflowNotFoundError,
  type WorkflowHandle,
} from '@temporalio/client';
import { config } from './config.js';
import type { CallContext, FulfillArgs, ToolOutcome } from './types.js';

let clientPromise: Promise<Client> | undefined;

function getClient(): Promise<Client> {
  if (!clientPromise) {
    clientPromise = (async () => {
      const apiKey = config.temporalApiKey;
      const connection = await Connection.connect({
        address: config.temporalAddress,
        ...(apiKey ? { tls: true, apiKey } : {}),
      });
      return new Client({ connection, namespace: config.temporalNamespace });
    })();
    // Do not cache a failed connection attempt: the next call tries again.
    clientPromise.catch(() => (clientPromise = undefined));
  }
  return clientPromise;
}

export function workflowIdFor(requestId: string): string {
  return `fulfill-${requestId}`;
}

function failureMessage(err: unknown): string {
  if (err instanceof WorkflowFailedError) {
    // Walk to the innermost cause: that is the message the Workflow or Activity raised.
    const chain: string[] = [];
    let cur: unknown = err.cause;
    while (cur instanceof Error) {
      if (cur.message) chain.push(cur.message);
      cur = (cur as { cause?: unknown }).cause;
    }
    return chain.length > 0 ? `workflow failed: ${chain.join(': ')}` : `workflow failed: ${err.message}`;
  }
  return err instanceof Error ? err.message : String(err);
}

export async function fulfillTemporal(args: FulfillArgs, ctx: CallContext): Promise<ToolOutcome> {
  const workflowId = workflowIdFor(args.request_id);
  let client: Client;
  try {
    client = await getClient();
  } catch (err) {
    return { ok: false, outcome: 'temporal_unavailable', body: `cannot reach Temporal at ${config.temporalAddress}: ${failureMessage(err)}` };
  }

  let handle: WorkflowHandle;
  try {
    handle = await client.workflow.start(config.workflowType, {
      taskQueue: config.taskQueue,
      workflowId,
      // The request id is the idempotency key. A repeated call while the run is open attaches to it
      // (conflict policy); a repeated call after it closed gets that run's result (reuse policy, below).
      workflowIdConflictPolicy: 'USE_EXISTING',
      workflowIdReusePolicy: 'REJECT_DUPLICATE',
      args: [
        {
          request_id: args.request_id,
          decision: args.decision,
          amount: args.amount,
          reason: args.reason,
          approval_timeout_s: config.approvalTimeoutS,
        },
      ],
    });
  } catch (err) {
    if (!(err instanceof WorkflowExecutionAlreadyStartedError)) {
      return { ok: false, outcome: 'start_failed', body: `could not start ${workflowId}: ${failureMessage(err)}` };
    }
    // This request already ran to completion: answer with that run's result instead of doing the work again.
    handle = client.workflow.getHandle(workflowId);
  }

  // While waiting (a human approval can take minutes), report the Workflow's stage.
  let timer: ReturnType<typeof setInterval> | undefined;
  if (ctx.reportProgress) {
    const report = ctx.reportProgress;
    timer = setInterval(() => {
      handle
        .query<{ stage?: string; awaiting_approval?: boolean }>('progress')
        .then((p) => report(`stage=${p?.stage ?? 'unknown'}${p?.awaiting_approval ? ' (waiting for approval)' : ''}`))
        .catch(() => report('waiting for the workflow'));
    }, config.progressIntervalMs);
  }

  // If the client goes away, stop waiting. The Workflow keeps running; a later call attaches to it.
  let onAbort: (() => void) | undefined;
  const aborted = new Promise<'aborted'>((resolve) => {
    onAbort = () => resolve('aborted');
    if (ctx.signal.aborted) resolve('aborted');
    else ctx.signal.addEventListener('abort', onAbort, { once: true });
  });

  try {
    const result = await Promise.race([handle.result(), aborted]);
    if (result === 'aborted') {
      return { ok: false, outcome: 'client_gone', body: `client disconnected; ${workflowId} keeps running` };
    }
    const outcome = (result as { outcome?: string } | null)?.outcome ?? 'completed';
    return { ok: true, outcome, body: result };
  } catch (err) {
    return { ok: false, outcome: 'workflow_failed', body: failureMessage(err) };
  } finally {
    if (timer) clearInterval(timer);
    if (onAbort) ctx.signal.removeEventListener('abort', onAbort);
  }
}

export async function statusTemporal(requestId: string): Promise<ToolOutcome> {
  const workflowId = workflowIdFor(requestId);
  try {
    const client = await getClient();
    const progress = await client.workflow.getHandle(workflowId).query('progress');
    const stage = (progress as { stage?: string } | null)?.stage ?? 'unknown';
    return { ok: true, outcome: `stage=${stage}`, body: progress };
  } catch (err) {
    if (err instanceof WorkflowNotFoundError) {
      return { ok: false, outcome: 'not_found', body: `no fulfillment found for request_id "${requestId}"` };
    }
    return { ok: false, outcome: 'query_failed', body: `status query for ${workflowId} failed: ${failureMessage(err)}` };
  }
}
