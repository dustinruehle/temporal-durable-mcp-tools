// Direct mode: today's way. Plain HTTP calls to the heritage services, in order,
// with no retries and no compensation. A failure part way leaves earlier side
// effects (for example an open ledger hold) in place. That is on purpose.
import { config } from './config.js';
import { STEP_NAMES, type FulfillArgs, type StepName, type StepState, type ToolOutcome } from './types.js';

export const APPROVAL_REQUIRED_MESSAGE =
  'approval required: this tool must end here and a second tool call must finish later';

/** Last known status per request, shaped like the Workflow's progress() Query result. */
interface DirectStatus {
  request_id: string;
  mode: 'direct';
  stage: StepName | 'done';
  steps: Record<StepName, StepState>;
  awaiting_approval: boolean;
  approval_deadline: null;
  hold_id: string | null;
  outcome: string | null;
  attempts: Partial<Record<StepName, number>>;
  error: string | null;
  updated_at: string;
}

const statuses = new Map<string, DirectStatus>();

export function directStatus(requestId: string): DirectStatus | undefined {
  return statuses.get(requestId);
}

export function resetDirectStatuses(): void {
  statuses.clear();
}

class StepError extends Error {
  constructor(readonly step: StepName, message: string) {
    super(message);
  }
}

function connectErrorCode(err: unknown): string {
  const cause = (err as { cause?: any })?.cause;
  return cause?.code ?? cause?.errors?.[0]?.code ?? cause?.message ?? (err as Error)?.message ?? String(err);
}

async function post(step: StepName, baseUrl: string, path: string, body: unknown): Promise<any> {
  const url = `${baseUrl}${path}`;
  let res: Response;
  try {
    res = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(config.directHttpTimeoutMs),
    });
  } catch (err) {
    const reason = err instanceof Error && err.name === 'TimeoutError'
      ? `timed out after ${config.directHttpTimeoutMs} ms`
      : `could not connect to ${baseUrl} (${connectErrorCode(err)})`;
    throw new StepError(step, `${step} step failed: POST ${path} ${reason}`);
  }
  const text = await res.text();
  if (!res.ok) {
    let detail = text;
    try {
      const parsed = JSON.parse(text);
      if (parsed && typeof parsed.error === 'string') detail = parsed.error;
    } catch {
      /* keep raw text */
    }
    throw new StepError(step, `${step} step failed: POST ${path} returned ${res.status}${detail ? `: ${detail}` : ''}`);
  }
  try {
    return text ? JSON.parse(text) : {};
  } catch {
    throw new StepError(step, `${step} step failed: POST ${path} returned a body that is not JSON`);
  }
}

export async function fulfillDirect(args: FulfillArgs): Promise<ToolOutcome> {
  const { request_id, decision, amount } = args;
  const steps = Object.fromEntries(STEP_NAMES.map((s) => [s, 'pending'])) as Record<StepName, StepState>;
  const status: DirectStatus = {
    request_id,
    mode: 'direct',
    stage: 'records',
    steps,
    awaiting_approval: false,
    approval_deadline: null,
    hold_id: null,
    outcome: null,
    attempts: {},
    error: null,
    updated_at: new Date().toISOString(),
  };
  statuses.set(request_id, status);

  const run = async <T>(step: StepName, fn: () => Promise<T>): Promise<T> => {
    status.stage = step;
    steps[step] = 'running';
    status.attempts[step] = (status.attempts[step] ?? 0) + 1;
    status.updated_at = new Date().toISOString();
    const value = await fn();
    steps[step] = 'done';
    status.updated_at = new Date().toISOString();
    return value;
  };
  const skip = (...names: StepName[]) => names.forEach((n) => (steps[n] = 'skipped'));

  try {
    if (decision === 'deny') {
      // Same as the Workflow: a denial changes nothing in the ledger, it only tells the requester.
      skip('records', 'policy', 'hold', 'approval', 'commit');
      const message = `Request ${request_id} was denied: ${args.reason}`;
      await run('notify', () =>
        post('notify', config.heritagePyUrl, '/notify/send', { request_id, outcome: 'denied', message }),
      );
      return finish(status, 'denied', message);
    }

    await run('records', () => post('records', config.heritagePyUrl, '/records/fetch', { request_id }));
    const policy = await run('policy', () => post('policy', config.heritagePyUrl, '/policy/check', { request_id, amount }));

    const hold = await run('hold', () => post('hold', config.heritageJavaUrl, '/ledger/hold', { request_id, amount }));
    status.hold_id = typeof hold?.hold_id === 'string' ? hold.hold_id : null;

    if (decision === 'refer' || amount > config.approvalThreshold || policy?.eligible === false) {
      // There is nothing here that can wait for a person. The tool call ends and the hold stays open.
      status.stage = 'approval';
      steps.approval = 'pending';
      status.error = APPROVAL_REQUIRED_MESSAGE;
      status.updated_at = new Date().toISOString();
      return {
        ok: false,
        outcome: 'approval_required',
        body: `${APPROVAL_REQUIRED_MESSAGE} (hold ${status.hold_id ?? 'unknown'} is still open)`,
      };
    }
    skip('approval');

    await run('commit', () => post('commit', config.heritageJavaUrl, '/ledger/commit', { hold_id: status.hold_id }));
    const message = `Request ${request_id} was fulfilled for ${amount}`;
    await run('notify', () =>
      post('notify', config.heritagePyUrl, '/notify/send', { request_id, outcome: 'fulfilled', message }),
    );
    return finish(status, 'fulfilled', message);
  } catch (err) {
    const step = err instanceof StepError ? err.step : (status.stage as StepName);
    steps[step] = 'failed';
    status.outcome = 'failed';
    const leftovers = STEP_NAMES.filter((s) => steps[s] === 'done');
    let message = err instanceof Error ? err.message : String(err);
    if (leftovers.length > 0) {
      message += `. Earlier steps are not undone: ${leftovers.join(', ')}`;
      if (status.hold_id && steps.commit !== 'done') message += ` (hold ${status.hold_id} is still open)`;
    }
    status.error = message;
    status.updated_at = new Date().toISOString();
    return { ok: false, outcome: `failed@${step}`, body: message };
  }
}

function finish(status: DirectStatus, outcome: 'fulfilled' | 'denied', message: string): ToolOutcome {
  status.stage = 'done';
  status.outcome = outcome;
  status.updated_at = new Date().toISOString();
  return {
    ok: true,
    outcome,
    body: {
      request_id: status.request_id,
      outcome,
      hold_id: status.hold_id,
      steps: { ...status.steps },
      approved_by: null,
      message,
    },
  };
}
