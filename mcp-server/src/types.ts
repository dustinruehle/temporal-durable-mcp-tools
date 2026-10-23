export type Decision = 'approve' | 'refer' | 'deny';

export interface FulfillArgs {
  request_id: string;
  decision: Decision;
  amount: number;
  reason: string;
}

export type StepName = 'records' | 'policy' | 'hold' | 'approval' | 'commit' | 'notify';
export type StepState = 'pending' | 'running' | 'done' | 'skipped' | 'failed' | 'compensated';
export const STEP_NAMES: readonly StepName[] = ['records', 'policy', 'hold', 'approval', 'commit', 'notify'];

/** What the tool returns as text content. */
export interface ToolOutcome {
  ok: boolean;
  /** Short label for the log line. */
  outcome: string;
  /** JSON-serializable body (ok) or error message (not ok). */
  body: unknown;
}

/** Provided by the MCP layer so a long call can report progress and notice a client that left. */
export interface CallContext {
  signal: AbortSignal;
  reportProgress?: (message: string) => Promise<void>;
}
