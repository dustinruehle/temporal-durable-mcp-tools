export type ToolMode = 'direct' | 'temporal';

export const MODES: readonly ToolMode[] = ['direct', 'temporal'];

export function isMode(value: unknown): value is ToolMode {
  return typeof value === 'string' && (MODES as readonly string[]).includes(value);
}

function intFromEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === '') return fallback;
  const n = Number.parseInt(raw, 10);
  if (!Number.isFinite(n)) throw new Error(`${name} must be an integer, got "${raw}"`);
  return n;
}

function initialMode(): ToolMode {
  const raw = process.env.TOOL_MODE;
  if (raw === undefined || raw.trim() === '') return 'temporal';
  if (!isMode(raw)) throw new Error(`TOOL_MODE must be "direct" or "temporal", got "${raw}"`);
  return raw;
}

const stripSlash = (url: string) => url.replace(/\/+$/, '');

export const config = {
  port: intFromEnv('MCP_PORT', 9200),
  host: process.env.MCP_HOST ?? '127.0.0.1',
  initialMode: initialMode(),

  temporalAddress: process.env.TEMPORAL_ADDRESS ?? 'localhost:7233',
  temporalNamespace: process.env.TEMPORAL_NAMESPACE ?? 'default',
  // Read once; never logged.
  temporalApiKey: process.env.TEMPORAL_API_KEY || undefined,
  taskQueue: 'durable-tools',
  workflowType: 'FulfillRequest',
  approvalTimeoutS: intFromEnv('APPROVAL_TIMEOUT_S', 300),

  heritagePyUrl: stripSlash(process.env.HERITAGE_PY_URL ?? 'http://localhost:9101'),
  heritageJavaUrl: stripSlash(process.env.HERITAGE_JAVA_URL ?? 'http://localhost:9103'),
  // Per-call timeout for direct-mode HTTP calls. A timeout is a failure; there are no retries.
  directHttpTimeoutMs: intFromEnv('DIRECT_HTTP_TIMEOUT_MS', 30_000),

  // Amounts above this (strictly greater) need a human approval.
  approvalThreshold: 5000,

  // How often a long tool call reports progress (when the client asked for it).
  progressIntervalMs: intFromEnv('PROGRESS_INTERVAL_MS', 5_000),
  // SSE comment frames on open response streams, so idle timeouts do not cut long calls.
  sseKeepAliveMs: intFromEnv('SSE_KEEPALIVE_MS', 15_000),
} as const;
