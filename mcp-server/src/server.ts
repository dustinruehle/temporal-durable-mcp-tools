// MCP tool server. Exposes the fulfillment tools over Streamable HTTP at /mcp and runs
// each call in the current mode: "direct" (plain HTTP calls) or "temporal" (a Workflow).
import type { Request, Response } from 'express';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { createMcpExpressApp } from '@modelcontextprotocol/sdk/server/express.js';
import { z } from 'zod';
import { config, isMode, type ToolMode } from './config.js';
import { directStatus, fulfillDirect, resetDirectStatuses } from './direct.js';
import { fulfillTemporal, statusTemporal } from './temporal.js';
import type { CallContext, FulfillArgs, ToolOutcome } from './types.js';

let mode: ToolMode = config.initialMode;

const stats = { calls: 0, ok: 0, errors: 0, in_flight: 0, status_calls: 0 };

function log(line: string): void {
  console.log(`${new Date().toISOString()} ${line}`);
}

function toToolResult(out: ToolOutcome) {
  const text = typeof out.body === 'string' ? out.body : JSON.stringify(out.body, null, 2);
  return { content: [{ type: 'text' as const, text }], ...(out.ok ? {} : { isError: true }) };
}

const FULFILL_DESCRIPTION = [
  'Carry out a decision you already made on a request: fetch its records, check policy,',
  'place a ledger hold, commit the hold and notify the requester.',
  'Call it once per request, after you decide.',
  'decision "approve" fulfills the request; "refer" sends it to a human approver; "deny" only notifies the requester.',
  'Any request with amount above 5000 needs a human approval, whatever the decision.',
  'When a human approval is needed, this call can stay open for several minutes while it waits. Do not cancel it.',
  'Returns the result as JSON: request_id, outcome (fulfilled, denied, rejected, timed_out or failed), hold_id, steps, approved_by and message.',
  'On failure it returns an error whose message names the step that failed.',
  'To check progress without waiting, call get_fulfillment_status with the same request_id.',
].join(' ');

const STATUS_DESCRIPTION = [
  'Look up the current status of a fulfillment started with fulfill_request, without waiting for it to finish.',
  'Returns JSON: request_id, stage, the state of each step (records, policy, hold, approval, commit, notify),',
  'awaiting_approval, approval_deadline, hold_id, outcome and attempts per step.',
  'Returns an error if no fulfillment is known for the request_id.',
].join(' ');

function buildMcpServer(): McpServer {
  const server = new McpServer({ name: 'durable-tools', version: '0.1.0' }, { capabilities: { logging: {} } });

  server.registerTool(
    'fulfill_request',
    {
      title: 'Fulfill request',
      description: FULFILL_DESCRIPTION,
      inputSchema: {
        request_id: z.string().min(1).describe('The request id, for example "approve" or "burst-07".'),
        decision: z.enum(['approve', 'refer', 'deny']).describe('Your decision on the request.'),
        amount: z.number().nonnegative().describe('The amount to fulfill, in the request currency.'),
        reason: z.string().describe('One or two sentences on why you made this decision.'),
      },
    },
    async (args, extra) => {
      const callMode = mode;
      const started = Date.now();
      stats.calls++;
      stats.in_flight++;

      const progressToken = extra._meta?.progressToken;
      let tick = 0;
      const ctx: CallContext = {
        signal: extra.signal,
        reportProgress:
          progressToken === undefined
            ? undefined
            : async (message: string) => {
                tick++;
                try {
                  await extra.sendNotification({
                    method: 'notifications/progress',
                    params: { progressToken, progress: tick, message },
                  });
                } catch {
                  /* stream already closed */
                }
              },
      };

      let out: ToolOutcome;
      try {
        out =
          callMode === 'temporal'
            ? await fulfillTemporal(args as FulfillArgs, ctx)
            : await fulfillDirect(args as FulfillArgs);
      } catch (err) {
        out = { ok: false, outcome: 'internal_error', body: `internal error: ${(err as Error)?.message ?? err}` };
      } finally {
        stats.in_flight--;
      }
      if (out.ok) stats.ok++;
      else stats.errors++;
      log(
        `tool=fulfill_request mode=${callMode} request_id=${args.request_id} decision=${args.decision} ` +
          `outcome=${out.outcome} ms=${Date.now() - started}`,
      );
      return toToolResult(out);
    },
  );

  server.registerTool(
    'get_fulfillment_status',
    {
      title: 'Get fulfillment status',
      description: STATUS_DESCRIPTION,
      inputSchema: {
        request_id: z.string().min(1).describe('The request id you passed to fulfill_request.'),
      },
      annotations: { readOnlyHint: true },
    },
    async ({ request_id }) => {
      const callMode = mode;
      stats.status_calls++;
      let out: ToolOutcome;
      if (callMode === 'temporal') {
        out = await statusTemporal(request_id);
      } else {
        const s = directStatus(request_id);
        out = s
          ? { ok: true, outcome: `stage=${s.stage}`, body: s }
          : { ok: false, outcome: 'not_found', body: `no fulfillment found for request_id "${request_id}"` };
      }
      log(`tool=get_fulfillment_status mode=${callMode} request_id=${request_id} outcome=${out.outcome}`);
      return toToolResult(out);
    },
  );

  return server;
}

const app = createMcpExpressApp({ host: config.host });

// Stateless Streamable HTTP: a fresh server and transport per POST.
app.post('/mcp', async (req: Request, res: Response) => {
  const server = buildMcpServer();
  const transport = new StreamableHTTPServerTransport({
    sessionIdGenerator: undefined,
    keepAliveMs: config.sseKeepAliveMs,
  });
  res.on('close', () => {
    void transport.close();
    void server.close();
  });
  try {
    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
  } catch (err) {
    log(`mcp request error: ${(err as Error)?.message ?? err}`);
    if (!res.headersSent) {
      res.status(500).json({ jsonrpc: '2.0', error: { code: -32603, message: 'Internal server error' }, id: null });
    }
  }
});

const methodNotAllowed = (_req: Request, res: Response) => {
  res.status(405).json({ jsonrpc: '2.0', error: { code: -32000, message: 'Method not allowed.' }, id: null });
};
app.get('/mcp', methodNotAllowed);
app.delete('/mcp', methodNotAllowed);

app.get('/healthz', (_req: Request, res: Response) => {
  res.json({ ok: true, mode });
});

app.get('/admin/mode', (_req: Request, res: Response) => {
  res.json({ mode });
});

app.post('/admin/mode', (req: Request, res: Response) => {
  const next = req.body?.mode;
  if (!isMode(next)) {
    res.status(400).json({ error: 'mode must be "direct" or "temporal"' });
    return;
  }
  if (next !== mode) log(`mode ${mode} -> ${next}`);
  mode = next;
  res.json({ mode });
});

app.get('/admin/stats', (_req: Request, res: Response) => {
  res.json({ mode, ...stats });
});

// Clears counters and direct-mode status. In-flight calls are not affected.
app.post('/admin/reset', (_req: Request, res: Response) => {
  stats.calls = stats.ok = stats.errors = stats.status_calls = 0;
  resetDirectStatuses();
  res.json({ ok: true });
});

const httpServer = app.listen(config.port, config.host, (err?: Error) => {
  if (err) {
    console.error(`cannot listen on ${config.host}:${config.port}: ${err.message}`);
    process.exit(1);
  }
  log(`MCP tool server on http://localhost:${config.port}/mcp (mode=${mode})`);
});

// A tool call can wait minutes for a human approval. Do not let Node cut it.
httpServer.requestTimeout = 0; // no limit on the whole request/response
httpServer.timeout = 0; // no socket inactivity timeout (the SSE stream also sends keep-alive frames)
httpServer.keepAliveTimeout = 65_000; // idle keep-alive sockets between requests
httpServer.headersTimeout = 66_000; // must exceed keepAliveTimeout

const shutdown = () => {
  log('shutting down');
  httpServer.close();
  httpServer.closeAllConnections();
  process.exit(0);
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
