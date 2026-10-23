// Smoke test with a real MCP client. Usage:
//   tsx scripts/smoke.ts temporal   (needs a Temporal server and scripts/stub_worker.py)
//   tsx scripts/smoke.ts direct     (needs scripts/fake_heritage.ts; FAKE_PY_URL / FAKE_JAVA_URL)
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';

const MCP_URL = process.env.MCP_URL ?? 'http://localhost:9200/mcp';
const ADMIN = MCP_URL.replace(/\/mcp$/, '');
const FAKE_PY = process.env.FAKE_PY_URL ?? 'http://localhost:9601';
const FAKE_JAVA = process.env.FAKE_JAVA_URL ?? 'http://localhost:9603';

async function connect(name: string): Promise<Client> {
  const client = new Client({ name, version: '0.0.1' });
  await client.connect(new StreamableHTTPClientTransport(new URL(MCP_URL)));
  return client;
}

async function call(client: Client, tool: string, args: Record<string, unknown>, label: string) {
  const t0 = Date.now();
  const res = await client.callTool({ name: tool, arguments: args }, undefined, {
    timeout: 15 * 60_000,
    resetTimeoutOnProgress: true,
    onprogress: (p) => console.log(`  [${label}] progress ${p.progress}: ${p.message ?? ''}`),
  });
  const text = (res.content as Array<{ type: string; text?: string }>)
    .map((c) => c.text ?? '')
    .join('\n');
  console.log(`[${label}] ${tool} isError=${res.isError ? 'true' : 'false'} ms=${Date.now() - t0}`);
  console.log(text.split('\n').map((l) => `    ${l}`).join('\n'));
  return { res, text };
}

async function postJson(url: string, body: unknown) {
  const r = await fetch(url, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
  return r.json();
}

async function temporal() {
  console.log('mode:', JSON.stringify(await postJson(`${ADMIN}/admin/mode`, { mode: 'temporal' })));
  const a = await connect('smoke-a');
  const b = await connect('smoke-b');
  const tools = await a.listTools();
  console.log('tools:', tools.tools.map((t) => t.name).join(', '));

  await call(a, 'fulfill_request', { request_id: 'approve', decision: 'approve', amount: 1200, reason: 'all criteria met' }, 'single');

  const id = `dup-${Date.now()}`;
  console.log(`two concurrent calls, request_id=${id}`);
  const p1 = call(a, 'fulfill_request', { request_id: id, decision: 'approve', amount: 900, reason: 'first' }, 'call-1');
  await new Promise((r) => setTimeout(r, 300));
  const status = call(b, 'get_fulfillment_status', { request_id: id }, 'status');
  const p2 = call(b, 'fulfill_request', { request_id: id, decision: 'approve', amount: 900, reason: 'second' }, 'call-2');
  await Promise.all([p1, p2, status]);
  console.log(`DUP_ID=${id}`);

  await call(a, 'fulfill_request', { request_id: 'fail-1', decision: 'approve', amount: 10, reason: 'x' }, 'failing');
  await call(a, 'get_fulfillment_status', { request_id: 'never-started' }, 'unknown');
  console.log('stats:', JSON.stringify(await (await fetch(`${ADMIN}/admin/stats`)).json()));
  await a.close();
  await b.close();
}

async function direct() {
  console.log('mode:', JSON.stringify(await postJson(`${ADMIN}/admin/mode`, { mode: 'direct' })));
  await postJson(`${FAKE_PY}/admin/reset`, {});
  const c = await connect('smoke-direct');

  console.log('--- success');
  await call(c, 'fulfill_request', { request_id: 'approve', decision: 'approve', amount: 1200, reason: 'all criteria met' }, 'approve');

  console.log('--- deny');
  await call(c, 'fulfill_request', { request_id: 'deny', decision: 'deny', amount: 300, reason: 'criteria not met' }, 'deny');

  console.log('--- 503 on commit (after the hold was placed)');
  await postJson(`${FAKE_JAVA}/admin/faults`, { service: 'commit', mode: 'fail_next', count: 1 });
  await call(c, 'fulfill_request', { request_id: 'burst-01', decision: 'approve', amount: 800, reason: 'ok' }, 'fault');
  await call(c, 'get_fulfillment_status', { request_id: 'burst-01' }, 'fault-status');

  console.log('--- 503 on policy');
  await postJson(`${FAKE_PY}/admin/faults`, { service: 'policy', mode: 'fail_next', count: 1 });
  await call(c, 'fulfill_request', { request_id: 'burst-02', decision: 'approve', amount: 800, reason: 'ok' }, 'fault-policy');

  console.log('--- approval required');
  await call(c, 'fulfill_request', { request_id: 'refer', decision: 'refer', amount: 8000, reason: 'needs review' }, 'refer');

  console.log('heritage:', JSON.stringify(await (await fetch(`${FAKE_JAVA}/admin/stats`)).json()));
  console.log('stats:', JSON.stringify(await (await fetch(`${ADMIN}/admin/stats`)).json()));
  await c.close();
}

const which = process.argv[2];
(which === 'direct' ? direct() : which === 'temporal' ? temporal() : Promise.reject(new Error('usage: smoke.ts temporal|direct'))).catch(
  (err) => {
    console.error(err);
    process.exit(1);
  },
);
