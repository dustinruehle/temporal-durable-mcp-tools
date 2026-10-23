// Throwaway fake heritage services for testing direct mode. Canned responses only.
// One process, two ports: FAKE_PY_PORT (records, policy, notify) and FAKE_JAVA_PORT (ledger).
// POST /admin/faults {"service", "mode": "fail_next"|"clear", "count"} makes the next calls return 503.
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';

const pyPort = Number(process.env.FAKE_PY_PORT ?? 9601);
const javaPort = Number(process.env.FAKE_JAVA_PORT ?? 9603);

const failNext: Record<string, number> = {};
const holds = new Map<string, { hold_id: string; request_id: string; amount: number; status: string }>();
const calls: Record<string, number> = {};

const routes: Record<string, string> = {
  '/records/fetch': 'records',
  '/policy/check': 'policy',
  '/notify/send': 'notify',
  '/ledger/hold': 'hold',
  '/ledger/commit': 'commit',
};

function readBody(req: IncomingMessage): Promise<any> {
  return new Promise((resolve) => {
    let data = '';
    req.on('data', (c) => (data += c));
    req.on('end', () => {
      try {
        resolve(data ? JSON.parse(data) : {});
      } catch {
        resolve({});
      }
    });
  });
}

function send(res: ServerResponse, status: number, body: unknown) {
  res.writeHead(status, { 'content-type': 'application/json' }).end(JSON.stringify(body));
}

async function handle(req: IncomingMessage, res: ServerResponse) {
  const url = req.url ?? '/';
  const body = req.method === 'POST' ? await readBody(req) : {};

  if (url === '/admin/faults') {
    if (body.mode === 'clear') delete failNext[body.service];
    else failNext[body.service] = body.count ?? 1;
    return send(res, 200, { ok: true, faults: failNext });
  }
  if (url === '/admin/stats') {
    const open = [...holds.values()].filter((h) => h.status === 'held').length;
    return send(res, 200, { calls, open_holds: open, holds: [...holds.values()] });
  }
  if (url === '/admin/reset') {
    holds.clear();
    for (const k of Object.keys(failNext)) delete failNext[k];
    for (const k of Object.keys(calls)) delete calls[k];
    return send(res, 200, { ok: true });
  }

  const service = routes[url];
  if (!service) return send(res, 404, { error: `no route ${url}` });
  calls[service] = (calls[service] ?? 0) + 1;
  if ((failNext[service] ?? 0) > 0) {
    failNext[service]--;
    return send(res, 503, { error: `${service} service unavailable` });
  }

  switch (service) {
    case 'records':
      return send(res, 200, {
        request_id: body.request_id,
        requester: 'Sample Requester',
        amount: 1200,
        criteria_met: ['a', 'b'],
        criteria_total: 3,
        documents: ['form.pdf'],
      });
    case 'policy':
      return send(res, 200, { request_id: body.request_id, eligible: true, criteria_met: 2, criteria_total: 3 });
    case 'notify':
      return send(res, 200, { request_id: body.request_id, sent: true });
    case 'hold': {
      const existing = [...holds.values()].find((h) => h.request_id === body.request_id && h.status === 'held');
      if (existing) return send(res, 200, { hold_id: existing.hold_id, status: 'held' });
      const hold_id = `H-${body.request_id}-${holds.size + 1}`;
      holds.set(hold_id, { hold_id, request_id: body.request_id, amount: body.amount, status: 'held' });
      return send(res, 200, { hold_id, status: 'held' });
    }
    case 'commit': {
      const h = holds.get(body.hold_id);
      if (!h) return send(res, 404, { error: 'unknown hold' });
      h.status = 'committed';
      return send(res, 200, { hold_id: h.hold_id, status: 'committed' });
    }
  }
}

for (const port of [pyPort, javaPort]) {
  createServer((req, res) => void handle(req, res)).listen(port, '127.0.0.1', () =>
    console.log(`fake heritage on http://localhost:${port}`),
  );
}
