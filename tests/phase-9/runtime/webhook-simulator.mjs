// Phase 9 — Webhook Simulator (transport only).
//
// The simulator owns TRANSPORT. It does not contain business logic.
// It accepts a payload via HTTP POST, optionally rewrites it for
// chaos testing, and returns the body that will be injected into
// the runtime's real ingress. The runtime parses, governs, FSMs,
// dispatches workers, mutates state, and emits bus events. The
// simulator never sees any of that.

import http from 'node:http';
import path from 'node:path';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const CANONICAL_DIR = path.join(__dirname, '..', 'fixtures', 'webhooks', 'canonical');

const FIXTURES = {};
for (const f of fs.readdirSync(CANONICAL_DIR)) {
  if (f.endsWith('.json')) {
    FIXTURES[f.replace('.json', '')] = JSON.parse(fs.readFileSync(path.join(CANONICAL_DIR, f), 'utf8'));
  }
}

let deliveryCount = 0;
const deliveryLog = [];
const armedInjections = [];

function applyChaos(fixtureName) {
  for (let i = armedInjections.length - 1; i >= 0; i--) {
    const inj = armedInjections[i];
    if (inj.remaining <= 0) continue;
    if (inj.fixture && inj.fixture !== fixtureName) continue;
    inj.remaining -= 1;
    if (inj.scenario === 'rate-limit') return { fixtureName, body: { error: 'rate_limited' }, status: 429 };
    if (inj.scenario === 'malformed') return { fixtureName, body: { error: 'invalid_json' }, status: 400 };
    if (inj.scenario === 'schema-drift') return { fixtureName, body: { object: 'unknown', entry: [] }, status: 200 };
  }
  return { fixtureName, body: FIXTURES[fixtureName], status: 200 };
}

function send(res, status, body) {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    'Content-Type': 'application/json',
    'Content-Length': Buffer.byteLength(payload),
  });
  res.end(payload);
}

const deliveryServer = http.createServer(async (req, res) => {
  try {
    if (req.method === 'GET' && req.url === '/health') {
      return send(res, 200, { ok: true, fixtures: Object.keys(FIXTURES) });
    }
    const m = req.url.match(/^\/v1\/webhook\/([a-z0-9-]+)$/);
    if (req.method === 'POST' && m) {
      const fixtureName = m[1];
      if (!FIXTURES[fixtureName]) return send(res, 404, { error: 'unknown_fixture', fixture: fixtureName });
      const applied = applyChaos(fixtureName);
      if (applied.status === 200) {
        deliveryCount += 1;
        deliveryLog.push({ ts: Date.now(), fixture: fixtureName, status: 200 });
      } else {
        deliveryLog.push({ ts: Date.now(), fixture: fixtureName, status: applied.status });
      }
      return send(res, applied.status, applied.body);
    }
    return send(res, 404, { error: 'not_found', url: req.url });
  } catch (e) {
    return send(res, 500, { error: 'internal', message: String(e.message || e) });
  }
});

const controlServer = http.createServer(async (req, res) => {
  try {
    if (req.method === 'GET' && req.url === '/control/deliveries') {
      return send(res, 200, { count: deliveryCount, log: deliveryLog });
    }
    if (req.method === 'POST' && req.url === '/control/reset') {
      deliveryLog.length = 0;
      deliveryCount = 0;
      armedInjections.length = 0;
      return send(res, 200, { ok: true, reset: true });
    }
    if (req.method === 'POST' && req.url === '/control/clear') {
      armedInjections.length = 0;
      return send(res, 200, { ok: true, cleared: true });
    }
    if (req.method === 'POST' && req.url === '/control/inject') {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const body = JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}');
      armedInjections.push({ scenario: body.scenario, fixture: body.fixture, remaining: body.count || 1 });
      return send(res, 200, { ok: true, armed: body });
    }
    return send(res, 404, { error: 'not_found', url: req.url });
  } catch (e) {
    return send(res, 500, { error: 'internal', message: String(e.message || e) });
  }
});

deliveryServer.listen(9300, '0.0.0.0', () => {
  console.log('[phase-9-webhook-simulator] delivery listening on 9300');
});
controlServer.listen(9301, '0.0.0.0', () => {
  console.log('[phase-9-webhook-simulator] control  listening on 9301');
  console.log('[phase-9-webhook-simulator] fixtures ' + Object.keys(FIXTURES).join(', '));
});
