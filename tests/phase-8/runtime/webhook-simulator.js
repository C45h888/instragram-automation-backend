// Phase 8 — Webhook Simulator (CJS entrypoint for Docker).
//
// Docker mounts this file at /sim/server.js and runs `node /sim/server.js`.
// The project package.json has no "type":"module" so this .js file is
// treated as CJS. We re-implement the simulator here in CJS shape so
// it can boot under node 22-alpine without ESM config gymnastics.
//
// Keep this file in sync with webhook-simulator.mjs (which is the
// ESM twin used by vitest when loading via Node's ESM loader).

'use strict';

const http = require('http');

const FIXTURES = {
  'message-created': {
    object: 'instagram',
    entry: [{
      id: '17841405822304914', time: 1700000000,
      messaging: [{
        sender: { id: 'USER_SENDER_1' },
        recipient: { id: 'PAGE_RECIPIENT_1' },
        timestamp: 1700000000000,
        message: { mid: 'm_message_001', text: 'hello from phase-8 webhook simulator' },
      }],
    }],
  },
  'comment-created': {
    object: 'instagram',
    entry: [{
      id: '17841405822304914', time: 1700000001,
      changes: [{
        field: 'comments',
        value: {
          id: 'COMMENT_001', text: 'phase-8 comment fixture',
          from: { id: 'USER_COMMENTER_1', username: 'phase8_user' },
          media: { id: 'MEDIA_001' },
        },
      }],
    }],
  },
  'mention-created': {
    object: 'instagram',
    entry: [{
      id: '17841405822304914', time: 1700000002,
      changes: [{
        field: 'mentions',
        value: { comment_id: 'COMMENT_MENTION_001', media_id: 'MEDIA_001', text: '@phase8_target mention fixture' },
      }],
    }],
  },
  'story-reply': {
    object: 'instagram',
    entry: [{
      id: '17841405822304914', time: 1700000003,
      changes: [{
        field: 'story_insights',
        value: { story_id: 'STORY_001', reply: { text: 'story reply from phase-8' }, from: { id: 'USER_STORYREPLY_1' } },
      }],
    }],
  },
  'media-update': {
    object: 'instagram',
    entry: [{
      id: '17841405822304914', time: 1700000004,
      changes: [{
        field: 'media',
        value: { media_id: 'MEDIA_002', media_type: 'IMAGE', media_url: 'https://example.com/phase8.jpg' },
      }],
    }],
  },
  'conversation-update': {
    object: 'instagram',
    entry: [{
      id: '17841405822304914', time: 1700000005,
      changes: [{
        field: 'conversations',
        value: { conversation_id: 'CONV_001', participants: ['USER_SENDER_1', 'PAGE_RECIPIENT_1'], updated_fields: ['messages'] },
      }],
    }],
  },
};

const CHAOS = {
  'rate-limit':       { status: 429, body: { error: 'rate_limited' } },
  'schema-drift':     { status: 200, body: { object: 'unknown', entry: [] } },
  'duplicate':        { status: 200, body: null, repeatLast: true },
  'stale':            { status: 200, body: null, stale: true },
  'malformed':        { status: 400, body: { error: 'invalid_json' } },
  'token-failure':    { status: 401, body: { error: 'invalid_token' } },
  'scope-revocation': { status: 403, body: { error: 'scope_revoked' } },
};

let lastDelivered = null;
let lastFixtureName = null;
let staleSnapshot = null;
let deliveryCount = 0;
const deliveryLog = [];
const armedInjections = [];

function applyChaos(fixtureName) {
  for (let i = armedInjections.length - 1; i >= 0; i--) {
    const inj = armedInjections[i];
    if (inj.remaining <= 0) continue;
    if (inj.fixture && inj.fixture !== fixtureName) continue;
    inj.remaining -= 1;
    const chaos = CHAOS[inj.scenario];
    if (!chaos) return { fixtureName, body: FIXTURES[fixtureName], status: 200 };
    if (chaos.status !== 200) return { fixtureName, body: chaos.body, status: chaos.status };
    if (chaos.body) return { fixtureName, body: chaos.body, status: chaos.status };
    if (chaos.repeatLast && lastDelivered) {
      return { fixtureName, body: lastDelivered, status: 200, duplicated: true };
    }
    if (chaos.stale && staleSnapshot) {
      return { fixtureName, body: staleSnapshot, status: 200, stale: true };
    }
  }
  return { fixtureName, body: FIXTURES[fixtureName], status: 200 };
}

function send(res, status, body, extraHeaders) {
  const payload = JSON.stringify(body);
  res.writeHead(status, Object.assign(
    { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) },
    extraHeaders || {}
  ));
  res.end(payload);
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}')); }
      catch (e) { reject(e); }
    });
    req.on('error', reject);
  });
}

const deliveryServer = http.createServer(async (req, res) => {
  try {
    if (req.method === 'GET' && req.url === '/health') {
      return send(res, 200, { ok: true, fixtures: Object.keys(FIXTURES) });
    }
    if (req.method === 'GET' && req.url === '/v1/webhook/fixtures') {
      return send(res, 200, { fixtures: Object.keys(FIXTURES) });
    }
    if (req.method === 'POST' && req.url === '/v1/webhook/replay') {
      const results = [];
      for (const name of Object.keys(FIXTURES)) {
        const applied = applyChaos(name);
        if (applied.status === 200) {
          lastDelivered = applied.body;
          lastFixtureName = name;
          if (!staleSnapshot) staleSnapshot = JSON.parse(JSON.stringify(applied.body));
          deliveryCount += 1;
          deliveryLog.push({ ts: Date.now(), fixture: name, status: 200, scenario: 'replay' });
        }
        results.push({ fixture: name, status: applied.status });
      }
      return send(res, 200, { replayed: results });
    }
    const m = req.url.match(/^\/v1\/webhook\/([a-z0-9-]+)$/);
    if (req.method === 'POST' && m) {
      const fixtureName = m[1];
      if (!FIXTURES[fixtureName]) return send(res, 404, { error: 'unknown_fixture', fixture: fixtureName });
      const applied = applyChaos(fixtureName);
      if (applied.status === 200) {
        lastDelivered = applied.body;
        lastFixtureName = fixtureName;
        if (!staleSnapshot) staleSnapshot = JSON.parse(JSON.stringify(applied.body));
        deliveryCount += 1;
        deliveryLog.push({
          ts: Date.now(), fixture: fixtureName, status: 200,
          scenario: armedInjections.length ? 'chaos' : 'normal',
          duplicated: !!applied.duplicated, stale: !!applied.stale,
        });
      } else {
        deliveryLog.push({ ts: Date.now(), fixture: fixtureName, status: applied.status, scenario: 'chaos' });
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
      lastDelivered = null; lastFixtureName = null; staleSnapshot = null;
      deliveryCount = 0;
      armedInjections.length = 0;
      return send(res, 200, { ok: true, reset: true });
    }
    if (req.method === 'POST' && req.url === '/control/clear') {
      armedInjections.length = 0;
      return send(res, 200, { ok: true, cleared: true });
    }
    if (req.method === 'POST' && req.url === '/control/inject') {
      const body = await readJson(req);
      const scenario = body && body.scenario;
      const fixture = body && body.fixture;
      const count = (body && body.count) || 1;
      if (!CHAOS[scenario]) {
        return send(res, 400, { error: 'unknown_scenario', scenario, available: Object.keys(CHAOS) });
      }
      armedInjections.push({ scenario, fixture, remaining: count });
      return send(res, 200, { ok: true, armed: { scenario, fixture, count } });
    }
    return send(res, 404, { error: 'not_found', url: req.url });
  } catch (e) {
    return send(res, 500, { error: 'internal', message: String(e.message || e) });
  }
});

deliveryServer.listen(9200, '0.0.0.0', () => {
  console.log('[webhook-simulator] delivery  listening on 9200');
});
controlServer.listen(9201, '0.0.0.0', () => {
  console.log('[webhook-simulator] control   listening on 9201');
  console.log('[webhook-simulator] fixtures  ' + Object.keys(FIXTURES).join(', '));
});
