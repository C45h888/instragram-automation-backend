/**
 * Graph Runtime Simulator — Canonical external environment (Phase 7 contract §14)
 * ═══════════════════════════════════════════════════════════════════════════════
 *
 * NOT a 1:1 Meta API copy. The goal is RUNTIME FIDELITY, not API fidelity.
 * Workers must encounter realistic payloads, realistic pagination,
 * realistic failures, realistic rate limits, realistic auth failures,
 * realistic partial responses, and realistic schema drift.
 *
 * Architecture:
 *   - HTTP server bound to test-runtime-net. transport.js substrates
 *     call it via axios like they would call Meta.
 *   - Control API on a separate port, NOT exposed to workers. The
 *     runtime simulator and tests use it to deterministically inject
 *     failure scenarios.
 *
 * Endpoints (runtime primitives):
 *   Acquisition:
 *     GET /v1/accounts
 *     GET /v1/accounts/{id}/conversations?cursor=
 *     GET /v1/conversations/{threadId}/messages?cursor=
 *     GET /v1/accounts/{id}/media?cursor=
 *     GET /v1/media/{id}/comments?cursor=
 *     GET /v1/media/{id}/insights
 *   Publishing:
 *     POST /v1/accounts/{id}/media_publish
 *     GET  /v1/publish/{publishId}
 *   Capability:
 *     GET /v1/accounts/{id}/capability
 *     POST /v1/accounts/{id}/token/validate
 *     POST /v1/accounts/{id}/scopes/validate
 *     POST /v1/accounts/{id}/simulate/degrade
 *
 * Failure vocabulary (control API can set deterministically):
 *   - successful response
 *   - retryable failure (5xx)
 *   - permanent failure (4xx)
 *   - pagination chain
 *   - rate-limit response (429)
 *   - token failure (401)
 *   - malformed payload
 *   - partial response
 *   - schema drift
 *   - duplicate (idempotency-key collision)
 *   - stale data (last_updated < threshold)
 */

const http = require('http');
const crypto = require('crypto');

const FAILURE_MODES = [
  'success',
  'retryable',
  'permanent',
  'rate-limited',
  'token-failure',
  'malformed',
  'partial',
  'schema-drift',
  'duplicate',
  'stale',
];

class GraphSimulator {
  /**
   * @param {object} [opts]
   * @param {number} [opts.workerPort=9100] — port the workers call (HTTP)
   * @param {number} [opts.controlPort=9101] — port the test/control uses
   * @param {string} [opts.bindAddress='0.0.0.0']
   */
  constructor({
    workerPort = 9100,
    controlPort = 9101,
    bindAddress = '0.0.0.0',
    logLevel = 'warn',
  } = {}) {
    this._workerPort = workerPort;
    this._controlPort = controlPort;
    this._bindAddress = bindAddress;
    this._logLevel = logLevel;
    this._workerServer = null;
    this._controlServer = null;
    this._started = false;

    // State
    this._accounts = new Map();
    this._conversations = new Map();
    this._messages = new Map();
    this._media = new Map();
    this._comments = new Map();
    this._insights = new Map();
    this._publishes = new Map();
    this._capability = new Map();

    // Failure injection state — set via control API
    this._failureMode = 'success';           // global default
    this._failureEndpointScope = null;        // null = all endpoints
    this._failureCallCount = 0;
    this._failureMaxCalls = null;             // null = infinite
    this._rateLimitRemaining = 100;
    this._rateLimitReset = null;
    this._schemaDriftEnabled = false;
    this._staleThresholdMs = 0;
    this._paginatePageSize = 25;
    this._partialResponseFields = null;       // list of fields to omit
    this._idempotencyKeys = new Map();

    this._callLog = [];
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Lifecycle
  // ═══════════════════════════════════════════════════════════════════════════

  async start() {
    if (this._started) return;
    this._started = true;

    this._seedDefaultState();
    await this._startWorkerServer();
    await this._startControlServer();
  }

  async stop() {
    if (!this._started) return;
    this._started = false;
    if (this._workerServer) await new Promise((r) => this._workerServer.close(r));
    if (this._controlServer) await new Promise((r) => this._controlServer.close(r));
    this._workerServer = null;
    this._controlServer = null;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Seed — minimal default Graph state for tests
  // ═══════════════════════════════════════════════════════════════════════════

  _seedDefaultState() {
    const account = {
      id: 'acc-1',
      username: 'test_account',
      name: 'Test Account',
      biography: 'phase-7 test',
      website: 'https://example.com',
      media_count: 0,
      followers_count: 100,
      follows_count: 50,
    };
    this._accounts.set(account.id, account);
    this._capability.set(account.id, {
      accountId: account.id,
      tokenStatus: 'valid',
      scopes: ['instagram_basic', 'instagram_manage_comments', 'pages_show_list'],
      lastValidated: Date.now(),
      strikeCount: 0,
    });

    for (let i = 1; i <= 5; i++) {
      const mid = `m-${i}`;
      this._media.set(mid, {
        id: mid,
        accountId: account.id,
        caption: `post ${i}`,
        media_type: 'IMAGE',
        permalink: `https://instagram.com/p/${mid}`,
        timestamp: new Date(Date.now() - i * 3600_000).toISOString(),
        like_count: 10 * i,
        comments_count: 2 * i,
      });
      this._comments.set(mid, [
        { id: `c-${i}-1`, text: `comment on ${mid}`, username: 'u1', timestamp: new Date().toISOString() },
        { id: `c-${i}-2`, text: `another on ${mid}`, username: 'u2', timestamp: new Date().toISOString() },
      ]);
      this._insights.set(mid, {
        impressions: 1000 * i,
        reach: 800 * i,
        engagement: 50 * i,
      });
    }

    for (let i = 1; i <= 3; i++) {
      const tid = `t-${i}`;
      this._conversations.set(tid, {
        id: tid,
        accountId: account.id,
        participants: ['u_sender'],
        updated_at: new Date().toISOString(),
        message_count: 0,
      });
      this._messages.set(tid, [
        { id: `msg-${i}-1`, text: `hello ${i}`, from: 'u_sender', timestamp: new Date().toISOString() },
        { id: `msg-${i}-2`, text: `reply ${i}`, from: account.username, timestamp: new Date().toISOString() },
      ]);
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Worker-facing HTTP server
  // ═══════════════════════════════════════════════════════════════════════════

  async _startWorkerServer() {
    return new Promise((resolve, reject) => {
      this._workerServer = http.createServer((req, res) => this._handleWorkerRequest(req, res));
      this._workerServer.on('error', reject);
      this._workerServer.listen(this._workerPort, this._bindAddress, () => resolve());
    });
  }

  async _handleWorkerRequest(req, res) {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const path = url.pathname;
    const method = req.method;

    this._callLog.push({ method, path, ts: Date.now() });

    // Read body for POST
    let body = '';
    req.on('data', (chunk) => (body += chunk));
    req.on('end', () => {
      let parsed = null;
      if (body) {
        try { parsed = JSON.parse(body); } catch (_) { parsed = body; }
      }

      const result = this._route(method, path, url, parsed);
      this._writeJson(res, result.status, result.body);
    });
  }

  _route(method, path, url, body) {
    // Apply failure injection first
    const failure = this._maybeInjectFailure(path);
    if (failure) return failure;

    // Capability
    if (method === 'GET' && /^\/v1\/accounts\/([^/]+)\/capability$/.test(path)) {
      const accountId = path.split('/')[3];
      const cap = this._capability.get(accountId);
      if (!cap) return { status: 404, body: { error: 'account_not_found' } };
      return { status: 200, body: this._maybeDrift(cap) };
    }
    if (method === 'POST' && /\/token\/validate$/.test(path)) {
      const cap = this._capability.get(path.split('/')[3]);
      if (!cap) return { status: 404, body: { error: 'account_not_found' } };
      return { status: 200, body: { valid: cap.tokenStatus === 'valid', tokenStatus: cap.tokenStatus } };
    }
    if (method === 'POST' && /\/scopes\/validate$/.test(path)) {
      const cap = this._capability.get(path.split('/')[3]);
      if (!cap) return { status: 404, body: { error: 'account_not_found' } };
      const requested = (body && body.scopes) || [];
      const missing = requested.filter((s) => !cap.scopes.includes(s));
      return { status: 200, body: { ok: missing.length === 0, missing } };
    }
    if (method === 'POST' && /\/simulate\/degrade$/.test(path)) {
      const cap = this._capability.get(path.split('/')[3]);
      if (!cap) return { status: 404, body: { error: 'account_not_found' } };
      const action = body && body.action;
      if (action === 'expire-token') cap.tokenStatus = 'expired';
      else if (action === 'revoke-token') cap.tokenStatus = 'revoked';
      else if (action === 'auth-strike') cap.strikeCount++;
      else if (action === 'reset') {
        cap.tokenStatus = 'valid';
        cap.strikeCount = 0;
      }
      return { status: 200, body: cap };
    }

    // Acquisition
    if (method === 'GET' && path === '/v1/accounts') {
      return { status: 200, body: { data: Array.from(this._accounts.values()) } };
    }
    if (method === 'GET' && /\/conversations$/.test(path)) {
      const accountId = path.split('/')[3];
      return this._paginated(Array.from(this._conversations.values()).filter((c) => c.accountId === accountId), url);
    }
    if (method === 'GET' && /\/conversations\/([^/]+)\/messages$/.test(path)) {
      const tid = path.split('/')[3];
      return this._paginated(this._messages.get(tid) || [], url);
    }
    if (method === 'GET' && /\/media$/.test(path)) {
      const accountId = path.split('/')[3];
      return this._paginated(Array.from(this._media.values()).filter((m) => m.accountId === accountId), url);
    }
    if (method === 'GET' && /\/media\/([^/]+)\/comments$/.test(path)) {
      const mid = path.split('/')[3];
      return this._paginated(this._comments.get(mid) || [], url);
    }
    if (method === 'GET' && /\/media\/([^/]+)\/insights$/.test(path)) {
      const mid = path.split('/')[3];
      const insights = this._insights.get(mid);
      if (!insights) return { status: 404, body: { error: 'media_not_found' } };
      return { status: 200, body: { data: insights } };
    }

    // Publishing
    if (method === 'POST' && /\/media_publish$/.test(path)) {
      const accountId = path.split('/')[3];
      const idemKey = (body && body.idempotency_key) || null;
      if (idemKey && this._idempotencyKeys.has(idemKey)) {
        return { status: 200, body: this._idempotencyKeys.get(idemKey) };
      }
      const publishId = `pub-${crypto.randomUUID()}`;
      const publish = {
        id: publishId,
        accountId,
        status: 'in_progress',
        creation_id: body && body.creation_id,
        submitted_at: Date.now(),
      };
      this._publishes.set(publishId, publish);
      if (idemKey) this._idempotencyKeys.set(idemKey, publish);
      return { status: 200, body: publish };
    }
    if (method === 'GET' && /\/publish\/([^/]+)$/.test(path)) {
      const pid = path.split('/')[3];
      const pub = this._publishes.get(pid);
      if (!pub) return { status: 404, body: { error: 'publish_not_found' } };
      return { status: 200, body: this._maybeDrift(pub) };
    }

    return { status: 404, body: { error: 'not_found', method, path } };
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Control API (test/control plane; workers never see this port)
  // ═══════════════════════════════════════════════════════════════════════════

  async _startControlServer() {
    return new Promise((resolve, reject) => {
      this._controlServer = http.createServer((req, res) => this._handleControlRequest(req, res));
      this._controlServer.on('error', reject);
      this._controlServer.listen(this._controlPort, this._bindAddress, () => resolve());
    });
  }

  async _handleControlRequest(req, res) {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const path = url.pathname;
    const method = req.method;

    let body = '';
    req.on('data', (chunk) => (body += chunk));
    req.on('end', () => {
      let parsed = null;
      if (body) {
        try { parsed = JSON.parse(body); } catch (_) { parsed = body; }
      }

      if (method === 'GET' && path === '/control/state') {
        return this._writeJson(res, 200, this._controlState());
      }
      if (method === 'POST' && path === '/control/failure') {
        this._failureMode = (parsed && parsed.mode) || 'success';
        this._failureEndpointScope = (parsed && parsed.endpoint) || null;
        this._failureCallCount = 0;
        this._failureMaxCalls = parsed && parsed.maxCalls != null ? parsed.maxCalls : null;
        return this._writeJson(res, 200, { ok: true, mode: this._failureMode });
      }
      if (method === 'POST' && path === '/control/reset') {
        this._resetFailureState();
        return this._writeJson(res, 200, { ok: true });
      }
      if (method === 'POST' && path === '/control/rate-limit') {
        this._rateLimitRemaining = parsed.remaining;
        this._rateLimitReset = parsed.resetAt || Date.now() + 60_000;
        return this._writeJson(res, 200, { ok: true });
      }
      if (method === 'POST' && path === '/control/schema-drift') {
        this._schemaDriftEnabled = !!parsed.enabled;
        return this._writeJson(res, 200, { ok: true });
      }
      if (method === 'POST' && path === '/control/pagination') {
        this._paginatePageSize = parsed.pageSize || 25;
        return this._writeJson(res, 200, { ok: true });
      }
      if (method === 'POST' && path === '/control/partial') {
        this._partialResponseFields = parsed.omitFields || null;
        return this._writeJson(res, 200, { ok: true });
      }
      if (method === 'POST' && path === '/control/publish/complete') {
        const pub = this._publishes.get(parsed.publishId);
        if (pub) pub.status = parsed.status || 'finished';
        return this._writeJson(res, 200, { ok: !!pub });
      }
      if (method === 'GET' && path === '/control/call-log') {
        return this._writeJson(res, 200, { calls: this._callLog });
      }
      if (method === 'POST' && path === '/control/call-log/clear') {
        this._callLog = [];
        return this._writeJson(res, 200, { ok: true });
      }

      this._writeJson(res, 404, { error: 'control_not_found', path });
    });
  }

  _controlState() {
    return {
      failureMode: this._failureMode,
      failureEndpointScope: this._failureEndpointScope,
      failureCalls: this._failureCallCount,
      rateLimitRemaining: this._rateLimitRemaining,
      schemaDrift: this._schemaDriftEnabled,
      paginatePageSize: this._paginatePageSize,
      partialFields: this._partialResponseFields,
      accounts: this._accounts.size,
      publishes: this._publishes.size,
    };
  }

  _resetFailureState() {
    this._failureMode = 'success';
    this._failureEndpointScope = null;
    this._failureCallCount = 0;
    this._failureMaxCalls = null;
    this._rateLimitRemaining = 100;
    this._schemaDriftEnabled = false;
    this._partialResponseFields = null;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Failure injection
  // ═══════════════════════════════════════════════════════════════════════════

  _maybeInjectFailure(path) {
    if (this._failureMode === 'success') return null;
    if (this._failureEndpointScope && !path.startsWith(this._failureEndpointScope)) return null;
    if (this._failureMaxCalls != null && this._failureCallCount >= this._failureMaxCalls) return null;

    this._failureCallCount++;

    switch (this._failureMode) {
      case 'retryable':
        return { status: 503, body: { error: 'service_unavailable', retryable: true } };
      case 'permanent':
        return { status: 400, body: { error: 'bad_request', retryable: false } };
      case 'rate-limited':
        return {
          status: 429,
          body: { error: 'rate_limited', retryable: true },
          headers: {
            'x-ratelimit-remaining': String(this._rateLimitRemaining),
            'x-ratelimit-reset': String(this._rateLimitReset || Date.now() + 60_000),
          },
        };
      case 'token-failure':
        return { status: 401, body: { error: 'invalid_token', retryable: false } };
      case 'malformed':
        return { status: 200, body: { this_is: 'not the expected shape', data: null } };
      case 'partial':
        // Mark for downstream omission
        this._partialResponseFields = ['username', 'followers_count'];
        return null;
      case 'schema-drift':
        this._schemaDriftEnabled = true;
        return null;
      case 'duplicate':
        // Idempotency key handling happens in route; force a duplicate by
        // pre-seeding a fixed key. Simpler: return a duplicated payload.
        return null;
      case 'stale':
        this._staleThresholdMs = 24 * 3600_000;
        return null;
      default:
        return null;
    }
  }

  _maybeDrift(obj) {
    if (!this._schemaDriftEnabled && !this._partialResponseFields) return obj;
    const out = { ...obj };
    if (this._schemaDriftEnabled) {
      out.unexpectedField = 'drift-' + Math.random().toString(36).slice(2, 6);
      if ('username' in out) out.user_name = out.username; // renamed field
    }
    if (this._partialResponseFields) {
      for (const f of this._partialResponseFields) {
        delete out[f];
      }
    }
    return out;
  }

  _paginated(items, url) {
    const cursor = parseInt(url.searchParams.get('cursor') || '0', 10);
    const pageSize = this._paginatePageSize;
    const slice = items.slice(cursor, cursor + pageSize);
    const next = cursor + pageSize < items.length ? String(cursor + pageSize) : null;
    const body = { data: this._maybeDrift(slice) };
    if (next) body.paging = { cursors: { after: next }, next: next };
    return { status: 200, body };
  }

  _writeJson(res, status, body, extraHeaders = {}) {
    const json = JSON.stringify(body);
    res.writeHead(status, {
      'content-type': 'application/json',
      'content-length': Buffer.byteLength(json),
      ...extraHeaders,
    });
    res.end(json);
  }
}

module.exports = { GraphSimulator, FAILURE_MODES };

// ═══════════════════════════════════════════════════════════════════════════
// Bootstrap — runs only when executed directly (not imported)
// ═══════════════════════════════════════════════════════════════════════════

if (require.main === module) {
  const port = parseInt(process.env.GRAPH_SIM_WORKER_PORT || '9100', 10);
  const controlPort = parseInt(process.env.GRAPH_SIM_CONTROL_PORT || '9101', 10);
  const sim = new GraphSimulator({ workerPort: port, controlPort });
  sim.start().then(() => {
    console.log(`[graph-simulator] worker=${port} control=${controlPort}`);
  }).catch((err) => {
    console.error('[graph-simulator] start failed:', err);
    process.exit(1);
  });
  process.on('SIGTERM', () => sim.stop().then(() => process.exit(0)));
  process.on('SIGINT',  () => sim.stop().then(() => process.exit(0)));
}
