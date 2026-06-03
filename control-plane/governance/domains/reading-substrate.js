// control-plane/governance/domains/reading-substrate.js
// Reading Substrate: governed read operations under persist-telemetry-fsm governance.
//
// Owns: read execution dispatch (DB → Supabase, API → transport substrates),
//        per-domain read caching with FSM-governed TTL, concurrent read tracking,
//        health signals for FSM backpressure detection.
// Does NOT own: governance policy (FSM decides what's allowed),
//               routing (CK routes events to FSM),
//               authentication (delegates to credential cache like transports).
//
// Instantiated by CK. FSM delegates reads here — same pattern as db/writers for writes.
//
// Execution flow:
//   caller → CK(DB_READ_REQUESTED) → FSM(gate) → reading-substrate.executeRead()
//   → CK(DB_READ_COMPLETE) → FSM(complete) → caller
//
// Read domains:
//   db.media       — Supabase: recent media, monitored hashtags
//   db.engagement  — Supabase: engagement data
//   ig.content     — Instagram API: business posts
//   ig.engagement  — Instagram API: comments, conversations, messages
//   ig.insights    — Instagram API: account + media insights
//   ig.ugc         — Instagram API: hashtag search, tagged media

// ── Lazy governance reference — set by CK at boot ──────────────────────────
let _governance = null;
let _fsm = null;

function init(govContext) {
  _governance = govContext.governance;
  _fsm = govContext.fsm;
}

// ── Lazy substrate deps — deferred to avoid circular module loads ──────────
function _getDbReaders() {
  return require('../../../substrates/db/readers');
}

function _getContentTransport() {
  return require('../../../substrates/content/transport');
}

function _getEngagementTransport() {
  return require('../../../substrates/engagement/transport');
}

function _getInsightsTransport() {
  return require('../../../substrates/insights/transport');
}

function _getUgcTransport() {
  return require('../../../substrates/ugc/transport');
}

function _getPersistence() {
  return require('../../../substrates/persistence');
}

// ═══════════════════════════════════════════════════════════════════════════════
// 0. Read Domain Registry
// ═══════════════════════════════════════════════════════════════════════════════

const READ_DOMAIN_EXECUTORS = {
  // ── DB reads (Supabase) ──────────────────────────────────────────────────
  'db.media': {
    description: 'Recent media + monitored hashtags from Supabase',
    execute: async (params) => {
      const readers = _getDbReaders();
      if (params.query === 'getRecentMedia') {
        return readers.getRecentMedia(params.accountId);
      }
      if (params.query === 'getMonitoredHashtags') {
        return readers.getMonitoredHashtags(params.accountId);
      }
      return { error: `unknown db.media query: ${params.query}` };
    },
  },

  // ── IG API reads ─────────────────────────────────────────────────────────
  'ig.content': {
    description: 'Business-owned media posts from Instagram Graph API',
    execute: async (params) => {
      const transport = _getContentTransport();
      const persistence = _getPersistence();
      const creds = await persistence.resolveAccountCredentials(params.accountId);
      return transport.fetchPosts(params.accountId, params.limit || 50, creds);
    },
  },

  'ig.engagement': {
    description: 'Comments, conversations, messages from Instagram Graph API',
    execute: async (params) => {
      const transport = _getEngagementTransport();
      const persistence = _getPersistence();
      const creds = await persistence.resolveAccountCredentials(params.accountId);

      if (params.mediaId) {
        return transport.fetchComments(params.accountId, params.mediaId, params.limit, creds);
      }
      if (params.conversationId) {
        return transport.fetchMessages(params.accountId, params.conversationId, params.limit, creds);
      }
      return transport.fetchConversations(params.accountId, params.limit, creds);
    },
  },

  'ig.insights': {
    description: 'Account + media insights from Instagram Graph API',
    execute: async (params) => {
      const transport = _getInsightsTransport();
      const persistence = _getPersistence();
      const creds = await persistence.resolveAccountCredentials(params.accountId);

      if (params.mediaList) {
        return transport.fetchMediaInsightsBatch(params.mediaList, creds.pageToken);
      }
      const options = {
        since: params.since || Math.floor((Date.now() - 7 * 24 * 3600000) / 1000),
        until: params.until || Math.floor(Date.now() / 1000),
        hasWebsite: params.hasWebsite || false,
      };
      return transport.fetchAccountInsights(params.accountId, options, creds);
    },
  },

  'ig.ugc': {
    description: 'Hashtag search + tagged media from Instagram Graph API',
    execute: async (params) => {
      const transport = _getUgcTransport();
      const persistence = _getPersistence();
      const creds = await persistence.resolveAccountCredentials(params.accountId);

      if (params.hashtag) {
        return transport.fetchHashtagMedia(params.accountId, params.hashtag, params.limit, creds);
      }
      return transport.fetchTaggedMedia(params.accountId, params.limit, creds);
    },
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 1. Read Caching — FSM-governed TTL per domain
// ═══════════════════════════════════════════════════════════════════════════════

const _readCache = new Map(); // key → { data, expiresAt, domain }

const DEFAULT_CACHE_TTL_MS = {
  'db.media':     60_000,    // 1 min — recent media changes frequently
  'ig.content':   120_000,   // 2 min
  'ig.engagement': 60_000,   // 1 min
  'ig.insights':  300_000,   // 5 min — insights are expensive
  'ig.ugc':       120_000,   // 2 min
};

function _cacheKey(domain, params) {
  return `${domain}:${params.accountId}:${params.query || 'default'}`;
}

function getCached(domain, params) {
  const key = _cacheKey(domain, params);
  const entry = _readCache.get(key);
  if (entry && Date.now() < entry.expiresAt) {
    return entry.data;
  }
  if (entry) _readCache.delete(key); // expired
  return null;
}

function setCache(domain, params, data) {
  const ttl = DEFAULT_CACHE_TTL_MS[domain] || 60_000;
  const key = _cacheKey(domain, params);
  _readCache.set(key, { data, expiresAt: Date.now() + ttl, domain });
}

function clearCache(domain, accountId) {
  const prefix = accountId ? `${domain}:${accountId}:` : `${domain}:`;
  for (const key of _readCache.keys()) {
    if (key.startsWith(prefix)) _readCache.delete(key);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// 2. Concurrent Read Tracking
// ═══════════════════════════════════════════════════════════════════════════════

let _readsInFlight = 0;
const _activeReads = new Map(); // readId → { domain, accountId, startedAt }

function getReadsInFlight() { return _readsInFlight; }

// ═══════════════════════════════════════════════════════════════════════════════
// 3. Execute Governed Read
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Execute a governed read operation.
 * Called by persist-telemetry-fsm after gate validation.
 *
 * @param {string} domain     — read domain (e.g., 'db.media', 'ig.content')
 * @param {object} params     — { accountId, query, limit, ...domain-specific }
 * @param {string} readId     — unique read identifier for tracking
 * @returns {Promise<object>} { success, data, error, latencyMs }
 */
async function executeRead(domain, params, readId) {
  const startTime = Date.now();
  const executor = READ_DOMAIN_EXECUTORS[domain];

  if (!executor) {
    return { success: false, data: null, error: `unknown_read_domain: ${domain}`, latencyMs: Date.now() - startTime };
  }

  // Track in-flight
  _readsInFlight++;
  _activeReads.set(readId, { domain, accountId: params.accountId, startedAt: startTime });

  try {
    // Check cache first
    const cached = getCached(domain, params);
    if (cached !== null) {
      _readsInFlight = Math.max(0, _readsInFlight - 1);
      _activeReads.delete(readId);
      _emitObserved(params.accountId, domain, params.query || 'cached', Date.now() - startTime);
      return { success: true, data: cached, error: null, latencyMs: Date.now() - startTime, cached: true };
    }

    const result = await executor.execute(params);

    const elapsed = Date.now() - startTime;
    _readsInFlight = Math.max(0, _readsInFlight - 1);
    _activeReads.delete(readId);

    if (result.error) {
      _emitObserved(params.accountId, domain, params.query || 'read', elapsed, result.error);
      return { success: false, data: null, error: result.error, latencyMs: elapsed };
    }

    // Cache successful results
    setCache(domain, params, result);

    _emitObserved(params.accountId, domain, params.query || 'read', elapsed);
    return { success: true, data: result, error: null, latencyMs: elapsed };

  } catch (err) {
    const elapsed = Date.now() - startTime;
    _readsInFlight = Math.max(0, _readsInFlight - 1);
    _activeReads.delete(readId);
    _emitObserved(params.accountId, domain, params.query || 'read', elapsed, err.message);
    return { success: false, data: null, error: err.message, latencyMs: elapsed };
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// 4. Telemetry
// ═══════════════════════════════════════════════════════════════════════════════

function _emitObserved(accountId, domain, query, latencyMs, error = null) {
  if (!_governance) return;
  try {
    _governance.dispatch({
      type: 'DB_READ_OBSERVED',
      domain,
      query,
      accountId,
      latencyMs,
      error,
    });
  } catch (_) {}
}

// ═══════════════════════════════════════════════════════════════════════════════
// 5. Health
// ═══════════════════════════════════════════════════════════════════════════════

function getHealth() {
  return {
    ok: _readsInFlight < 20,
    readsInFlight: _readsInFlight,
    domains: Object.keys(READ_DOMAIN_EXECUTORS),
    cacheSize: _readCache.size,
  };
}

function getDomainWhitelist() {
  return Object.keys(READ_DOMAIN_EXECUTORS);
}

// ── Cache management for external callers (e.g. FSM on write completion) ──
function invalidateCache(domain, accountId) {
  clearCache(domain, accountId);
}

function invalidateAllCaches() {
  _readCache.clear();
}

module.exports = {
  init,
  executeRead,
  getHealth,
  getDomainWhitelist,
  getReadsInFlight,
  invalidateCache,
  invalidateAllCaches,
  clearCache,
};
