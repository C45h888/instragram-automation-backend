// substrates/db/reading/workers/accounts-worker.js
// Accounts Worker: governed Supabase reads for the accounts domain.
//
// Owns: getActiveAccounts() with FSM-observable caching.
// Does NOT own: governance policy (FSM), routing (CK), IG API calls, account mutations.
//
// Operationally bounded to: db.accounts read domain.
// Dispatched by: substrates/db/reading/index.js (via dispatchRead).
//
// Constitutional flow:
//   Caller → governance.governedRead('db.accounts', params)
//     → CK(DB_READ_REQUESTED) → persist-telemetry-fsm
//     → reading-substrate.executeRead → registry → worker.execute()
//     → DB_READ_COMPLETE → READ_RESULT_AVAILABLE
//
// Replaces: persistence.js getActiveAccounts() (account discovery).

const { getSupabaseAdmin } = require('../../../config/supabase');

// ── Per-worker cache ────────────────────────────────────────────────────────
const _cache = new Map(); // key → { data, expiresAt }

const crypto = require('crypto');

const CACHE_TTL_MS = {
  getActiveAccounts: 30_000,  // 30s (matches legacy persistence.js behavior)
  igIdToUserId:     30_000,  // 30s — per-igIds-set
  igThreadIdToUuid: 30_000,  // 30s — per-threadIds-set
};

function _cacheKey(query, context = null) {
  // 'igIdToUserId' is parameterized by the set of IG IDs — use sorted hash
  if (context) return `${query}:${context}`;
  return `${query}:null`;  // global query, no accountId dimension
}

function _sortedHash(arr) {
  return crypto.createHash('sha256').update([...arr].sort().join(',')).digest('hex').slice(0, 16);
}

function _getCached(query, context = null) {
  const key = _cacheKey(query, context);
  const entry = _cache.get(key);
  if (entry && Date.now() < entry.expiresAt) return entry.data;
  if (entry) _cache.delete(key);
  return null;
}

function _setCache(query, data, context = null) {
  const ttl = CACHE_TTL_MS[query] || 30_000;
  const key = _cacheKey(query, context);
  _cache.set(key, { data, expiresAt: Date.now() + ttl });
}

function _getStale(query, context = null) {
  const entry = _cache.get(_cacheKey(query, context));
  return entry ? entry.data : null;
}

// ── Execute ──────────────────────────────────────────────────────────────────

/**
 * Execute a governed DB read for the accounts domain.
 *
 * @param {object} params     — { query: 'getActiveAccounts' }
 * @param {object} governance — CK module (for DB_READ_OBSERVED emission)
 * @returns {Promise<{success: boolean, data?, error?, latencyMs: number, cached?: boolean, stale?: boolean}>}
 */
async function execute(params, governance) {
  const { query, igIds, threadIds } = params;
  const startTime = Date.now();

  const VALID_QUERIES = ['getActiveAccounts', 'igIdToUserId', 'igThreadIdToUuid'];
  if (!VALID_QUERIES.includes(query)) {
    const elapsed = Date.now() - startTime;
    _emitObserved(governance, query, elapsed, `unknown_query: ${query}`);
    return { success: false, data: null, error: `unknown_query: ${query}`, latencyMs: elapsed };
  }

  // ── igIdToUserId: batch-resolve instagram_business_id → user_id ──────────
  if (query === 'igIdToUserId') {
    const sorted = [...new Set(igIds || [])].sort();
    if (sorted.length === 0) {
      return { success: true, data: [], error: null, latencyMs: Date.now() - startTime, cached: false };
    }
    const hashCtx = _sortedHash(sorted);

    // Cache hit
    const cached = _getCached(query, hashCtx);
    if (cached !== null) {
      _emitObserved(governance, query, Date.now() - startTime, null, true);
      return { success: true, data: cached, error: null, latencyMs: Date.now() - startTime, cached: true };
    }

    const supabase = getSupabaseAdmin();
    if (!supabase) {
      const elapsed = Date.now() - startTime;
      _emitObserved(governance, query, elapsed, 'supabase_unavailable');
      return { success: false, data: null, error: 'supabase_unavailable', latencyMs: elapsed };
    }

    try {
      const { data, error } = await supabase
        .from('instagram_business_accounts')
        .select('instagram_business_id, user_id')
        .in('instagram_business_id', sorted);

      if (error) throw error;

      const result = data || [];
      const elapsed = Date.now() - startTime;
      _setCache(query, result, hashCtx);
      _emitObserved(governance, query, elapsed);
      return { success: true, data: result, error: null, latencyMs: elapsed };
    } catch (err) {
      const elapsed = Date.now() - startTime;
      _emitObserved(governance, query, elapsed, err.message);
      return { success: false, data: null, error: err.message, latencyMs: elapsed };
    }
  }

  // ── igThreadIdToUuid: batch-resolve instagram_thread_id → UUID ────────
  if (query === 'igThreadIdToUuid') {
    const sorted = [...new Set(threadIds || [])].sort();
    if (sorted.length === 0) {
      return { success: true, data: [], error: null, latencyMs: Date.now() - startTime, cached: false };
    }
    const hashCtx = _sortedHash(sorted);

    const cached = _getCached(query, hashCtx);
    if (cached !== null) {
      _emitObserved(governance, query, Date.now() - startTime, null, true);
      return { success: true, data: cached, error: null, latencyMs: Date.now() - startTime, cached: true };
    }

    const supabase = getSupabaseAdmin();
    if (!supabase) {
      const elapsed = Date.now() - startTime;
      _emitObserved(governance, query, elapsed, 'supabase_unavailable');
      return { success: false, data: null, error: 'supabase_unavailable', latencyMs: elapsed };
    }

    try {
      const { data, error } = await supabase
        .from('instagram_dm_conversations')
        .select('id, instagram_thread_id')
        .in('instagram_thread_id', sorted);

      if (error) throw error;

      const result = data || [];
      const elapsed = Date.now() - startTime;
      _setCache(query, result, hashCtx);
      _emitObserved(governance, query, elapsed);
      return { success: true, data: result, error: null, latencyMs: elapsed };
    } catch (err) {
      const elapsed = Date.now() - startTime;
      _emitObserved(governance, query, elapsed, err.message);
      return { success: false, data: null, error: err.message, latencyMs: elapsed };
    }
  }

  // ── getActiveAccounts ──────────────────────────────────────────────────
  const cached = _getCached(query);
  if (cached !== null) {
    _emitObserved(governance, query, Date.now() - startTime, null, true);
    return { success: true, data: cached, error: null, latencyMs: Date.now() - startTime, cached: true };
  }

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    const elapsed = Date.now() - startTime;
    _emitObserved(governance, query, elapsed, 'supabase_unavailable');
    return { success: false, data: null, error: 'supabase_unavailable', latencyMs: elapsed };
  }

  try {
    const result = await supabase
      .from('instagram_business_accounts')
      .select('id, instagram_business_id, user_id')
      .eq('is_connected', true)
      .eq('connection_status', 'active');

    if (result.error) throw result.error;

    const data = result.data || [];
    const elapsed = Date.now() - startTime;
    _setCache(query, data);
    _emitObserved(governance, query, elapsed);
    return { success: true, data, error: null, latencyMs: elapsed };

  } catch (err) {
    const elapsed = Date.now() - startTime;
    // Graceful degradation: return stale cache on error (matches legacy behavior)
    const stale = _getStale(query);
    if (stale !== null) {
      _emitObserved(governance, query, elapsed, err.message, false);
      return { success: true, data: stale, error: null, latencyMs: elapsed, stale: true };
    }
    _emitObserved(governance, query, elapsed, err.message);
    return { success: false, data: null, error: err.message, latencyMs: elapsed };
  }
}

// ── Telemetry ────────────────────────────────────────────────────────────────

function _emitObserved(governance, query, latencyMs, error = null, cached = false) {
  if (!governance || typeof governance.dispatch !== 'function') return;
  try {
    governance.dispatch({
      type: 'DB_READ_OBSERVED',
      domain: 'db.accounts',
      query,
      accountId: null,
      latencyMs,
      error,
      cached,
    });
  } catch (_) {}
}

// ── Cache management ─────────────────────────────────────────────────────────

function clearCache(accountId) {
  // Global query — clear all entries (accountId param unused)
  _cache.clear();
}

module.exports = { execute, clearCache };
