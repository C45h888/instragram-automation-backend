// substrates/db/reading/workers/post-queue-worker.js
// Post Queue + Scheduled Posts Worker: governed Supabase reads for the publishing domain.
//
// Owns: getPendingPostQueue(), getApprovedScheduledPosts() with FSM-governed caching.
// Does NOT own: governance policy (FSM), routing (CK), IG API calls, status mutations.
//
// This worker is the pull-based bridge between the cognition layer and the publishing FSM.
// Cognition-scanner provides push-based Realtime triggers; this worker provides
// the pull-fallback — querying the DB directly for publishable items on demand.
//
// Operationally bounded to: db.post-queue, db.scheduled-posts read domains.
// Dispatched by: substrates/db/reading/index.js
//
// Replaces: substrates/db/writers/publishing-writer.js (status mutations removed —
//           cognition-scanner's Realtime subscriptions are the sole trigger source;
//           this reader provides the governed pull path).

const { getSupabaseAdmin } = require('../../../../config/supabase');

// ── Per-worker cache ────────────────────────────────────────────────────────
const _cache = new Map(); // key → { data, expiresAt }

const CACHE_TTL_MS = {
  getPendingPostQueue:         60_000,  // 1 min
  getApprovedScheduledPosts:  120_000,  // 2 min (scheduled posts change less often)
};

function _cacheKey(accountId, query) {
  return `${query}:${accountId}`;
}

function _getCached(accountId, query) {
  const key = _cacheKey(accountId, query);
  const entry = _cache.get(key);
  if (entry && Date.now() < entry.expiresAt) return entry.data;
  if (entry) _cache.delete(key);
  return null;
}

function _setCache(accountId, query, data) {
  const ttl = CACHE_TTL_MS[query] || 60_000;
  const key = _cacheKey(accountId, query);
  _cache.set(key, { data, expiresAt: Date.now() + ttl });
}

// ── Execute ──────────────────────────────────────────────────────────────────

/**
 * Execute a governed DB read for the publishing domain.
 *
 * @param {object} params     — { accountId, query: 'getPendingPostQueue'|'getApprovedScheduledPosts' }
 * @param {object} governance — CK module (for DB_READ_OBSERVED emission)
 * @returns {Promise<{success: boolean, data?, error?, latencyMs: number, cached?: boolean}>}
 */
async function execute(params, governance) {
  const { accountId, query } = params;
  const startTime = Date.now();

  // ── Cache hit ────────────────────────────────────────────────────────────
  const cached = _getCached(accountId, query);
  if (cached !== null) {
    _emitObserved(governance, accountId, query, Date.now() - startTime, null, true);
    return { success: true, data: cached, error: null, latencyMs: Date.now() - startTime, cached: true };
  }

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    const elapsed = Date.now() - startTime;
    _emitObserved(governance, accountId, query, elapsed, 'supabase_unavailable');
    return { success: false, data: null, error: 'supabase_unavailable', latencyMs: elapsed };
  }

  try {
    let data;

    if (query === 'getApprovedScheduledPosts') {
      const result = await supabase
        .from('scheduled_posts')
        .select('*')
        .eq('business_account_id', accountId)
        .eq('status', 'approved')
        .order('scheduled_at', { ascending: true });
      if (result.error) throw result.error;
      data = result.data || [];
    } else {
      // default: getPendingPostQueue
      const result = await supabase
        .from('post_queue')
        .select('*')
        .eq('business_account_id', accountId)
        .in('status', ['pending', 'failed'])
        .order('created_at', { ascending: true });
      if (result.error) throw result.error;
      data = result.data || [];
    }

    const elapsed = Date.now() - startTime;
    _setCache(accountId, query, data);
    _emitObserved(governance, accountId, query, elapsed);
    return { success: true, data, error: null, latencyMs: elapsed };

  } catch (err) {
    const elapsed = Date.now() - startTime;
    _emitObserved(governance, accountId, query, elapsed, err.message);
    return { success: false, data: null, error: err.message, latencyMs: elapsed };
  }
}

// ── Telemetry ────────────────────────────────────────────────────────────────

function _emitObserved(governance, accountId, query, latencyMs, error = null, cached = false) {
  if (!governance || typeof governance.dispatch !== 'function') return;
  try {
    governance.dispatch({
      type: 'DB_READ_OBSERVED',
      domain: query.startsWith('getPending') ? 'db.post-queue' : 'db.scheduled-posts',
      query,
      accountId,
      latencyMs,
      error,
      cached,
    });
  } catch (_) {}
}

// ── Cache management ─────────────────────────────────────────────────────────

function clearCache(accountId) {
  if (accountId) {
    for (const key of _cache.keys()) {
      if (key.endsWith(`:${accountId}`)) _cache.delete(key);
    }
  } else {
    _cache.clear();
  }
}

module.exports = { execute, clearCache };
