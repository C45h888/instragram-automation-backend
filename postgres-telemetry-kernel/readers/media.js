// substrates/db/readers/media.js
// DB Readers — media: engagement + UGC domain read operations.
//
// Owns: getRecentMedia(), getMonitoredHashtags() with per-account TTL caching.
// Does NOT own: governance policy, write operations, authentication.
//
// Reads are direct — no CK routing. DB_READ_OBSERVED is emitted as
// fire-and-forget telemetry for observability only.
//
// For governed reads, use CK.governedRead('db.media', { accountId, query }).
// The reading-substrate (control-plane/governance/domains/reading-substrate.js)
// wraps these raw methods under persist-telemetry FSM governance.

const { getSupabaseAdmin } = require('../../config/supabase');

const _recentMediaCache = new Map(); // accountId → { data, expiresAt }
const _hashtagsCache    = new Map(); // accountId → { data, expiresAt }
const RECENT_MEDIA_TTL_MS = 60 * 1000;
const HASHTAGS_TTL_MS     = 5 * 60 * 1000;

let _governance = null;

function setGovernance(gov) { _governance = gov; }

/**
 * Governed read wrapper — routes through CK → persist-telemetry FSM → reading-substrate.
 * Preferred path for new callers. Falls back to direct read if governance not wired.
 *
 * @param {string} query — 'getRecentMedia' | 'getMonitoredHashtags'
 * @param {string} accountId
 * @returns {Promise<object>} { success, data, error }
 */
async function governedRead(query, accountId) {
  if (_governance && typeof _governance.governedRead === 'function') {
    return _governance.governedRead('db.media', { accountId, query });
  }
  // Fallback: direct read (ungoverned)
  const data = query === 'getMonitoredHashtags'
    ? await getMonitoredHashtags(accountId)
    : await getRecentMedia(accountId);
  return { success: true, data, error: null, latencyMs: 0 };
}

// ── Recent Media ─────────────────────────────────────────────────────────────

async function getRecentMedia(accountId) {
  const start = Date.now();
  const cached = _recentMediaCache.get(accountId);
  if (cached && Date.now() < cached.expiresAt) {
    _emitObserved('getRecentMedia', accountId, Date.now() - start);
    return cached.data;
  }

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    _emitObserved('getRecentMedia', accountId, Date.now() - start, 'supabase_unavailable');
    return cached?.data || [];
  }

  const { data, error } = await supabase
    .from('instagram_media')
    .select('instagram_media_id')
    .eq('business_account_id', accountId)
    .order('published_at', { ascending: false })
    .limit(10);

  if (error) {
    console.warn('[db/readers] Failed to fetch recent media:', error.message);
    _emitObserved('getRecentMedia', accountId, Date.now() - start, error.message);
    return cached?.data || [];
  }

  const result = data || [];
  _recentMediaCache.set(accountId, { data: result, expiresAt: Date.now() + RECENT_MEDIA_TTL_MS });
  _emitObserved('getRecentMedia', accountId, Date.now() - start);
  return result;
}

// ── Monitored Hashtags ───────────────────────────────────────────────────────

async function getMonitoredHashtags(accountId) {
  const start = Date.now();
  const cached = _hashtagsCache.get(accountId);
  if (cached && Date.now() < cached.expiresAt) {
    _emitObserved('getMonitoredHashtags', accountId, Date.now() - start);
    return cached.data;
  }

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    _emitObserved('getMonitoredHashtags', accountId, Date.now() - start, 'supabase_unavailable');
    return cached?.data || [];
  }

  const { data, error } = await supabase
    .from('ugc_monitored_hashtags')
    .select('hashtag')
    .eq('business_account_id', accountId)
    .eq('is_active', true);

  if (error) {
    console.warn('[db/readers] Failed to fetch hashtags:', error.message);
    _emitObserved('getMonitoredHashtags', accountId, Date.now() - start, error.message);
    return cached?.data || [];
  }

  const result = (data || []).map(h => h.hashtag);
  _hashtagsCache.set(accountId, { data: result, expiresAt: Date.now() + HASHTAGS_TTL_MS });
  _emitObserved('getMonitoredHashtags', accountId, Date.now() - start);
  return result;
}

// ── Cache Invalidation ───────────────────────────────────────────────────────

function clearRecentMediaCache(accountId) {
  if (accountId) _recentMediaCache.delete(accountId);
  else _recentMediaCache.clear();
}

function clearHashtagsCache(accountId) {
  if (accountId) _hashtagsCache.delete(accountId);
  else _hashtagsCache.clear();
}

// ── Telemetry ────────────────────────────────────────────────────────────────

function _emitObserved(query, accountId, latencyMs, error = null) {
  if (!_governance) return;
  try {
    _governance.dispatch({
      type: 'DB_READ_OBSERVED',
      domain: 'media',
      query,
      accountId,
      latencyMs,
      error,
    });
  } catch (_) {}
}

module.exports = {
  getRecentMedia,
  getMonitoredHashtags,
  governedRead,
  clearRecentMediaCache,
  clearHashtagsCache,
  setGovernance,
};
