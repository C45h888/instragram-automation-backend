// graph-capability-kernel/substrates/credential-resolver.js
// Credential Resolver: canonical credential resolution for Instagram API calls.
//
// Owns: resolving business_account_id UUID → {igUserId, pageToken, userId, pageId},
//       capability gating via verdict-gate, token retrieval via vault, caching.
// Does NOT own: IG API transport, DB writes, orchestration, retry decisions.
//
// Extracted from helpers/agent-helpers.js (decomposed).

const { getSupabaseAdmin, logAudit, shouldLog } = require('../../config/supabase');
const fsm = require('../fsm');
const vault = require('./vault');
const { clearCredentialCache: _clearCredentialCacheRaw, getFromCache, setInCache } = require('../../helpers/credential-cache');

// ═══════════════════════════════════════════════════════════════════════════════
// CREDENTIAL RESOLUTION
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Resolves business_account_id UUID to Instagram credentials.
 * @param {string} businessAccountId - UUID from instagram_business_accounts table
 * @returns {Promise<{igUserId: string, pageToken: string, userId: string}>}
 * @throws {Error} If account not found or token retrieval fails
 */
async function resolveAccountCredentials(businessAccountId) {
  const cached = getFromCache(businessAccountId);
  if (cached) return cached;

  try {
    const supabase = getSupabaseAdmin();
    if (!supabase) {
      throw new Error('Database not available');
    }

    const { data: account, error } = await supabase
      .from('instagram_business_accounts')
      .select('instagram_business_id, user_id, is_connected, username')
      .eq('id', businessAccountId)
      .single();

    if (error || !account) {
      throw new Error(`Business account not found: ${businessAccountId}`);
    }

    if (!account.is_connected) {
      throw new Error('Business account is disconnected');
    }

    const igUserId = account.instagram_business_id;
    const userId = account.user_id;

    // Capability gate first — deny if FSM verdict is not AUTHORIZED/LIMITED/DEGRADED with required scopes
    const verdict = await fsm.requireCapability(businessAccountId, [
      'instagram_basic',
      'pages_read_engagement'
    ]);
    if (!verdict.allowed) {
      throw new Error(`CAPABILITY_DENIED: ${verdict.reason} (state=${verdict.state})`);
    }

    const pageToken = await vault.pat.retrieve({ userId, businessAccountId });

    if (!pageToken) {
      throw new Error('Failed to retrieve access token');
    }

    // Fetch page_id from credential row — stored by storePageToken, needed for pages_* scoped ops
    let pageId = null;
    try {
      const { data: cred } = await supabase
        .from('instagram_credentials')
        .select('page_id')
        .eq('business_account_id', businessAccountId)
        .eq('token_type', 'page')
        .eq('is_active', true)
        .maybeSingle();
      pageId = cred?.page_id || null;
    } catch (pageIdErr) {
      console.warn('⚠️ page_id lookup failed (non-blocking):', pageIdErr.message);
    }

    const result = { igUserId, pageToken, userId, pageId, igUsername: account.username || null };
    setInCache(businessAccountId, result);
    return result;
  } catch (error) {
    console.error('❌ Credential resolution failed:', error.message);
    throw error;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// CREDENTIAL CACHE MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Clears the credential cache for an account.
 * Wraps the raw cache clear with optional debug audit logging.
 * @param {string} businessAccountId
 * @param {string} [reason] - why the cache is being cleared (for debug logging)
 */
function clearCredentialCache(businessAccountId, reason = 'explicit') {
  _clearCredentialCacheRaw(businessAccountId);
  if (shouldLog('debug')) {
    logAudit({
      event_type: 'credential_cache_cleared_debug',
      action: 'cache_clear',
      resource_type: 'credential',
      details: { account_id: businessAccountId, reason },
    }).catch(() => {});
  }
}

module.exports = { resolveAccountCredentials, clearCredentialCache };
