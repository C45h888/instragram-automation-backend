// graph-capability-kernel/substrates/credential-resolver.js
// Credential Resolver: canonical credential resolution for Instagram API calls.
//
// Owns: resolving business_account_id UUID → {igUserId, pageToken, userId, pageId, igUsername},
//       capability gating via verdict-gate, token retrieval via vault, caching.
// Does NOT own: IG API transport, DB reads (delegated to CK.governedRead),
//               DB writes, orchestration, retry decisions.
//
// DB reads route through: fsm.getGovernance().governedRead()
//   → CK → persist-telemetry FSM → reading-substrate → worker → Supabase
//   → READ_RESULT_AVAILABLE → Promise resolves
//
// Extracted from helpers/agent-helpers.js (decomposed).

const fsm = require('../fsm');
const vault = require('./vault');
const { logAudit, shouldLog } = require('../../config/supabase');
const { clearCredentialCache: _clearCredentialCacheRaw, getFromCache, setInCache } = require('../../helpers/credential-cache');

// ═══════════════════════════════════════════════════════════════════════════════
// CREDENTIAL RESOLUTION
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Resolves business_account_id UUID to Instagram credentials.
 * All DB reads flow through CK.governedRead → persist-telemetry FSM.
 * @param {string} businessAccountId - UUID from instagram_business_accounts table
 * @returns {Promise<{igUserId: string, pageToken: string, userId: string}>}
 * @throws {Error} If account not found or token retrieval fails
 */
async function resolveAccountCredentials(businessAccountId) {
  const cached = getFromCache(businessAccountId);
  if (cached) return cached;

  try {
    const ck = fsm.getGovernance();
    if (!ck || typeof ck.governedRead !== 'function') {
      throw new Error('Governance not available — GCFSM not bootstrapped');
    }

    // ── Business account lookup (constitutional) ──────────────────────────
    const baResult = await ck.governedRead('db.accounts', {
      query: 'getBusinessAccount',
      businessAccountId,
    });

    if (!baResult.success || !baResult.data) {
      throw new Error(`Business account not found: ${businessAccountId}`);
    }

    const account = baResult.data;
    if (!account.is_connected) {
      throw new Error('Business account is disconnected');
    }

    const igUserId = account.instagram_business_id;
    const userId = account.user_id;

    // ── Credential page_id lookup (constitutional) ────────────────────────
    const pageResult = await ck.governedRead('db.credential', {
      query: 'getCredentialPageId',
      businessAccountId,
    });
    const pageId = pageResult.success ? pageResult.data : null;

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
