// substrates/persistence.js
// LEGACY SUBSTRATE — retained for account discovery only during migration.
//
// Everything else has been migrated:
//   - Batch writers (store*Batch) → substrates/db/writers/ via dispatchWrite
//   - Normalizers (normalize*, transform*) → caller substrates (engagement, insights, content)
//   - syncHashtagsFromCaptions → deferred to Phase 4 enrichment membrane
//   - ensureConversationRows → deferred to Phase 4 repair membrane
//
// Remaining consumers of THIS FILE:
//   - control-plane/runtime/signal-intake.js     (getActiveAccounts)
//   - control-plane/runtime/lifecycle.js         (getActiveAccounts)
//   - control-plane/orchestration/cadence-orchestrator.js (getActiveAccounts)
//   - control-plane/orchastrator.js              (getActiveAccounts)
//   - control-plane/orchestration/acquisition-orchestrator.js (resolveAccountCredentials re-export)
//
// All of the above migrate to db kernel in Phase 2.
// This file is deleted after Phase 2.

const { getSupabaseAdmin } = require('../config/supabase');
const { resolveAccountCredentials } = require('../helpers/agent-helpers');
const { _setClearAccountsCache } = require('./retry');

// ── TTL Caches ───────────────────────────────────────────────────────────────

let _accountsCache = { data: [], expiresAt: 0 };
const ACCOUNTS_CACHE_TTL_MS = 30 * 1000;

// Wire retry substrate's cache clear hook
if (_setClearAccountsCache) {
  _setClearAccountsCache(() => { _accountsCache = { data: [], expiresAt: 0 }; });
}

// ═══════════════════════════════════════════════════════════════════════════════
// ACCOUNT DISCOVERY
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Returns active business accounts from DB, with 30s TTL cache.
 * Used by runtime bootstrap and lifecycle refresh.
 *
 * Migrates to: substrates/db/reading/workers/accounts-worker.js (Phase 2)
 */
async function getActiveAccounts() {
  if (Date.now() < _accountsCache.expiresAt) return _accountsCache.data;

  const supabase = getSupabaseAdmin();
  if (!supabase) return [];

  const { data, error } = await supabase
    .from('instagram_business_accounts')
    .select('id, instagram_business_id, user_id')
    .eq('is_connected', true)
    .eq('connection_status', 'active');

  if (error) {
    console.error('[persistence] Failed to fetch active accounts:', error.message);
    return _accountsCache.data;
  }

  _accountsCache = { data: data || [], expiresAt: Date.now() + ACCOUNTS_CACHE_TTL_MS };
  return _accountsCache.data;
}

// ═══════════════════════════════════════════════════════════════════════════════
// CREDENTIAL RESOLUTION — re-export
// ═══════════════════════════════════════════════════════════════════════════════

// Authority is substrates/vault. Re-export kept for backward compatibility
// during migration. All consumers migrate to vault directly in Phase 2.
module.exports.resolveAccountCredentials = resolveAccountCredentials;

// ═══════════════════════════════════════════════════════════════════════════════

module.exports = {
  getActiveAccounts,
  resolveAccountCredentials,  // re-export
};
