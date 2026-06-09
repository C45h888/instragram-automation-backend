// postgres-telemetry-kernel/substrates/graph-capability/workers/read-credential-worker.js
// Credential reader: governed SELECTs on instagram_credentials.
//
// Owns: single-cred lookup (SELECT * WHERE user_id, business_account_id, token_type,
//        is_active=true) and batch scan (scanActivePageCredentials — SELECT active
//        page credentials, optionally filtered by business_account_id).
// Does NOT own: decryption (vault concern — caller does decrypt RPC separately),
//               signal dispatch, cache invalidation.
//
// Query types:
//   default (no query param) — single-cred lookup, requires userId + businessAccountId
//   scanActivePageCredentials — batch scan, returns array of active page credentials
//   scanExpiringUATs — batch scan, returns user-type creds expiring within windowDays
//   scanDataAccessExpiry — batch scan, returns user-type creds with data_access_expires_at within windowDays
//
// Dispatched via: CK.governedRead('db.credential', params)

const { getSupabaseAdmin } = require('../../../../config/supabase');

/**
 * @param {object} params
 * @param {string} [params.query] — 'scanActivePageCredentials' | 'scanExpiringUATs' | 'scanDataAccessExpiry'
 * @param {string} [params.userId] — required for single-cred lookup
 * @param {string} [params.businessAccountId] — required for single-cred; optional filter for scans
 * @param {string} [params.tokenType] — default 'page' for single-cred lookup
 * @param {number} [params.windowDays] — expiry window in days (default 14, only for scanExpiringUATs / scanDataAccessExpiry)
 * @param {object} governance — CK reference (unused, kept for contract)
 * @returns {Promise<{ success: boolean, data?: object|Array|null, error?: string }>}
 */
async function execute(params, governance) {
  const supabase = getSupabaseAdmin();
  if (!supabase) {
    return { success: false, data: null, error: 'supabase_unavailable' };
  }

  // ── Batch scan: active page credentials ──────────────────────────────────
  if (params.query === 'scanActivePageCredentials') {
    try {
      let query = supabase
        .from('instagram_credentials')
        .select('id, user_id, business_account_id, debug_token_checked_at, issued_at')
        .eq('token_type', 'page')
        .eq('is_active', true);

      if (params.businessAccountId) {
        query = query.eq('business_account_id', params.businessAccountId);
      }

      const { data, error } = await query;
      if (error) return { success: false, data: null, error: error.message };
      return { success: true, data: data || [], error: null };
    } catch (err) {
      return { success: false, data: null, error: err.message };
    }
  }

  // ── Batch scan: expiring UATs ────────────────────────────────────────────
  if (params.query === 'scanExpiringUATs') {
    try {
      const windowDays = params.windowDays || 14;
      const cutoff = new Date(Date.now() + windowDays * 24 * 60 * 60 * 1000).toISOString();

      let query = supabase
        .from('instagram_credentials')
        .select('id, user_id, business_account_id, expires_at')
        .eq('token_type', 'user')
        .eq('is_active', true)
        .not('expires_at', 'is', null)
        .lt('expires_at', cutoff);

      if (params.businessAccountId) {
        query = query.eq('business_account_id', params.businessAccountId);
      }

      const { data, error } = await query;
      if (error) return { success: false, data: null, error: error.message };
      return { success: true, data: data || [], error: null };
    } catch (err) {
      return { success: false, data: null, error: err.message };
    }
  }

  // ── Batch scan: data_access_expires_at within window ──────────────────────
  if (params.query === 'scanDataAccessExpiry') {
    try {
      const windowDays = params.windowDays || 30;
      const cutoff = new Date(Date.now() + windowDays * 24 * 60 * 60 * 1000).toISOString();

      let query = supabase
        .from('instagram_credentials')
        .select('id, user_id, business_account_id, data_access_expires_at')
        .eq('token_type', 'user')
        .eq('is_active', true)
        .not('data_access_expires_at', 'is', null)
        .lt('data_access_expires_at', cutoff);

      if (params.businessAccountId) {
        query = query.eq('business_account_id', params.businessAccountId);
      }

      const { data, error } = await query;
      if (error) return { success: false, data: null, error: error.message };
      return { success: true, data: data || [], error: null };
    } catch (err) {
      return { success: false, data: null, error: err.message };
    }
  }

  // ── Single-cred lookup (default) ─────────────────────────────────────────

  // ── getCredentialPageId — page_id only, no userId required ──────────────
  if (params.query === 'getCredentialPageId') {
    if (!params.businessAccountId) {
      return { success: false, data: null, error: 'businessAccountId required' };
    }
    try {
      const { data, error } = await supabase
        .from('instagram_credentials')
        .select('page_id')
        .eq('business_account_id', params.businessAccountId)
        .eq('token_type', 'page')
        .eq('is_active', true)
        .maybeSingle();
      if (error) return { success: false, data: null, error: error.message };
      return { success: true, data: data?.page_id || null, error: null };
    } catch (err) {
      return { success: false, data: null, error: err.message };
    }
  }

  // ── Single-cred lookup (default, no query param) ────────────────────────
  const { userId, businessAccountId, tokenType = 'page' } = params;
  if (!userId || !businessAccountId) {
    return { success: false, data: null, error: 'userId and businessAccountId required' };
  }

  try {
    const { data, error } = await supabase
      .from('instagram_credentials')
      .select('*')
      .eq('user_id', userId)
      .eq('business_account_id', businessAccountId)
      .eq('token_type', tokenType)
      .eq('is_active', true)
      .single();

    if (error) {
      if (error.code === 'PGRST116') return { success: false, data: null, error: 'credential_not_found' };
      return { success: false, data: null, error: error.message };
    }

    return { success: true, data, error: null };
  } catch (err) {
    return { success: false, data: null, error: err.message };
  }
}

module.exports = { execute };
