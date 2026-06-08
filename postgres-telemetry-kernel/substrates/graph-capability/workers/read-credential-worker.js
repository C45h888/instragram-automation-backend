// postgres-telemetry-kernel/substrates/graph-capability/workers/read-credential-worker.js
// Credential reader: one bounded SELECT on instagram_credentials.
//
// Owns: SELECT * WHERE user_id, business_account_id, token_type, is_active=true.
// Does NOT own: decryption (vault concern — caller does decrypt RPC separately),
//               signal dispatch, cache invalidation.

const { getSupabaseAdmin } = require('../../../../config/supabase');

/**
 * @param {{ userId: string, businessAccountId: string, tokenType?: string }} params
 * @param {object} governance — CK reference (unused, kept for contract)
 * @returns {Promise<{ success: boolean, data?: object|null, error?: string }>}
 */
async function execute(params, governance) {
  const { userId, businessAccountId, tokenType = 'page' } = params;
  if (!userId || !businessAccountId) {
    return { success: false, data: null, error: 'userId and businessAccountId required' };
  }

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    return { success: false, data: null, error: 'supabase_unavailable' };
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
