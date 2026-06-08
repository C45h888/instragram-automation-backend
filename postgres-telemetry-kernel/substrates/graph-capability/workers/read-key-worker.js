// postgres-telemetry-kernel/substrates/graph-capability/workers/read-key-worker.js
// Encryption key reader: one bounded SELECT on instagram_business_accounts.
//
// Owns: SELECT encryption_key_id WHERE user_id, id = businessAccountId.
// Does NOT own: key provisioning, encryption RPC (vault concern), signal dispatch.

const { getSupabaseAdmin } = require('../../../../config/supabase');

/**
 * @param {{ userId: string, businessAccountId: string }} params
 * @param {object} governance — CK reference (unused, kept for contract)
 * @returns {Promise<{ success: boolean, data?: { encryption_key_id: string|null }|null, error?: string }>}
 */
async function execute(params, governance) {
  const { userId, businessAccountId } = params;
  if (!userId || !businessAccountId) {
    return { success: false, data: null, error: 'userId and businessAccountId required' };
  }

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    return { success: false, data: null, error: 'supabase_unavailable' };
  }

  try {
    const { data, error } = await supabase
      .from('instagram_business_accounts')
      .select('encryption_key_id')
      .eq('user_id', userId)
      .eq('id', businessAccountId)
      .maybeSingle();

    if (error) {
      return { success: false, data: null, error: error.message };
    }

    return { success: true, data: data || { encryption_key_id: null }, error: null };
  } catch (err) {
    return { success: false, data: null, error: err.message };
  }
}

module.exports = { execute };
