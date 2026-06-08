// postgres-telemetry-kernel/substrates/graph-capability/workers/update-credential-status-worker.js
// Credential status updater: one bounded UPDATE on instagram_credentials.
//
// Owns: UPDATE is_active, debug_token_checked_at, updated_at WHERE id = credentialId.
// Does NOT own: validation (caller concern), signal dispatch, audit logging.

const { getSupabaseAdmin } = require('../../../../config/supabase');

/**
 * @param {{
 *   credentialId: string,
 *   isActive?: boolean,
 *   debugTokenChecked?: boolean,
 * }} params
 * @param {object} governance — CK reference (unused, kept for contract)
 * @returns {Promise<{ success: boolean, error?: string }>}
 */
async function execute(params, governance) {
  // FSM passes { domain, accountId, intentId, table, rows }.
  // Operation-specific fields are in rows[0].
  const { rows } = params;
  const row = (rows && rows[0]) || {};
  const { credentialId, isActive, debugTokenChecked } = row;
  if (!credentialId) {
    return { success: false, error: 'credentialId required' };
  }

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    return { success: false, error: 'supabase_unavailable' };
  }

  const updates = { updated_at: new Date().toISOString() };
  if (typeof isActive === 'boolean') updates.is_active = isActive;
  if (debugTokenChecked) updates.debug_token_checked_at = new Date().toISOString();

  if (Object.keys(updates).length <= 1) {
    return { success: false, error: 'no fields to update' };
  }

  try {
    const { error } = await supabase
      .from('instagram_credentials')
      .update(updates)
      .eq('id', credentialId);

    if (error) return { success: false, error: error.message };
    return { success: true, error: null };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

module.exports = { execute };
