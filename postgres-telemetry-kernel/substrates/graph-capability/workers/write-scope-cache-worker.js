// postgres-telemetry-kernel/substrates/graph-capability/workers/write-scope-cache-worker.js
// Scope cache writer: one bounded UPDATE on instagram_credentials.
//
// Owns: UPDATE scope_cache, scope_cache_updated_at WHERE id = credentialId.
// Does NOT own: cache TTL logic (caller concern), signal dispatch, vault ops.

const { getSupabaseAdmin } = require('../../../../config/supabase');

/**
 * @param {{ credentialId: string, scopes: string[] }} params
 * @param {object} governance — CK reference (unused, kept for contract)
 * @returns {Promise<{ success: boolean, error?: string }>}
 */
async function execute(params, governance) {
  // FSM passes { domain, accountId, intentId, table, rows }.
  // Operation-specific fields are in rows[0].
  const { rows } = params;
  const row = (rows && rows[0]) || {};
  const { credentialId, scopes } = row;
  if (!credentialId) {
    return { success: false, error: 'credentialId required' };
  }
  if (!Array.isArray(scopes) || scopes.length === 0) {
    return { success: false, error: 'scopes required' };
  }

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    return { success: false, error: 'supabase_unavailable' };
  }

  try {
    const { error } = await supabase
      .from('instagram_credentials')
      .update({
        scope_cache: scopes,
        scope_cache_updated_at: new Date().toISOString(),
      })
      .eq('id', credentialId);

    if (error) return { success: false, error: error.message };
    return { success: true, error: null };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

module.exports = { execute };
