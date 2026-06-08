// postgres-telemetry-kernel/substrates/graph-capability/workers/read-scope-cache-worker.js
// Scope cache reader: one bounded SELECT on instagram_credentials.scope_cache.
//
// Owns: SELECT scope_cache, scope_cache_updated_at WHERE id = credentialId.
// Does NOT own: cache TTL logic (caller concern), signal dispatch, vault ops.

const { getSupabaseAdmin } = require('../../../../config/supabase');

/**
 * @param {{ credentialId: string }} params
 * @param {object} governance — CK reference (unused, kept for contract)
 * @returns {Promise<{ success: boolean, data?: { scope_cache: string[], scope_cache_updated_at: string }|null, error?: string }>}
 */
async function execute(params, governance) {
  const { credentialId } = params;
  if (!credentialId) {
    return { success: false, data: null, error: 'credentialId required' };
  }

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    return { success: false, data: null, error: 'supabase_unavailable' };
  }

  try {
    const { data, error } = await supabase
      .from('instagram_credentials')
      .select('scope_cache, scope_cache_updated_at')
      .eq('id', credentialId)
      .single();

    if (error) {
      if (error.code === 'PGRST116') return { success: true, data: null, error: null }; // not found = no cache
      return { success: false, data: null, error: error.message };
    }

    return { success: true, data, error: null };
  } catch (err) {
    return { success: false, data: null, error: err.message };
  }
}

module.exports = { execute };
