// postgres-telemetry-kernel/substrates/graph-capability/workers/read-scope-cache-worker.js
// Scope cache reader: SELECT on instagram_credentials.scope_cache.
//
// Owns: param validation. All Supabase I/O delegated to bedrock.
// Does NOT own: cache TTL logic, signal dispatch, vault ops.

const bedrock = require('../../../bedrock');

async function execute(params, governance) {
  const { credentialId } = params;
  if (!credentialId) {
    return { success: false, data: null, error: 'credentialId required' };
  }

  const result = await bedrock.token.getScopeCache(credentialId);

  // Map PGRST116 (row not found) → success with null data (no cache)
  if (!result.success && result.error === 'credential_not_found') {
    return { success: true, data: null, error: null };
  }

  return result;
}

module.exports = { execute };
