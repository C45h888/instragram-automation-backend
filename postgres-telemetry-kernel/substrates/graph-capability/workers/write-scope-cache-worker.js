// postgres-telemetry-kernel/substrates/graph-capability/workers/write-scope-cache-worker.js
// Scope cache writer: UPDATE on instagram_credentials.
//
// Owns: param validation. All Supabase I/O delegated to bedrock.
// Does NOT own: cache TTL logic, signal dispatch, vault ops.

const bedrock = require('../../../bedrock');

async function execute(params, governance) {
  const { domain, accountId, intentId, rows } = params;
  const row = (rows && rows[0]) || {};
  const { credentialId, scopes } = row;
  if (!credentialId) {
    const err = 'credentialId required';
    governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId, table: 'instagram_credentials', count: 0, rows, error: err, rawError: { message: err }, workerName: 'write-scope-cache-worker', lineageId: intentId, primaryKeyField: 'id', primaryKeyValue: credentialId, attemptN: 1, operation: 'write', source: 'supabase' });
    return { success: false, error: err };
  }
  if (!Array.isArray(scopes) || scopes.length === 0) {
    const err = 'scopes required';
    governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId, table: 'instagram_credentials', count: 0, rows, error: err, rawError: { message: err }, workerName: 'write-scope-cache-worker', lineageId: intentId, primaryKeyField: 'id', primaryKeyValue: credentialId, attemptN: 1, operation: 'write', source: 'supabase' });
    return { success: false, error: err };
  }

  const result = await bedrock.token.updateScopeCache(credentialId, scopes, {
    accountId, intentId, governance, domain,
  });

  if (!result.success) {
    governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId, table: 'instagram_credentials', count: 0, rows, error: result.error, rawError: { message: result.error }, workerName: 'write-scope-cache-worker', lineageId: intentId, primaryKeyField: 'id', primaryKeyValue: credentialId, attemptN: 1, operation: 'write', source: 'supabase' });
    return { success: false, error: result.error };
  }

  return { success: true, error: null };
}

module.exports = { execute };
