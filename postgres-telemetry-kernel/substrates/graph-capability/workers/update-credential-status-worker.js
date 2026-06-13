// postgres-telemetry-kernel/substrates/graph-capability/workers/update-credential-status-worker.js
// Credential status updater: one bounded UPDATE on instagram_credentials.
//
// Owns: param validation + field assembly. All Supabase I/O delegated to bedrock.
// Does NOT own: signal dispatch, audit logging, failure classification, retry.

const bedrock = require('../../../bedrock');

async function execute(params, governance) {
  const { domain, accountId, intentId, rows } = params;
  const row = (rows && rows[0]) || {};
  const { credentialId, isActive, debugTokenChecked } = row;
  if (!credentialId) {
    const err = 'credentialId required';
    governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId, table: 'instagram_credentials', count: 0, rows, error: err, rawError: { message: err }, workerName: 'update-credential-status-worker', lineageId: intentId, primaryKeyField: 'id', primaryKeyValue: credentialId, attemptN: 1, operation: 'update', source: 'supabase' });
    return { success: false, error: err };
  }

  const updates = { updated_at: new Date().toISOString() };
  if (typeof isActive === 'boolean') updates.is_active = isActive;
  if (debugTokenChecked) updates.debug_token_checked_at = new Date().toISOString();

  if (Object.keys(updates).length <= 1) {
    const err = 'no fields to update';
    governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId, table: 'instagram_credentials', count: 0, rows, error: err, rawError: { message: err }, workerName: 'update-credential-status-worker', lineageId: intentId, primaryKeyField: 'id', primaryKeyValue: credentialId, attemptN: 1, operation: 'update', source: 'supabase' });
    return { success: false, error: err };
  }

  const result = await bedrock.token.updateCredentialStatus(credentialId, updates, {
    accountId, intentId, governance, domain,
  });

  if (!result.success) {
    governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId, table: 'instagram_credentials', count: 0, rows, error: result.error, rawError: { message: result.error }, workerName: 'update-credential-status-worker', lineageId: intentId, primaryKeyField: 'id', primaryKeyValue: credentialId, attemptN: 1, operation: 'update', source: 'supabase' });
    return { success: false, error: result.error };
  }

  return { success: true, error: null };
}

module.exports = { execute };
