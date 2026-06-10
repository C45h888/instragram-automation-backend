// postgres-telemetry-kernel/substrates/graph-capability/workers/update-credential-status-worker.js
// Credential status updater: one bounded UPDATE on instagram_credentials.
//
// Owns: UPDATE is_active, debug_token_checked_at, updated_at WHERE id = credentialId.
// Does NOT own: validation (caller concern), signal dispatch, audit logging,
//               failure classification (persistence-failure-substrate),
//               retry policy (retry-cadence-kernel).

const { getSupabaseAdmin } = require('../../../../config/supabase');

/**
 * @param {{
 *   credentialId: string,
 *   isActive?: boolean,
 *   debugTokenChecked?: boolean,
 * }} params
 * @param {object} governance — CK reference (used to emit DB_WRITE_FAILED on failure)
 * @returns {Promise<{ success: boolean, error?: string }>}
 */
async function execute(params, governance) {
  // FSM passes { domain, accountId, intentId, table, rows }.
  // Operation-specific fields are in rows[0].
  const { domain, accountId, intentId, table, rows } = params;
  const row = (rows && rows[0]) || {};
  const { credentialId, isActive, debugTokenChecked } = row;
  if (!credentialId) {
    const err = 'credentialId required';
    governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId, table: 'instagram_credentials', count: 0, rows, error: err, rawError: { message: err }, workerName: 'update-credential-status-worker', lineageId: intentId, primaryKeyField: 'id', primaryKeyValue: credentialId, attemptN: 1, operation: 'update', source: 'supabase' });
    return { success: false, error: err };
  }

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    const err = 'supabase_unavailable';
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

  try {
    const { error } = await supabase
      .from('instagram_credentials')
      .update(updates)
      .eq('id', credentialId);

    if (error) {
      governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId, table: 'instagram_credentials', count: 0, rows, error: error.message, rawError: error, workerName: 'update-credential-status-worker', lineageId: intentId, primaryKeyField: 'id', primaryKeyValue: credentialId, attemptN: 1, operation: 'update', source: 'supabase' });
      return { success: false, error: error.message };
    }
    return { success: true, error: null };
  } catch (err) {
    governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId, table: 'instagram_credentials', count: 0, rows, error: err.message, rawError: err, workerName: 'update-credential-status-worker', lineageId: intentId, primaryKeyField: 'id', primaryKeyValue: credentialId, attemptN: 1, operation: 'update', source: 'supabase' });
    return { success: false, error: err.message };
  }
}

module.exports = { execute };
