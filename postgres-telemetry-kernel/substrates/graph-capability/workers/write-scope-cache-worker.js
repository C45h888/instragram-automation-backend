// postgres-telemetry-kernel/substrates/graph-capability/workers/write-scope-cache-worker.js
// Scope cache writer: one bounded UPDATE on instagram_credentials.
//
// Owns: UPDATE scope_cache, scope_cache_updated_at WHERE id = credentialId.
// Does NOT own: cache TTL logic (caller concern), signal dispatch, vault ops,
//               failure classification (persistence-failure-substrate),
//               retry policy (retry-cadence-kernel).

const { getSupabaseAdmin } = require('../../../../config/supabase');
const { analyzeFailure } = require('../../../persistence-failure-substrate');

/**
 * @param {{ credentialId: string, scopes: string[] }} params
 * @param {object} governance — CK reference (used to emit DB_WRITE_FAILED on failure)
 * @returns {Promise<{ success: boolean, error?: string }>}
 */
async function execute(params, governance) {
  // FSM passes { domain, accountId, intentId, table, rows }.
  // Operation-specific fields are in rows[0].
  const { domain, accountId, intentId, table, rows } = params;
  const row = (rows && rows[0]) || {};
  const { credentialId, scopes } = row;
  if (!credentialId) {
    const err = 'credentialId required';
    const analysis = analyzeFailure({ message: err }, 'write', 'supabase', { attemptN: 1, lineageId: intentId, workerName: 'write-scope-cache-worker', primaryKeyField: 'id', primaryKeyValue: credentialId });
    governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId, table: 'instagram_credentials', count: 0, rows, analysis, errorShape: { category: analysis.category, subtype: analysis.subtype, retryable: analysis.retryable, retryAfterMs: analysis.rateLimit.retryAfterMs }, error: err });
    return { success: false, error: err };
  }
  if (!Array.isArray(scopes) || scopes.length === 0) {
    const err = 'scopes required';
    const analysis = analyzeFailure({ message: err }, 'write', 'supabase', { attemptN: 1, lineageId: intentId, workerName: 'write-scope-cache-worker', primaryKeyField: 'id', primaryKeyValue: credentialId });
    governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId, table: 'instagram_credentials', count: 0, rows, analysis, errorShape: { category: analysis.category, subtype: analysis.subtype, retryable: analysis.retryable, retryAfterMs: analysis.rateLimit.retryAfterMs }, error: err });
    return { success: false, error: err };
  }

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    const err = 'supabase_unavailable';
    const analysis = analyzeFailure({ message: err }, 'write', 'supabase', { attemptN: 1, lineageId: intentId, workerName: 'write-scope-cache-worker', primaryKeyField: 'id', primaryKeyValue: credentialId });
    governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId, table: 'instagram_credentials', count: 0, rows, analysis, errorShape: { category: analysis.category, subtype: analysis.subtype, retryable: analysis.retryable, retryAfterMs: analysis.rateLimit.retryAfterMs }, error: err });
    return { success: false, error: err };
  }

  try {
    const { error } = await supabase
      .from('instagram_credentials')
      .update({
        scope_cache: scopes,
        scope_cache_updated_at: new Date().toISOString(),
      })
      .eq('id', credentialId);

    if (error) {
      const analysis = analyzeFailure(error, 'write', 'supabase', { attemptN: 1, lineageId: intentId, workerName: 'write-scope-cache-worker', primaryKeyField: 'id', primaryKeyValue: credentialId });
      governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId, table: 'instagram_credentials', count: 0, rows, analysis, errorShape: { category: analysis.category, subtype: analysis.subtype, retryable: analysis.retryable, retryAfterMs: analysis.rateLimit.retryAfterMs }, error: error.message });
      return { success: false, error: error.message };
    }
    return { success: true, error: null };
  } catch (err) {
    const analysis = analyzeFailure(err, 'write', 'supabase', { attemptN: 1, lineageId: intentId, workerName: 'write-scope-cache-worker', primaryKeyField: 'id', primaryKeyValue: credentialId });
    governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId, table: 'instagram_credentials', count: 0, rows, analysis, errorShape: { category: analysis.category, subtype: analysis.subtype, retryable: analysis.retryable, retryAfterMs: analysis.rateLimit.retryAfterMs }, error: err.message });
    return { success: false, error: err.message };
  }
}

module.exports = { execute };
