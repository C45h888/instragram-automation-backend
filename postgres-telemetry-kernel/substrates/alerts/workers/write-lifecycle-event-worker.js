// postgres-telemetry-kernel/substrates/alerts/workers/write-lifecycle-event-worker.js
// Lifecycle event writer: one bounded INSERT into token_lifecycle_events.
//
// Owns: INSERT INTO token_lifecycle_events (credential_id, business_account_id, event_type, token_age_days, details).
// Does NOT own: dedup logic (caller concern), signal dispatch, audit logging,
//               failure classification (persistence-failure-substrate),
//               retry policy (retry-cadence-kernel).
//
// FSM contract: receives { domain, accountId, intentId, table, rows } from persist-telemetry FSM.
//   rows[0] = { credential_id, business_account_id, event_type, token_age_days, details }.
// Emits DB_WRITE_COMPLETE on success, DB_WRITE_FAILED on failure (with errorShape).
//
// Best-effort: fire-and-forget semantics. The error is reported via governance;
//   retry cadence is owned by retry-cadence-kernel.

const { getSupabaseAdmin } = require('../../../../config/supabase');
const { analyzeFailure } = require('../../../persistence-failure-substrate');

/**
 * @param {{ domain: string, accountId: string, intentId?: string, table: string, rows: Array }} params
 * @param {object} governance — CK reference (for DB_WRITE_COMPLETE / DB_WRITE_FAILED emission)
 */
async function execute(params, governance) {
   const { domain, accountId, intentId, table, rows } = params;
   const row = (rows && rows[0]) || {};
   const { credential_id, business_account_id, event_type, token_age_days, details } = row;

   if (!credential_id || !event_type) {
     const err = 'credential_id and event_type are required';
     const analysis = analyzeFailure({ message: err }, 'write', 'supabase', { attemptN: 1, lineageId: intentId, workerName: 'write-lifecycle-event-worker', primaryKeyField: 'credential_id', primaryKeyValue: credential_id });
     governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId,
       table, count: 0, rows, analysis, errorShape: { category: analysis.category, subtype: analysis.subtype, retryable: analysis.retryable, retryAfterMs: analysis.rateLimit.retryAfterMs }, error: err });
     return { success: false, error: err };
   }

   const supabase = getSupabaseAdmin();
   if (!supabase) {
     const err = 'supabase_unavailable';
     const analysis = analyzeFailure({ message: err }, 'write', 'supabase', { attemptN: 1, lineageId: intentId, workerName: 'write-lifecycle-event-worker', primaryKeyField: 'credential_id', primaryKeyValue: credential_id });
     governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId,
       table, count: 0, rows, analysis, errorShape: { category: analysis.category, subtype: analysis.subtype, retryable: analysis.retryable, retryAfterMs: analysis.rateLimit.retryAfterMs }, error: err });
     return { success: false, error: err };
   }

   try {
     const { error } = await supabase
       .from('token_lifecycle_events')
       .insert({
         credential_id,
         business_account_id: business_account_id || null,
         event_type,
         token_age_days: token_age_days ?? null,
         details: details || {},
       });

     if (error) {
       console.warn('[write-lifecycle-event-worker] Insert failed:', error.message);
       const analysis = analyzeFailure(error, 'write', 'supabase', { attemptN: 1, lineageId: intentId, workerName: 'write-lifecycle-event-worker', primaryKeyField: 'credential_id', primaryKeyValue: credential_id });
       governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId,
         table, count: 0, rows, analysis, errorShape: { category: analysis.category, subtype: analysis.subtype, retryable: analysis.retryable, retryAfterMs: analysis.rateLimit.retryAfterMs }, error: error.message });
       return { success: false, error: error.message };
     }

     governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId,
       table, count: 1, error: null });
     return { success: true, error: null };
   } catch (err) {
     console.warn('[write-lifecycle-event-worker] Insert failed:', err.message);
     const analysis = analyzeFailure(err, 'write', 'supabase', { attemptN: 1, lineageId: intentId, workerName: 'write-lifecycle-event-worker', primaryKeyField: 'credential_id', primaryKeyValue: credential_id });
     governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId,
       table, count: 0, rows, analysis, errorShape: { category: analysis.category, subtype: analysis.subtype, retryable: analysis.retryable, retryAfterMs: analysis.rateLimit.retryAfterMs }, error: err.message });
     return { success: false, error: err.message };
   }
 }

module.exports = { execute };
