// postgres-telemetry-kernel/substrates/alerts/workers/write-alert-worker.js
// Alert writer: one bounded INSERT into system_alerts.
//
// Owns: INSERT INTO system_alerts (alert_type, business_account_id, message, details, resolved).
// Does NOT own: dedup logic (caller concern — health-substrate owns the pre-insert dedup check),
//               signal dispatch, audit logging, FK validation,
//               failure classification (persistence-failure-substrate),
//               retry policy (retry-cadence-kernel).
//
// FSM contract: receives { domain, accountId, intentId, table, rows } from persist-telemetry FSM.
//   rows[0] = { alert_type, business_account_id, message, details, resolved }.
// Emits DB_WRITE_COMPLETE on success, DB_WRITE_FAILED on failure (with errorShape).
//
// Best-effort: fire-and-forget semantics. The error is reported via governance.
//   The retry cadence is owned by retry-cadence-kernel — this worker does
//   NOT retry. It only classifies and reports.

const { getSupabaseAdmin } = require('../../../../config/supabase');
const { analyzeFailure } = require('../../../persistence-failure-substrate');

/**
 * @param {{ domain: string, accountId: string, intentId?: string, table: string, rows: Array }} params
 * @param {object} governance — CK reference (for DB_WRITE_COMPLETE / DB_WRITE_FAILED emission)
 */
async function execute(params, governance) {
   const { domain, accountId, intentId, table, rows } = params;
   const row = (rows && rows[0]) || {};
   const { alert_type, business_account_id, message, details, resolved } = row;

   if (!alert_type || !business_account_id || !message) {
     const err = 'alert_type, business_account_id, and message are required';
     const analysis = analyzeFailure({ message: err }, 'write', 'supabase', { attemptN: 1, lineageId: intentId, workerName: 'write-alert-worker', primaryKeyField: 'business_account_id', primaryKeyValue: business_account_id });
     governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId,
       table, count: 0, rows, analysis, errorShape: { category: analysis.category, subtype: analysis.subtype, retryable: analysis.retryable, retryAfterMs: analysis.rateLimit.retryAfterMs }, error: err });
     return { success: false, error: err };
   }

   const supabase = getSupabaseAdmin();
   if (!supabase) {
     const err = 'supabase_unavailable';
     const analysis = analyzeFailure({ message: err }, 'write', 'supabase', { attemptN: 1, lineageId: intentId, workerName: 'write-alert-worker', primaryKeyField: 'business_account_id', primaryKeyValue: business_account_id });
     governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId,
       table, count: 0, rows, analysis, errorShape: { category: analysis.category, subtype: analysis.subtype, retryable: analysis.retryable, retryAfterMs: analysis.rateLimit.retryAfterMs }, error: err });
     return { success: false, error: err };
   }

   try {
     const { error } = await supabase
       .from('system_alerts')
       .insert({
         alert_type,
         business_account_id,
         message,
         details: details || {},
         resolved: typeof resolved === 'boolean' ? resolved : false,
       });

     if (error) {
       console.warn('[write-alert-worker] Insert failed:', error.message);
       const analysis = analyzeFailure(error, 'write', 'supabase', { attemptN: 1, lineageId: intentId, workerName: 'write-alert-worker', primaryKeyField: 'business_account_id', primaryKeyValue: business_account_id });
       governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId,
         table, count: 0, rows, analysis, errorShape: { category: analysis.category, subtype: analysis.subtype, retryable: analysis.retryable, retryAfterMs: analysis.rateLimit.retryAfterMs }, error: error.message });
       return { success: false, error: error.message };
     }

     governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId,
       table, count: 1, error: null });
     return { success: true, error: null };
   } catch (err) {
     console.warn('[write-alert-worker] Insert failed:', err.message);
     const analysis = analyzeFailure(err, 'write', 'supabase', { attemptN: 1, lineageId: intentId, workerName: 'write-alert-worker', primaryKeyField: 'business_account_id', primaryKeyValue: business_account_id });
     governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId,
       table, count: 0, rows, analysis, errorShape: { category: analysis.category, subtype: analysis.subtype, retryable: analysis.retryable, retryAfterMs: analysis.rateLimit.retryAfterMs }, error: err.message });
     return { success: false, error: err.message };
   }
 }

module.exports = { execute };
