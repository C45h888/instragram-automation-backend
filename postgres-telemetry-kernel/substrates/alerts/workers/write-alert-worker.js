// postgres-telemetry-kernel/substrates/alerts/workers/write-alert-worker.js
// Alert writer: one bounded INSERT into system_alerts.
//
// Owns: INSERT INTO system_alerts (alert_type, business_account_id, message, details, resolved).
// Does NOT own: dedup logic (caller concern — health-substrate owns the pre-insert dedup check),
//               signal dispatch, audit logging, FK validation.
//
// FSM contract: receives { domain, accountId, intentId, table, rows } from persist-telemetry FSM.
//   rows[0] = { alert_type, business_account_id, message, details, resolved }.
// Emits DB_WRITE_COMPLETE through governance on both success and failure.
//
// Best-effort: fire-and-forget semantics. A failed INSERT is reported via governance
//   but never retried by this worker. The health-substrate's existing pattern (console.warn
//   on alert insert failure) is preserved through the DB_WRITE_COMPLETE status.

const { getSupabaseAdmin } = require('../../../../config/supabase');

/**
 * @param {{ domain: string, accountId: string, intentId?: string, table: string, rows: Array }} params
 * @param {object} governance — CK reference (for DB_WRITE_COMPLETE emission)
 */
async function execute(params, governance) {
  const { domain, accountId, intentId, table, rows } = params;
  const row = (rows && rows[0]) || {};
  const { alert_type, business_account_id, message, details, resolved } = row;

  if (!alert_type || !business_account_id || !message) {
    const err = 'alert_type, business_account_id, and message are required';
    governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId,
      table, count: 0, status: 'failed', error: err });
    return { success: false, error: err };
  }

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId,
      table, count: 0, status: 'failed', error: 'supabase_unavailable' });
    return { success: false, error: 'supabase_unavailable' };
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
      governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId,
        table, count: 0, status: 'failed', error: error.message });
      return { success: false, error: error.message };
    }

    governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId,
      table, count: 1, status: 'success', error: null });
    return { success: true, error: null };
  } catch (err) {
    governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId,
      table, count: 0, status: 'failed', error: err.message });
    return { success: false, error: err.message };
  }
}

module.exports = { execute };
