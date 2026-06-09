// postgres-telemetry-kernel/substrates/alerts/workers/write-lifecycle-event-worker.js
// Lifecycle event writer: one bounded INSERT into token_lifecycle_events.
//
// Owns: INSERT INTO token_lifecycle_events (credential_id, business_account_id, event_type, token_age_days, details).
// Does NOT own: dedup logic (caller concern), signal dispatch, audit logging.
//
// FSM contract: receives { domain, accountId, intentId, table, rows } from persist-telemetry FSM.
//   rows[0] = { credential_id, business_account_id, event_type, token_age_days, details }.
// Emits DB_WRITE_COMPLETE through governance on both success and failure.
//
// Best-effort: fire-and-forget semantics. Matches the existing health-substrate pattern
//   where _writeLifecycleEvent does console.warn on insert failure.

const { getSupabaseAdmin } = require('../../../../config/supabase');

/**
 * @param {{ domain: string, accountId: string, intentId?: string, table: string, rows: Array }} params
 * @param {object} governance — CK reference (for DB_WRITE_COMPLETE emission)
 */
async function execute(params, governance) {
  const { domain, accountId, intentId, table, rows } = params;
  const row = (rows && rows[0]) || {};
  const { credential_id, business_account_id, event_type, token_age_days, details } = row;

  if (!credential_id || !event_type) {
    const err = 'credential_id and event_type are required';
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
      governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId,
        table, count: 0, status: 'failed', error: error.message });
      return { success: false, error: error.message };
    }

    governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId,
      table, count: 1, status: 'success', error: null });
    return { success: true, error: null };
  } catch (err) {
    console.warn('[write-lifecycle-event-worker] Insert failed:', err.message);
    governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId,
      table, count: 0, status: 'failed', error: err.message });
    return { success: false, error: err.message };
  }
}

module.exports = { execute };
