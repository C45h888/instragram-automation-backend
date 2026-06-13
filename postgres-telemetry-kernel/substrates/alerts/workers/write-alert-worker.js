// postgres-telemetry-kernel/substrates/alerts/workers/write-alert-worker.js
// Alert writer: INSERT into system_alerts.
//
// Owns: param extraction. All Supabase I/O delegated to bedrock.
// Does NOT own: dedup logic, signal dispatch, audit logging.

const bedrock = require('../../../bedrock');

async function execute(params, governance) {
  const { domain, accountId, intentId, rows } = params;
  const row = (rows && rows[0]) || {};

  if (!row.alert_type || !row.business_account_id) {
    const err = 'alert_type and business_account_id required';
    governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId, table: 'system_alerts', count: 0, rows, error: err, rawError: { message: err }, workerName: 'write-alert-worker', lineageId: intentId, primaryKeyField: 'id', primaryKeyValue: row.id || 'new', attemptN: 1, operation: 'write', source: 'supabase' });
    return;
  }

  await bedrock.token.persistAlert({
    business_account_id: row.business_account_id,
    account_id: accountId,
    alert_type: row.alert_type,
    message: row.message || null,
    details: row.details || {},
    resolved: row.resolved !== undefined ? row.resolved : false,
  }, {
    accountId, intentId, governance, domain,
  });
}

module.exports = { execute };
