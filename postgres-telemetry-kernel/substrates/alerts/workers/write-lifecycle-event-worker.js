// postgres-telemetry-kernel/substrates/alerts/workers/write-lifecycle-event-worker.js
// Lifecycle event writer: INSERT into token_lifecycle_events.
//
// Owns: param extraction. All Supabase I/O delegated to bedrock.
// Does NOT own: dedup logic, signal dispatch, audit logging.

const bedrock = require('../../../bedrock');

async function execute(params, governance) {
  const { domain, accountId, intentId, rows } = params;
  const row = (rows && rows[0]) || {};

  await bedrock.token.persistLifecycleEvent({
    credential_id: row.credential_id || null,
    business_account_id: row.business_account_id || null,
    event_type: row.event_type || 'unknown',
    token_age_days: row.token_age_days || null,
    details: row.details || {},
  }, {
    accountId, intentId, governance, domain,
  });
}

module.exports = { execute };
