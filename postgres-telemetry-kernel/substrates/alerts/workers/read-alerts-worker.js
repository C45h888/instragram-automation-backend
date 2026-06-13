// postgres-telemetry-kernel/substrates/alerts/workers/read-alerts-worker.js
// Alert reader: governed SELECTs on system_alerts.
//
// Owns: query routing. All Supabase I/O delegated to bedrock.
// Does NOT own: governance policy, signal dispatch, alert dedup.

const bedrock = require('../../../bedrock');

async function execute(params, governance) {
  const accountId = params.accountId || params.businessAccountId;
  const { query = 'by_account', alertType, limit = 50 } = params;
  const startTime = Date.now();

  // ── Dedup check ──────────────────────────────────────────────────────
  if (query === 'checkExistingWarning') {
    if (!accountId || !alertType) {
      return { success: false, data: null, error: 'accountId and alertType required', latencyMs: Date.now() - startTime };
    }
    const result = await bedrock.token.checkExistingWarning(accountId, alertType);
    return { success: result.success, data: !!result.data, error: result.error, latencyMs: result.latencyMs || (Date.now() - startTime) };
  }

  if (!accountId && query !== 'unresolved') {
    return { success: false, data: null, error: 'accountId required', latencyMs: Date.now() - startTime };
  }

  // ── Query routing ────────────────────────────────────────────────────
  let result;
  if (query === 'by_account') {
    result = await bedrock.token.getAlerts(accountId, { limit });
  } else if (query === 'unresolved') {
    result = await bedrock.token.getUnresolvedAlerts({ accountId, limit });
  } else if (query === 'by_type') {
    if (!alertType) {
      return { success: false, data: null, error: 'alertType required', latencyMs: Date.now() - startTime };
    }
    result = await bedrock.token.getAlertsByType(alertType, { accountId, limit });
  } else {
    result = await bedrock.token.getAlerts(accountId, { limit });
  }

  return { success: result.success, data: result.data || [], error: result.error, latencyMs: result.latencyMs || (Date.now() - startTime) };
}

module.exports = { execute };
