// postgres-telemetry-kernel/substrates/alerts/workers/read-lifecycle-events-worker.js
// Lifecycle event reader: governed SELECTs on token_lifecycle_events.
//
// Owns: query routing. All Supabase I/O delegated to bedrock.
// Does NOT own: governance policy, signal dispatch.

const bedrock = require('../../../bedrock');

async function execute(params, governance) {
  const { accountId, query = 'recent', credentialId, eventType, limit = 50 } = params;
  const startTime = Date.now();

  if (query === 'by_account' && !accountId) {
    return { success: false, data: null, error: 'accountId required for by_account query', latencyMs: Date.now() - startTime };
  }
  if (query === 'by_credential' && !credentialId) {
    return { success: false, data: null, error: 'credentialId required for by_credential query', latencyMs: Date.now() - startTime };
  }

  const result = await bedrock.token.getLifecycleEvents({
    accountId: query === 'by_account' ? accountId : undefined,
    credentialId: query === 'by_credential' ? credentialId : undefined,
    eventType: eventType || undefined,
    limit,
  });

  return { success: result.success, data: result.data || [], error: result.error, latencyMs: result.latencyMs || (Date.now() - startTime) };
}

module.exports = { execute };
