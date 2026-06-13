// postgres-telemetry-kernel/reading/workers/api-usage-worker.js
// API Usage Worker: governed read for rate-limit check.
//
// Owns: query routing. All Supabase I/O delegated to bedrock.
// Does NOT own: governance policy (FSM), routing (CK), rate-limit decisions.

const bedrock = require('../../bedrock');

async function execute(params, governance) {
  const { query = 'checkHourlyLimit', userId, limit = 200 } = params;
  const startTime = Date.now();

  if (!userId) {
    return { success: false, data: null, error: 'userId required', latencyMs: Date.now() - startTime };
  }

  const result = await bedrock.token.checkApiUsage(userId, limit);

  return {
    ...result,
    latencyMs: result.latencyMs || (Date.now() - startTime),
  };
}

module.exports = { execute };
