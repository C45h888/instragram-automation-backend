// postgres-telemetry-kernel/reading/workers/user-profiles-worker.js
// User Profiles Worker: governed count read.
//
// Owns: query routing. All Supabase I/O delegated to bedrock.
// Does NOT own: governance policy (FSM), routing (CK).

const bedrock = require('../../bedrock');

async function execute(params, governance) {
  const { query = 'count' } = params;
  const startTime = Date.now();

  const result = await bedrock.token.countUserProfiles();

  return {
    success: result.success,
    data: result.data,
    error: result.error,
    latencyMs: result.latencyMs || (Date.now() - startTime),
  };
}

module.exports = { execute };
