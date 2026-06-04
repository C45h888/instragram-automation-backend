// substrates/vault/scope-substrate/index.js
// Scope substrate façade: factory-creates workers, owns pre-flight.
// Does NOT do I/O — workers do.

const DetectDynamicWorker = require('./workers/detect-dynamic-worker');

/**
 * Detect live scopes for a token via /debug_token with 7-day DB cache.
 * @param {{ token: string, supabase?: object, credentialId?: string|null }} input
 * @returns {Promise<string[]>}
 */
async function detectDynamic({ token, supabase, credentialId = null }) {
  if (!token) {
    throw new Error('token is required');
  }
  const worker = new DetectDynamicWorker();
  return worker.execute({ token, supabase, credentialId });
}

module.exports = { detectDynamic };
