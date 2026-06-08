// graph-capability-kernel/substrates/vault/scope-substrate/workers/detect-dynamic-worker.js
// Scope detect worker: one bounded /debug_token HTTP call.
// Cache read/write moved to scope-substrate façade (governance membrane concern).
//
// Owns: ONE bounded HTTP call to Meta's /debug_token endpoint.
// Does NOT own: cache logic, signal dispatch, scope defaults.

const { axios, GRAPH_API_BASE } = require('../../../api-surface');

class DetectDynamicWorker {
  /**
   * @param {{ token: string }} input
   * @returns {Promise<string[]>} detected scopes
   */
  async execute({ token }) {
    try {
      const debugResponse = await axios.get(`${GRAPH_API_BASE}/debug_token`, {
        params: { input_token: token, access_token: token },
        timeout: 5000,
      });
      return debugResponse.data.data?.scopes || [];
    } catch (debugError) {
      console.warn('⚠️  Scope detection failed:', debugError.message);
      return null;  // null signals "Meta unavailable" — caller uses fallback
    }
  }
}

module.exports = DetectDynamicWorker;
