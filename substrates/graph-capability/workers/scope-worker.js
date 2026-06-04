// substrates/graph-capability/workers/scope-worker.js
// Scope observer worker: DB read of instagram_credentials scope cache.
//
// Owns: cadence loop, observation packaging.
// Does NOT own: Graph API calls, FSM state, DB writes (cache is updated by detection-worker).
//
// Contract: execute() → observation envelope.
// In Phase 2, execute() returns a stub. Phase 3 wires the SELECT.

const cadence = require('../cadence');

class ScopeWorker {
  /**
   * @param {{ onObservation: Function }} bindings
   */
  constructor({ onObservation } = {}) {
    this._onObservation = onObservation || (() => {});
    this._interval = null;
  }

  /**
   * Run one bounded scope observation.
   * Phase 3: replace stub with supabase.from('instagram_credentials').select('scope, scope_cache, scope_cache_updated_at')
   * @returns {Promise<{ grantedScopes: string[], missingRequired: string[], cacheAgeMs: number|null, freshness: string, observedAt: number, evidence: object }>}
   */
  async execute() {
    // Phase 2 stub — Phase 3 will wire supabase SELECT
    return {
      grantedScopes: null,
      missingRequired: null,
      cacheAgeMs: null,
      freshness: 'unknown',
      observedAt: Date.now(),
      evidence: { source: 'scope-worker-stub' },
    };
  }

  async _tick() {
    try {
      const envelope = await this.execute();
      this._onObservation(envelope);
    } catch (err) {
      console.warn('[scope-worker] tick failed:', err.message);
    }
  }

  start() {
    if (this._interval) return;
    this._interval = setInterval(() => this._tick(), cadence.SCOPE_RECHECK_INTERVAL_MS);
    this._tick();
  }

  stop() {
    if (this._interval) clearInterval(this._interval);
    this._interval = null;
  }
}

module.exports = ScopeWorker;
