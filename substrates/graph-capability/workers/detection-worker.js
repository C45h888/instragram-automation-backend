// substrates/graph-capability/workers/detection-worker.js
// Detection observer worker: /debug_token via Meta Graph API + 7-day scope cache write.
//
// Owns: cadence loop, /debug_token rate-limited call, scope_cache write.
// Does NOT own: FSM state, OAuth flow, PAT/UAT storage, scope judgment (delegated to scope-worker).
//
// Emits AUTH_FAILURE_STRIKE on 401/190 into the capability bus.
// On success, writes scope_cache + scope_cache_updated_at to instagram_credentials.
//
// Contract: execute() → observation envelope.
// In Phase 2, execute() returns a stub. Phase 3 wires axios + cache write.

const cadence = require('../cadence');

class DetectionWorker {
  /**
   * @param {{ onObservation: Function }} bindings
   */
  constructor({ onObservation } = {}) {
    this._onObservation = onObservation || (() => {});
    this._interval = null;
  }

  /**
   * Run one bounded /debug_token observation.
   * Phase 3: replace stub with axios.get + supabase update.
   * @returns {Promise<{ isValid: boolean, type: string|null, scopes: string[], expiresAt: number|null, issuedAt: number|null, userId: string|null, appId: string|null, dataAccessExpiresAt: number|null, reliabilityImpaired: boolean, reason: string|null, observedAt: number, evidence: object }>}
   */
  async execute() {
    // Phase 2 stub — Phase 3 will wire axios /debug_token + cache write
    return {
      isValid: null,
      type: null,
      scopes: null,
      expiresAt: null,
      issuedAt: null,
      userId: null,
      appId: null,
      dataAccessExpiresAt: null,
      reliabilityImpaired: false,
      reason: null,
      observedAt: Date.now(),
      evidence: { source: 'detection-worker-stub' },
    };
  }

  async _tick() {
    try {
      const envelope = await this.execute();
      this._onObservation(envelope);
    } catch (err) {
      console.warn('[detection-worker] tick failed:', err.message);
    }
  }

  start() {
    if (this._interval) return;
    this._interval = setInterval(() => this._tick(), cadence.PAT_HEALTH_INTERVAL_MS);
    this._tick();
  }

  stop() {
    if (this._interval) clearInterval(this._interval);
    this._interval = null;
  }
}

module.exports = DetectionWorker;
