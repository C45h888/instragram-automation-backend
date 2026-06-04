// substrates/graph-capability/workers/uat-worker.js
// UAT observer worker: read-only capability observation of the vault + expiry check.
//
// Owns: cadence loop, observation packaging, expiry proximity detection.
// Does NOT own: vault I/O (Phase 3), encrypt/decrypt, fb_exchange_token, FSM state.
//
// Emits UAT_REFRESH_NEEDED into the capability bus when daysToExpiry ≤ 14.
// UAT_REFRESH_NEEDED is consumed by the Vault plane, not the FSM.
//
// Contract: execute() → observation envelope.
// In Phase 2, execute() returns a stub. Phase 3 wires the vault RPC.

const cadence = require('../cadence');

class UatWorker {
  /**
   * @param {{ onObservation: Function }} bindings
   */
  constructor({ onObservation } = {}) {
    this._onObservation = onObservation || (() => {});
    this._interval = null;
  }

  /**
   * Run one bounded UAT observation.
   * Phase 3: replace stub with vault RPC + expires_at diff.
   * @returns {Promise<{ isPresent: boolean, isDecryptable: boolean, expiresAt: string|null, dataAccessExpiresAt: string|null, daysToExpiry: number|null, refreshNeeded: boolean, observedAt: number, evidence: object }>}
   */
  async execute() {
    // Phase 2 stub — Phase 3 will wire vault RPC getUATObservation
    return {
      isPresent: null,
      isDecryptable: null,
      expiresAt: null,
      dataAccessExpiresAt: null,
      daysToExpiry: null,
      refreshNeeded: false,
      observedAt: Date.now(),
      evidence: { source: 'uat-worker-stub' },
    };
  }

  async _tick() {
    try {
      const envelope = await this.execute();
      this._onObservation(envelope);
    } catch (err) {
      console.warn('[uat-worker] tick failed:', err.message);
    }
  }

  start() {
    if (this._interval) return;
    this._interval = setInterval(() => this._tick(), cadence.UAT_RECHECK_INTERVAL_MS);
    this._tick();
  }

  stop() {
    if (this._interval) clearInterval(this._interval);
    this._interval = null;
  }
}

module.exports = UatWorker;
