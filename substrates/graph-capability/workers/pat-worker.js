// substrates/graph-capability/workers/pat-worker.js
// PAT observer worker: read-only capability observation of the vault.
//
// Owns: cadence loop, observation packaging.
// Does NOT own: vault I/O (Phase 3), encrypt/decrypt, OAuth flow, FSM state.
//
// Contract: execute() → observation envelope.
// In Phase 2, execute() returns a stub. Phase 3 wires the vault RPC.

const cadence = require('../cadence');

class PatWorker {
  /**
   * @param {{ onObservation: Function }} bindings
   */
  constructor({ onObservation } = {}) {
    this._onObservation = onObservation || (() => {});
    this._interval = null;
  }

  /**
   * Run one bounded capability observation.
   * Phase 3: replace stub with vault RPC call.
   * @returns {Promise<{ isPresent: boolean, isDecryptable: boolean, observedAt: number, evidence: object }>}
   */
  async execute() {
    // Phase 2 stub — Phase 3 will wire vault RPC getCapabilityObservation
    return {
      isPresent: null,      // unknown — vault not yet wired
      isDecryptable: null,  // unknown
      observedAt: Date.now(),
      evidence: { source: 'pat-worker-stub' },
    };
  }

  async _tick() {
    try {
      const envelope = await this.execute();
      this._onObservation(envelope);
    } catch (err) {
      console.warn('[pat-worker] tick failed:', err.message);
    }
  }

  start() {
    if (this._interval) return;
    this._interval = setInterval(() => this._tick(), cadence.PAT_HEALTH_INTERVAL_MS);
    // Fire one immediate tick so the façade has data on boot
    this._tick();
  }

  stop() {
    if (this._interval) clearInterval(this._interval);
    this._interval = null;
  }
}

module.exports = PatWorker;
