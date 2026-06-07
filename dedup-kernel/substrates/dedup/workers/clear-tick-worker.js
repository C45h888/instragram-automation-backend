// dedup-kernel/substrates/dedup/workers/clear-tick-worker.js
// Clear Tick Worker: operationally bounded, semantically blind.
//
// Bound to: substrates/dedup/index.js (canonical Redis owner).
// One bounded operation: substrate.clearTick() — clears identity cache.
//
// Semantically blind: clears cache, returns void. FSM decides when to
// invoke (on DEDUP_BATCH_END transition). Worker does NOT evaluate
// batch metrics, does NOT emit governance signals.
//
// Authority chain: CK → dedup FSM → ctx.invokeWorker → this worker → substrate

const substrate = require('../index');

/**
 * Execute one bounded clear-tick operation through the canonical substrate.
 * Clears the identity cache; resource tracker persists across ticks.
 */
function execute(_params) {
  substrate.clearTick();
}

module.exports = { execute };
