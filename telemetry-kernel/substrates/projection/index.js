// telemetry-kernel/substrates/projection/index.js
// Projection Substrate: factory for 5 bounded projection workers.
//
// Owns: worker lifecycle, event-driven tick scheduling coordination,
//        projection synthesis inputs, deterministic replay.
//
// Does NOT own: FSM semantics, governance decisions, lineage persistence,
//               transition writing. Those belong to the FSM and transition-writers.
//
// Architecture:
//   projection workers (5) → PROJECTION_INTENT → observability plane
//                                                        ↓
//   CK cadence → FSM reads intents → validates → orders → serializes
//                                                        ↓
//            SEMANTIC_PROJECTION_TRANSITION → transition-writers → lineage ledger
//
// Worker→FSM→TransitionWriter chain is:
//   Projection Worker (kernel) → observability → FSM (kernel) → transition-writers (control-plane)
// The transition-writers remain in control-plane as the mechanical write pipe.

const { workers, startAll, stopAll, getAllHealth, getAllProjections } = require('./workers');

/**
 * Start all 5 projection workers.
 * Workers emit PROJECTION_INTENT to the observability plane on each tick.
 *
 * @param {number} [pollIntervalMs] — override debounce interval for all workers
 */
async function startProjections(pollIntervalMs) {
  await startAll(pollIntervalMs);
}

/**
 * Stop all 5 projection workers gracefully.
 */
async function stopProjections() {
  await stopAll();
}

/**
 * Return health signals for all 5 projection workers.
 * @returns {object} per-namespace health status
 */
function getProjectionHealth() {
  return getAllHealth();
}

/**
 * Return current cached projections from all 5 workers.
 * @returns {object} per-namespace projection payload
 */
function getProjections() {
  return getAllProjections();
}

module.exports = {
  startProjections,
  stopProjections,
  getProjectionHealth,
  getProjections,
  workers, // direct worker access if needed
};
