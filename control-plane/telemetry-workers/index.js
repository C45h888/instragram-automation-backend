// control-plane/telemetry-workers/index.js
// Bounded Telemetry Projection Layer: unified export for all projection workers.
//
// Architecture:
//   Raw Telemetry Projection
//         ↓
//   Shared Projection Workers (5 bounded workers)
//         ↓
//   Observability Plane (SEMANTIC_PROJECTION_TRANSITION events)
//         ↓
//   Lineage Worker (Layer A — validates and persists, no synthesis)
//         ↓
//   Immutable Unified Ledger
//         ↓
//   Bounded Interpreters (FSM/HSM/Recon)
//
// All 5 projection workers are exported here. They are started/stopped
// as a group by the orchestrator.
//
// Workers:
//   RuntimeProjectionWorker       — runtimeState, executionPressure, retryPressure
//   IntegrityProjectionWorker      — replayContinuity, causationIntegrity
//   AuthorityProjectionWorker      — authorityContinuity, authorityOscillation
//   HealthProjectionWorker         — failureRate, runtimeEntropy, degradationSignals
//   GovernanceRuntimeProjectionWorker — governancePressure, convergenceConfidence

const RuntimeProjectionWorker = require('./runtime-projection-worker');
const IntegrityProjectionWorker = require('./integrity-projection-worker');
const AuthorityProjectionWorker = require('./authority-projection-worker');
const HealthProjectionWorker = require('./health-projection-worker');
const GovernanceRuntimeProjectionWorker = require('./governance-runtime-projection-worker');

// ── Worker instances ───────────────────────────────────────────────────────────

const workers = {
  runtime: new RuntimeProjectionWorker(),
  integrity: new IntegrityProjectionWorker(),
  authority: new AuthorityProjectionWorker(),
  health: new HealthProjectionWorker(),
  governanceRuntime: new GovernanceRuntimeProjectionWorker(),
};

// ── Group lifecycle ─────────────────────────────────────────────────────────────

/**
 * Start all 5 projection workers.
 * Workers should be started BEFORE lineageWorker.start()
 * so they can produce projections for the lineage worker to consume.
 *
 * @param {number} [pollIntervalMs] — override poll interval for all workers
 */
async function startAll(pollIntervalMs) {
  const order = ['governanceRuntime', 'health', 'integrity', 'authority', 'runtime'];
  for (const key of order) {
    await workers[key].start(pollIntervalMs);
  }
  console.log('[telemetry-workers] All 5 projection workers started');
}

/**
 * Stop all 5 projection workers gracefully.
 */
async function stopAll() {
  const order = ['runtime', 'authority', 'integrity', 'health', 'governanceRuntime'];
  for (const key of order) {
    await workers[key].stop();
  }
  console.log('[telemetry-workers] All projection workers stopped');
}

/**
 * Return health signals for all workers.
 */
function getAllHealth() {
  const result = {};
  for (const [key, worker] of Object.entries(workers)) {
    result[key] = worker.getHealth();
  }
  return result;
}

/**
 * Return current projections for all workers.
 */
function getAllProjections() {
  const result = {};
  for (const [key, worker] of Object.entries(workers)) {
    result[key] = worker.getProjection();
  }
  return result;
}

module.exports = {
  // Worker instances
  workers,

  // Group lifecycle
  startAll,
  stopAll,

  // Query
  getAllHealth,
  getAllProjections,
};
