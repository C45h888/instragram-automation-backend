// telemetry-kernel/substrates/projection/workers/index.js
const RuntimeProjectionWorker = require('./runtime-projection-worker');
const IntegrityProjectionWorker = require('./integrity-projection-worker');
const AuthorityProjectionWorker = require('./authority-projection-worker');
const HealthProjectionWorker = require('./health-projection-worker');
const SystemicPressureProjectionWorker = require('./systemic-pressure-projection-worker');
const CapabilityProjectionWorker = require('./capability-projection-worker');
const PersistTelemetryProjectionWorker = require('./persist-telemetry-projection-worker');
const ReconciliationProjectionWorker = require('./reconciliation-projection-worker');
const SchedulingProjectionWorker = require('./scheduling-projection-worker');
const DedupProjectionWorker = require('./dedup-projection-worker');
const PublishingProjectionWorker = require('./publishing-projection-worker');
const AcquisitionProjectionWorker = require('./acquisition-projection-worker');

const workers = {
  runtime: new RuntimeProjectionWorker(),
  integrity: new IntegrityProjectionWorker(),
  authority: new AuthorityProjectionWorker(),
  health: new HealthProjectionWorker(),
  systemic: new SystemicPressureProjectionWorker(),
  capability: new CapabilityProjectionWorker(),
  persistTelemetry: new PersistTelemetryProjectionWorker(),
  reconciliation: new ReconciliationProjectionWorker(),
  scheduling: new SchedulingProjectionWorker(),
  dedup: new DedupProjectionWorker(),
  publishing: new PublishingProjectionWorker(),
  acquisition: new AcquisitionProjectionWorker(),
};

async function startAll(pollIntervalMs) {
  const order = ['systemic', 'health', 'integrity', 'authority', 'runtime', 'reconciliation', 'capability', 'persistTelemetry', 'scheduling', 'dedup', 'publishing', 'acquisition'];
  for (const key of order) { await workers[key].start(pollIntervalMs); }
  console.log('[telemetry-kernel/projection-workers] All 12 projection workers started');
}

async function stopAll() {
  const order = ['acquisition', 'publishing', 'dedup', 'scheduling', 'persistTelemetry', 'capability', 'reconciliation', 'runtime', 'authority', 'integrity', 'health', 'systemic'];
  for (const key of order) { await workers[key].stop(); }
  console.log('[telemetry-kernel/projection-workers] All 12 projection workers stopped');
}

function getAllHealth() {
  const result = {};
  for (const [key, worker] of Object.entries(workers)) { result[key] = worker.getHealth(); }
  return result;
}

function getAllProjections() {
  const result = {};
  for (const [key, worker] of Object.entries(workers)) { result[key] = worker.getProjection(); }
  return result;
}

module.exports = { workers, startAll, stopAll, getAllHealth, getAllProjections };
