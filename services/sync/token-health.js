// backend.api/services/sync/token-health.js
// BACK-COMPAT SHIM — code has been migrated to graph-capability-kernel.
//
// Migration target:
//   graph-capability-kernel/substrates/health-substrate/
//     - index.js (runTokenHealthCheck, runUATRefreshCheck orchestrators)
//     - workers/scan-credentials-worker.js
//     - workers/recovery-worker.js
//     - workers/uat-refresh-worker.js
//     - workers/data-access-expiry-worker.js
//
// This shim preserves the legacy module.exports contract:
//   require('./services/sync/token-health').runTokenHealthCheck
//   require('./services/sync/token-health').runUATRefreshCheck
//
// New code MUST import from graph-capability-kernel directly:
//   const { health } = require('./graph-capability-kernel');
//   await health.runTokenHealthCheck();
//   await health.runUATRefreshCheck();
//
// This shim will be deleted in a subsequent cleanup pass.

const { runTokenHealthCheck, runUATRefreshCheck } = require('../../graph-capability-kernel/substrates/health-substrate');

module.exports = { runTokenHealthCheck, runUATRefreshCheck };
