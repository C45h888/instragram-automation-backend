// postgres-telemetry-kernel/readers/index.js
//
// Re-export surface for the postgres-telemetry-kernel read workers.
// The production code in acquisition-kernel/substrates/engagement-substrate
// requires this module by the plural name ('readers'). The canonical
// directory is 'reading' (singular). This file re-exports the read
// functions from the reading/workers modules so both import paths
// resolve to the same code.
//
// Fix for Phase 7 finding B-NEW-1: the engagement-substrate's
// `require('../../../postgres-telemetry-kernel/readers')` was failing
// because the directory was named 'reading' (singular). This shim
// makes the plural path resolve.

const mediaWorker = require('../reading/workers/media-worker');

module.exports = {
  getRecentMedia: mediaWorker.getRecentMedia,
  getMonitoredHashtags: mediaWorker.getMonitoredHashtags,
};
