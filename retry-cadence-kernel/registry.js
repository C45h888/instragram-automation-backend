// retry-cadence-kernel/registry.js
// THIN GETTER. The single source of truth for domain→worker binding
// lives in substrate-registry.js. This file exists only as a stable
// import path for retry-cadence-kernel/index.js. All lookups delegate.
//
// Do NOT add a WORKER_MAP here. Drift is caught at boot by
// substrate-registry.validate().

const substrateRegistry = require('../acquisition-kernel/substrate-registry');

function getRetryWorker(domain) {
  return substrateRegistry.getRetryWorker(domain);
}

function getClassificationWorker(domain) {
  return substrateRegistry.getClassificationWorker(domain);
}

module.exports = { getRetryWorker, getClassificationWorker };
