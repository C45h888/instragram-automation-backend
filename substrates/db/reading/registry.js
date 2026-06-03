// substrates/db/reading/registry.js
// Reading Substrate Registry: domain → worker module mapping.
//
// Each worker exports: execute(params, governance) → { success, data, error, latencyMs }
// Workers are operationally bounded to one read domain.
// Add new read domains here — no other file changes needed.

const WORKER_MAP = {
  'db.media': './workers/media-worker',
};

function getWorker(domain) {
  const path = WORKER_MAP[domain];
  if (!path) return null;
  return require(path);
}

function getDomains() {
  return Object.keys(WORKER_MAP);
}

module.exports = { getWorker, getDomains };
