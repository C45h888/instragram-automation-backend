// substrates/db/reading/registry.js
// Reading Substrate Registry: domain → worker module mapping.
//
// Each worker exports: execute(params, governance) → { success, data, error, latencyMs }
// Workers are operationally bounded to one read domain.
// Add new read domains here — no other file changes needed.

const WORKER_MAP = {
  'db.media':            './workers/media-worker',
  'db.post-queue':       './workers/post-queue-worker',
  'db.scheduled-posts':  './workers/post-queue-worker',
  'db.accounts':         './workers/accounts-worker',
  'db.scope-cache':      '../substrates/graph-capability/workers/read-scope-cache-worker',
  'db.credential':       '../substrates/graph-capability/workers/read-credential-worker',
  'db.encryption-key':   '../substrates/graph-capability/workers/read-key-worker',
  'db.alerts':           '../substrates/alerts/workers/read-alerts-worker',
  'db.lifecycle-events': '../substrates/alerts/workers/read-lifecycle-events-worker',
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
