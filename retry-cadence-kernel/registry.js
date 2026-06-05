// substrates/retry-cadence/registry.js
// Retry-cadence registry: domain → retry worker module.
//
// Owns: mapping domain names to bounded retry workers.
// Does NOT own: execution, policy, scheduling, governance.

const WORKER_MAP = {
  comments:  './workers/engagement-retry-worker',
  messages:  './workers/engagement-retry-worker',
  ugc:       './workers/ugc-retry-worker',
  insights:  './workers/insights-retry-worker',
  media:     './workers/content-retry-worker',
};

function getWorker(domain) {
  const path = WORKER_MAP[domain];
  if (!path) return null;
  return require(path);
}

module.exports = { getWorker };
