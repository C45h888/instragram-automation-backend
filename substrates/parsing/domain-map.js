// substrates/parsing/domain-map.js
// Parsing substrate domain map: domain → worker module.
//
// Owns: mapping domain names to their bounded parsing workers.
// Does NOT own: execution, persistence, normalization, orchestration.
//
// Pure lookup — zero side effects, zero state.

const WORKER_MAP = {
  comments:  './workers/comments-worker',
  messages:  './workers/messages-worker',
  ugc:       './workers/ugc-worker',
  insights:  './workers/insights-worker',
  media:     './workers/content-worker',
};

/**
 * Return the worker module for a domain.
 * @param {string} domain
 * @returns {object|null} worker module with execute()
 */
function getWorker(domain) {
  const path = WORKER_MAP[domain];
  if (!path) return null;
  return require(path);
}

module.exports = { getWorker };
