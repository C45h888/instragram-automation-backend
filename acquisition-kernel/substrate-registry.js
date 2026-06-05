// control-plane/execution/substrate-registry.js
// Substrate Registry: SINGLE domain→substrate ownership map.
// CONSTITUTIONAL OWNER: this is the only registry that binds a domain
// name to its bounded worker. All other lookup tables (parsing/domain-map,
// retry-cadence/registry) are leaf convenience getters that call this.
//
// Owns: mapping domain names to { fetch, persist, parsingWorker }.
// Does NOT own: orchestration, governance, policy, retry, execution flow.
//
// Constitutional invariant: parsing-worker lookup flows through this
// registry, not through a sibling registry. One domain name = one owner.
//
// Publish domains removed — migrated to pull-based publishing pipeline
// (post-queue-worker under persist-telemetry-fsm governance).

const engagement = require('./substrates/engagement');
const content     = require('./substrates/content');
const ugc         = require('./substrates/ugc');
const insights    = require('./substrates/insights');

// Parsing workers — domain-bounded, registered here, looked up via
// substrateRegistry.getParsingWorker(domain). These replace the old
// parsing/domain-map.js which has been deleted.
const PARSING_WORKER_MAP = {
  comments:  './parsing/workers/comments-worker',
  messages:  './parsing/workers/messages-worker',
  ugc:       './parsing/workers/ugc-worker',
  insights:  './parsing/workers/insights-worker',
  media:     './parsing/workers/content-worker',
};

const DOMAIN_REGISTRY = {
  comments:  { fetch: engagement.fetch.bind(engagement), persist: engagement.persist.bind(engagement) },
  messages:  { fetch: engagement.fetch.bind(engagement), persist: engagement.persist.bind(engagement) },
  ugc:       { fetch: ugc.fetch.bind(ugc),              persist: ugc.persist.bind(ugc) },
  insights:  { fetch: insights.fetch.bind(insights),     persist: insights.persist.bind(insights) },
  media:     { fetch: content.fetch.bind(content),       persist: content.persist.bind(content) },
};

function lookup(domain) {
  // Publish domains are no longer routed through substrate-registry.
  // Publishing now flows through the pull-based pipeline:
  //   cognition-scanner → CK → publishing-fsm → persist-telemetry-fsm → post-queue-worker
  if (domain && domain.startsWith('publish:')) {
    return null;
  }
  return DOMAIN_REGISTRY[domain] || null;
}

/**
 * Return the bounded parsing worker module for a domain.
 * Single owner of domain→worker binding. parsing/index.js MUST go through this.
 *
 * @param {string} domain
 * @returns {object|null} worker module with execute()
 */
function getParsingWorker(domain) {
  const workerPath = PARSING_WORKER_MAP[domain];
  if (!workerPath) return null;
  // Resolve relative to this file's directory so require() works from anywhere.
  return require(workerPath);
}

function domainForAction(actionType) {
  // Removed — publish domain routing no longer uses substrate-registry.
  // Publishing intents now flow through the pull-based pipeline.
  // Returns a neutral key to avoid breaking emission.js LPUSH for orphaned intents.
  return 'publish:deprecated';
}

function fetchTypeForAction(actionType) {
  return 'publish:deprecated';
}

function allDomains() {
  return Object.keys(DOMAIN_REGISTRY);
}

module.exports = {
  lookup,
  getParsingWorker,
  domainForAction,
  fetchTypeForAction,
  allDomains,
};
