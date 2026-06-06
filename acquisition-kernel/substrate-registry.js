// control-plane/execution/substrate-registry.js
// Substrate Registry: SINGLE domain→substrate ownership map.
// CONSTITUTIONAL OWNER: this is the only registry that binds a domain
// name to its bounded worker. All other lookup tables (parsing/domain-map,
// retry-cadence/registry) are leaf convenience getters that call this.
//
// Owns: mapping domain names to { fetch, parsingWorker, retryWorker,
//        classificationWorker }.
// Does NOT own: orchestration, governance, policy, execution flow.
//
// Constitutional invariant: every domain-bounded worker lookup flows
// through this registry, not through a sibling registry. One domain
// name = one owner.
//
// Publish domains removed — migrated to pull-based publishing pipeline
// (post-queue-worker under persist-telemetry-fsm governance).

const engagement = require('./substrates/engagement');
const content     = require('./substrates/content');
const ugc         = require('./substrates/ugc');
const insights    = require('./substrates/insights');

// Parsing workers — domain-bounded, registered here, looked up via
// substrateRegistry.getParsingWorker(domain).
const PARSING_WORKER_MAP = {
  comments:  './parsing/workers/comments-worker',
  messages:  './parsing/workers/messages-worker',
  ugc:       './parsing/workers/ugc-worker',
  insights:  './parsing/workers/insights-worker',
  media:     './parsing/workers/content-worker',
};

// Retry-substrate workers — domain-bounded, registered here, looked up
// via substrateRegistry.getRetryWorker(domain). These are the bounded
// executors that re-run fetch+parse+persist for a retry attempt. They
// are operationally complete and semantically blind — they do not
// classify errors, do not mutate engagement state. They report raw
// outcomes to governance and stop.
const RETRY_WORKER_MAP = {
  comments:  '../retry-cadence-kernel/workers/engagement-retry-worker',
  messages:  '../retry-cadence-kernel/workers/engagement-retry-worker',
  ugc:       '../retry-cadence-kernel/workers/ugc-retry-worker',
  insights:  '../retry-cadence-kernel/workers/insights-retry-worker',
  media:     '../retry-cadence-kernel/workers/content-retry-worker',
};

// Classification workers — semantically blind, bounded. They receive
// a raw error payload, run classification rules, return a classified
// action tag. They do not decide retry vs skip vs break. They do not
// mutate state. They only classify. The FSM consumes the classification.
const CLASSIFICATION_WORKER_MAP = {
  comments:  '../retry-cadence-kernel/workers/classification-worker',
  messages:  '../retry-cadence-kernel/workers/classification-worker',
  ugc:       '../retry-cadence-kernel/workers/classification-worker',
  insights:  '../retry-cadence-kernel/workers/classification-worker',
  media:     '../retry-cadence-kernel/workers/classification-worker',
};

const DOMAIN_REGISTRY = {
  comments:  { fetch: engagement.fetch.bind(engagement) },
  messages:  { fetch: engagement.fetch.bind(engagement) },
  ugc:       { fetch: ugc.fetch.bind(ugc) },
  insights:  { fetch: insights.fetch.bind(insights) },
  media:     { fetch: content.fetch.bind(content) },
};

function lookup(domain) {
  if (domain && domain.startsWith('publish:')) {
    return null;
  }
  return DOMAIN_REGISTRY[domain] || null;
}

/**
 * Return the bounded parsing worker module for a domain.
 * @param {string} domain
 * @returns {object|null} worker module with execute()
 */
function getParsingWorker(domain) {
  const workerPath = PARSING_WORKER_MAP[domain];
  if (!workerPath) return null;
  return require(workerPath);
}

/**
 * Return the bounded retry-substrate worker for a domain.
 * @param {string} domain
 * @returns {object|null} worker module with schedule()
 */
function getRetryWorker(domain) {
  const workerPath = RETRY_WORKER_MAP[domain];
  if (!workerPath) return null;
  return require(workerPath);
}

/**
 * Return the bounded classification worker for a domain.
 * The classification worker is the semantically-blind error classifier.
 * It receives raw error, returns classified action tag. The FSM
 * (engagement-fsm) consumes the tag and decides state mutation.
 *
 * @param {string} domain
 * @returns {object|null} worker module with classify()
 */
function getClassificationWorker(domain) {
  const workerPath = CLASSIFICATION_WORKER_MAP[domain];
  if (!workerPath) return null;
  return require(workerPath);
}

function domainForAction(actionType) {
  return 'publish:deprecated';
}

function fetchTypeForAction(actionType) {
  return 'publish:deprecated';
}

function allDomains() {
  return Object.keys(DOMAIN_REGISTRY);
}

/**
 * Boot-time validation: every domain in DOMAIN_REGISTRY has a binding
 * in every worker map. Catches drift at boot, not at runtime.
 */
function validate() {
  const domains = new Set(Object.keys(DOMAIN_REGISTRY));
  const issues = [];

  for (const [mapName, map] of [
    ['PARSING_WORKER_MAP', PARSING_WORKER_MAP],
    ['RETRY_WORKER_MAP', RETRY_WORKER_MAP],
    ['CLASSIFICATION_WORKER_MAP', CLASSIFICATION_WORKER_MAP],
  ]) {
    for (const d of domains) {
      if (!map[d]) issues.push(`${mapName} missing domain: ${d}`);
    }
    for (const k of Object.keys(map)) {
      if (!domains.has(k)) issues.push(`${mapName} has unknown domain: ${k}`);
    }
  }

  if (issues.length > 0) {
    throw new Error(
      `[substrate-registry] boot validation failed:\n  ${issues.join('\n  ')}`
    );
  }
}

module.exports = {
  lookup,
  getParsingWorker,
  getRetryWorker,
  getClassificationWorker,
  domainForAction,
  fetchTypeForAction,
  allDomains,
  validate,
};
