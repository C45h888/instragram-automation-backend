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

const engagement = require('./substrates/engagement-substrate');
const ugcContent   = require('./substrates/ugc-content-substrate');
const insights    = require('./substrates/insights-substrate');

// Publish substrates — for the publish:* domains, "fetch" is a
// misnomer. The publishing substrates execute the outbound action
// (post/story/comment/message). They are bound to the publish
// retry workers (one per substrate, per the dual-binding pattern).
const publishContent    = require('../publishing-kernel/substrates/content/index');
const publishEngagement = require('../publishing-kernel/substrates/engagement/index');

// Parsing workers — domain-bounded, registered here, looked up via
// substrateRegistry.getParsingWorker(domain).
//
// Publish domains DO NOT have parsing workers (outbound actions —
// no response shape to parse). The validate() function tolerates
// missing parsing workers for domains not in PARSING_WORKER_MAP.
const PARSING_WORKER_MAP = {
  comments:  './substrates/parsing-substrate/workers/comments-parser',
  messages:  './substrates/parsing-substrate/workers/messages-parser',
  ugc:       './substrates/parsing-substrate/workers/ugc-parser',
  insights:  './substrates/parsing-substrate/workers/insights-parser',
  media:     './substrates/parsing-substrate/workers/content-parser',
};

// Retry-substrate workers — domain-bounded, registered here, looked up
// via substrateRegistry.getRetryWorker(domain). These are the bounded
// executors that re-run fetch+parse+persist for a retry attempt. They
// are operationally complete and semantically blind — they do not
// classify errors, do not mutate engagement state. They report raw
// outcomes to governance and stop.
const RETRY_WORKER_MAP = {
  comments:           '../retry-cadence-kernel/workers/engagement-retry-worker',
  messages:           '../retry-cadence-kernel/workers/engagement-retry-worker',
  ugc:                '../retry-cadence-kernel/workers/ugc-retry-worker',
  insights:           '../retry-cadence-kernel/workers/insights-retry-worker',
  media:              '../retry-cadence-kernel/workers/content-retry-worker',
  // ── Publish retry workers (Step 6) ────────────────────────
  // Two workers, bound to the two publish substrates. The
  // dual-binding pattern: each publish:* domain has a dedicated
  // retry worker that imports its substrate directly.
  'publish:post':     '../retry-cadence-kernel/workers/publish-content-retry-worker',
  'publish:story':    '../retry-cadence-kernel/workers/publish-content-retry-worker',
  'publish:comment':  '../retry-cadence-kernel/workers/publish-engagement-retry-worker',
  'publish:message':  '../retry-cadence-kernel/workers/publish-engagement-retry-worker',
  'dedup:redis':      '../retry-cadence-kernel/workers/dedup-redis-retry-worker',
  'dedup:repair':     '../retry-cadence-kernel/workers/dedup-repair-retry-worker',
  reconciliation:     '../retry-cadence-kernel/workers/reconciliation-retry-worker',
  // ── Telemetry retry workers (Step 5 workers) ────────────
  // 5 namespace-specific workers, one per projection namespace.
  // _resolveWorkerName in engagement-fsm returns the namespace-specific
  // key based on params.namespace from the RETRY_CADENCE_REQUEST event.
  'telemetry:runtime':   '../retry-cadence-kernel/workers/telemetry-retry-runtime-worker',
  'telemetry:integrity': '../retry-cadence-kernel/workers/telemetry-retry-integrity-worker',
  'telemetry:authority': '../retry-cadence-kernel/workers/telemetry-retry-authority-worker',
  'telemetry:health':    '../retry-cadence-kernel/workers/telemetry-retry-health-worker',
  'telemetry:systemic':  '../retry-cadence-kernel/workers/telemetry-retry-systemic-worker',
  'telemetry:capability': '../retry-cadence-kernel/workers/telemetry-retry-capability-worker',
  // ── Persist-telemetry retry domain (phase 3) ─────────────────────────
  // The stub was replaced by the connection-recovery-worker under the
  // retry-execution-substrate. The substrate-registry still maps the
  // domain to a retry worker for validate() symmetry.
  'persist-telemetry':      '../retry-cadence-kernel/workers/connection-recovery-worker',
  'persist-telemetry-read': '../retry-cadence-kernel/workers/connection-recovery-worker',
};

// Classification workers — semantically blind, bounded. They receive
// a raw error payload, run classification rules, return a classified
// action tag. They do not decide retry vs skip vs break. They do not
// mutate state. They only classify. The FSM consumes the classification.
//
// The same classification-worker module handles all domains (read and
// publish) — the IG error shape is the same.
const CLASSIFICATION_WORKER_MAP = {
  comments:           '../retry-cadence-kernel/workers/classification-worker',
  messages:           '../retry-cadence-kernel/workers/classification-worker',
  ugc:                '../retry-cadence-kernel/workers/classification-worker',
  insights:           '../retry-cadence-kernel/workers/classification-worker',
  media:              '../retry-cadence-kernel/workers/classification-worker',
  'publish:post':     '../retry-cadence-kernel/workers/classification-worker',
  'publish:story':    '../retry-cadence-kernel/workers/classification-worker',
  'publish:comment':  '../retry-cadence-kernel/workers/classification-worker',
  'publish:message':  '../retry-cadence-kernel/workers/classification-worker',
  'dedup:redis':      '../retry-cadence-kernel/workers/classification-worker',
  'dedup:repair':     '../retry-cadence-kernel/workers/classification-worker',
  reconciliation:     '../retry-cadence-kernel/workers/classification-worker',
  // Telemetry retry classification — shared across all 5 namespaces
  'telemetry:runtime':   '../retry-cadence-kernel/workers/classification-worker',
  'telemetry:integrity': '../retry-cadence-kernel/workers/classification-worker',
  'telemetry:authority': '../retry-cadence-kernel/workers/classification-worker',
  'telemetry:health':    '../retry-cadence-kernel/workers/classification-worker',
  'telemetry:systemic':  '../retry-cadence-kernel/workers/classification-worker',
  'telemetry:capability': '../retry-cadence-kernel/workers/classification-worker',
  // Persist-telemetry classification is owned by the substrate
  // (persistence-failure-substrate.js). The retry-cadence-kernel does
  // NOT re-classify — it consumes the substrate's errorShape. So
  // this entry maps to the substrate module, not a separate worker.
  // The FSM's DB_PERSIST_FAILURE handler treats the substrate's
  // reportFailure output as authoritative.
  'persist-telemetry':      '../postgres-telemetry-kernel/substrates/persistence-failure-substrate',
  'persist-telemetry-read': '../postgres-telemetry-kernel/substrates/persistence-failure-substrate',
};

// DOMAIN_REGISTRY — the canonical set of domain names. Publish
// domains are included so the validate() function can check the
// worker maps. The `execute` field is the publish substrate's
// execute (the outbound action).
const DOMAIN_REGISTRY = {
  comments:         { fetch: engagement.fetch.bind(engagement) },
  messages:         { fetch: engagement.fetch.bind(engagement) },
  ugc:              { fetch: ugcContent.fetch.bind(ugcContent) },
  insights:         { fetch: insights.fetch.bind(insights) },
  media:            { fetch: ugcContent.fetch.bind(ugcContent) },
  'publish:post':   { execute: publishContent.execute.bind(publishContent) },
  'publish:story':  { execute: publishContent.execute.bind(publishContent) },
  'publish:comment':{ execute: publishEngagement.execute.bind(publishEngagement) },
  'publish:message':{ execute: publishEngagement.execute.bind(publishEngagement) },
  'dedup:redis':      { },  // governance domain — Redis dedup retry
  'dedup:repair':     { },  // governance domain — conversation repair retry
  reconciliation:     { },  // governance domain — reconciliation cycle retry
  // Telemetry retry — 5 namespace-specific domains
  'telemetry:runtime':   { },  // runtime projection retry
  'telemetry:integrity': { },  // integrity projection retry
  'telemetry:authority': { },  // authority projection retry
  'telemetry:health':    { },  // health projection retry
  'telemetry:systemic':  { },  // systemic pressure projection retry
  'telemetry:capability': { },  // capability projection retry
  // ── Persist-telemetry retry domain (base phase) ─────────────────
  // Two domains: writes and reads. The substrate is the persistence
  // domain itself; the FSM is the executor. No fetch or execute
  // binding here — the retry worker is the only worker (currently
  // a stub; phase 2 will replace it).
  'persist-telemetry':      { },
  'persist-telemetry-read': { },
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
  try {
    return require(workerPath);
  } catch (err) {
    if (err.code === 'MODULE_NOT_FOUND') return null;
    throw err;
  }
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
 *
 * Parsing workers are OPTIONAL: outbound (publish:*) domains have
 * no parsing worker (no response shape to parse). The check for
 * PARSING_WORKER_MAP only runs if the domain has a parsing entry.
 */
function validate() {
  const domains = new Set(Object.keys(DOMAIN_REGISTRY));
  const issues = [];

  // PARSING_WORKER_MAP: optional. Only validate that every entry
  // in PARSING_WORKER_MAP corresponds to a known domain.
  for (const k of Object.keys(PARSING_WORKER_MAP)) {
    if (!domains.has(k)) issues.push(`PARSING_WORKER_MAP has unknown domain: ${k}`);
  }

  // RETRY_WORKER_MAP: required for every domain (every domain can
  // fail and need a retry).
  for (const d of domains) {
    if (!RETRY_WORKER_MAP[d]) issues.push(`RETRY_WORKER_MAP missing domain: ${d}`);
  }
  for (const k of Object.keys(RETRY_WORKER_MAP)) {
    if (!domains.has(k)) issues.push(`RETRY_WORKER_MAP has unknown domain: ${k}`);
  }

  // CLASSIFICATION_WORKER_MAP: required for every domain (every
  // domain can fail and need classification).
  for (const d of domains) {
    if (!CLASSIFICATION_WORKER_MAP[d]) {
      issues.push(`CLASSIFICATION_WORKER_MAP missing domain: ${d}`);
    }
  }
  for (const k of Object.keys(CLASSIFICATION_WORKER_MAP)) {
    if (!domains.has(k)) issues.push(`CLASSIFICATION_WORKER_MAP has unknown domain: ${k}`);
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
