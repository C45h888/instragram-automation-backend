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
const insights    = require('./substrates/insights-substrate');
const webhookAcquisition =
  require('./substrates/webhook-acquisition-substrate');

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
  insights:  './substrates/parsing-substrate/workers/insights-parser',
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
  insights:           '../retry-cadence-kernel/workers/insights-retry-worker',
  // ── Publish retry workers (Step 6) ────────────────────────
  // Two workers, bound to the two publish substrates. The
  // dual-binding pattern: each publish:* domain has a dedicated
  // retry worker that imports its substrate directly.
  // ── Publish retry workers — REMOVED ─────────────────────────────────
  // The publish retry workers were a static re-classification layer
  // that duplicated work the publish substrate already does via
  // substrates/transport/error-classifier.categorizeIgError.
  // The publishing-kernel/orchestrator now emits WORKER_OUTCOME_REPORTED
  // directly with the substrate's already-classified errorShape.
  // Publish failures are no longer routed through the retry-cadence
  // path; the publishing FSM owns the publish failure surface.
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
  'telemetry:persist-telemetry': '../retry-cadence-kernel/workers/telemetry-retry-persist-telemetry-worker',
  'telemetry:reconciliation': '../retry-cadence-kernel/workers/telemetry-retry-reconciliation-worker',
  'telemetry:scheduling': '../retry-cadence-kernel/workers/telemetry-retry-scheduling-worker',
  'telemetry:dedup': '../retry-cadence-kernel/workers/telemetry-retry-dedup-worker',
  'telemetry:publishing': '../retry-cadence-kernel/workers/telemetry-retry-publishing-worker',
  'telemetry:acquisition': '../retry-cadence-kernel/workers/telemetry-retry-acquisition-worker',
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
  insights:           '../retry-cadence-kernel/workers/classification-worker',
  // Publish classification — owned by the Instagram Reliability
  // Substrate (substrates/ig-reliability-substrate.js).
  // The 4 publish:* domains flow through IG_FAILURE_OBSERVED →
  // engagement-fsm → substrate.analyzeFailure(). The substrate
  // returns the canonical analysis; the FSM emits *_AUTHORIZED
  // actions per analysis.recommendations. Kernels MUST NOT
  // classify errors directly — that is semantic contamination.
  // The substrate is the canonical IG failure interpreter.
  'publish:post':     '../substrates/ig-reliability-substrate',
  'publish:story':    '../substrates/ig-reliability-substrate',
  'publish:comment':  '../substrates/ig-reliability-substrate',
  'publish:message':  '../substrates/ig-reliability-substrate',
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
  'telemetry:persist-telemetry': '../retry-cadence-kernel/workers/classification-worker',
  'telemetry:reconciliation': '../retry-cadence-kernel/workers/classification-worker',
  'telemetry:scheduling': '../retry-cadence-kernel/workers/classification-worker',
  'telemetry:dedup': '../retry-cadence-kernel/workers/classification-worker',
  'telemetry:publishing': '../retry-cadence-kernel/workers/classification-worker',
  'telemetry:acquisition': '../retry-cadence-kernel/workers/classification-worker',
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
// Webhook worker map — domain-bounded, one worker per event type.
// Workers live in the acquisition kernel; they mount on the
// ig-reliability-substrate bedrock for failure analysis.
// Used by substrateRegistry.getWebhookWorker(domain).
const WEBHOOK_WORKER_MAP = {
  'webhook:messages':         './substrates/webhook-acquisition-substrate/workers/messages-worker',
  'webhook:comments':         './substrates/webhook-acquisition-substrate/workers/comments-worker',
  'webhook:mentions':         './substrates/webhook-acquisition-substrate/workers/mentions-worker',
  'webhook:story-mentions':   './substrates/webhook-acquisition-substrate/workers/story-mentions-worker',
  'webhook:comment-replies':  './substrates/webhook-acquisition-substrate/workers/comment-replies-worker',
  'webhook:live-comments':    './substrates/webhook-acquisition-substrate/workers/live-comments-worker',
  'webhook:message-reactions':'./substrates/webhook-acquisition-substrate/workers/message-reactions-worker',
  'webhook:message-seen':     './substrates/webhook-acquisition-substrate/workers/message-seen-worker',
  'webhook:standby':          './substrates/webhook-acquisition-substrate/workers/standby-worker',
  'webhook:media-publish':    './substrates/webhook-acquisition-substrate/workers/media-publish-worker',
  'webhook:tags':             './substrates/webhook-acquisition-substrate/workers/tags-worker',
};

const DOMAIN_REGISTRY = {
  comments:         { fetch: engagement.fetch.bind(engagement) },
  messages:         { fetch: engagement.fetch.bind(engagement) },
  insights:         { fetch: insights.fetch.bind(insights) },
  // ── Webhook acquisition domains (Phase 1) ────────────────────────
  // The substrate owns routing; the binding exposes process so any
  // kernel can ask for the canonical processWebhook entry point.
  'webhook:messages':         { process: webhookAcquisition.processWebhook.bind(webhookAcquisition) },
  'webhook:comments':         { process: webhookAcquisition.processWebhook.bind(webhookAcquisition) },
  'webhook:mentions':         { process: webhookAcquisition.processWebhook.bind(webhookAcquisition) },
  'webhook:story-mentions':   { process: webhookAcquisition.processWebhook.bind(webhookAcquisition) },
  'webhook:comment-replies':  { process: webhookAcquisition.processWebhook.bind(webhookAcquisition) },
  'webhook:live-comments':    { process: webhookAcquisition.processWebhook.bind(webhookAcquisition) },
  'webhook:message-reactions':{ process: webhookAcquisition.processWebhook.bind(webhookAcquisition) },
  'webhook:message-seen':     { process: webhookAcquisition.processWebhook.bind(webhookAcquisition) },
  'webhook:standby':          { process: webhookAcquisition.processWebhook.bind(webhookAcquisition) },
  'webhook:media-publish':    { process: webhookAcquisition.processWebhook.bind(webhookAcquisition) },
  'webhook:tags':             { process: webhookAcquisition.processWebhook.bind(webhookAcquisition) },
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
  'telemetry:persist-telemetry': { },  // persist-telemetry projection retry
  'telemetry:reconciliation': { },  // reconciliation projection retry
  'telemetry:scheduling': { },  // scheduling projection retry
  'telemetry:dedup': { },  // dedup projection retry
  'telemetry:publishing': { },  // publishing projection retry
  'telemetry:acquisition': { },  // acquisition projection retry
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
 * It receives a raw error, returns classified action tag. The FSM
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

/**
 * Return the bounded webhook worker for a webhook domain.
 * One worker per event type (messages, comments, mentions, story-mentions).
 * Workers are semantically isolated; each one validates + normalizes one
 * event shape and dispatches to the acquisition-fsm.
 *
 * @param {string} domain — 'webhook:messages' | 'webhook:comments' | etc.
 * @returns {object|null} worker module with execute()
 */
function getWebhookWorker(domain) {
  const workerPath = WEBHOOK_WORKER_MAP[domain];
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
  getWebhookWorker,
  domainForAction,
  fetchTypeForAction,
  allDomains,
  validate,
};
