// control-plane/observability/normalizer.js
// Normalizer: transforms raw subsystem events into canonical STATE_TRANSITION schema.
//
// Owns: mapping raw subsystem event semantics to a uniform transition schema.
// Does NOT own: state storage, projection logic, query interfaces.
//
// Every raw event type from every subsystem maps to exactly one canonical form:
//   { type: 'STATE_TRANSITION', domain, entity, entityId, previousState, nextState, ... }
//
// The normalizer is a pure function — no side effects, no state mutation.
// It is called by the transition emitter on every observability.transition() call.

const { getCurrentContext } = require('./context');

// ── Normalization rules per event type ────────────────────────────────────────
// Each rule maps a raw { domain, entity, entityId, previousState, nextState, ... }
// observation to the canonical schema.
//
// Rules are registered by domain + entity + (previousState → nextState) pattern.
// The normalizer handles raw events from ALL subsystems uniformly.

const _rules = new Map(); // key: "domain:entity:previousState:nextState" → normalizer fn

/**
 * Register a normalization rule for a specific transition pattern.
 *
 * @param {string} domain — 'acquisition'|'publishing'|'scheduling'|'execution'|'governance'|'realtime'|'buffer'|'quota' etc.
 * @param {string} entity — the entity type, e.g. 'circuit_breaker', 'dedup_entry', 'buffer', 'attempt'
 * @param {string} previousState — the state before transition, or null for new entities
 * @param {string} nextState — the state after transition
 * @param {Function} normalizer — (raw, ctx) => partial canonical transition
 */
function addRule(domain, entity, previousState, nextState, normalizer) {
  const key = _makeRuleKey(domain, entity, previousState, nextState);
  _rules.set(key, normalizer);
}

function _makeRuleKey(domain, entity, previousState, nextState) {
  return `${domain}:${entity}:${previousState || '_null'}:${nextState || '_null'}`;
}

/**
 * Normalize a raw transition event into the canonical STATE_TRANSITION schema.
 * Falls back to a best-effort generic normalization if no rule matches.
 *
 * @param {object} raw — raw event from the subsystem
 * @param {object} overrideCtx — context override (typically not needed — auto-populated)
 * @returns {object} canonical STATE_TRANSITION
 */
function normalize(raw, overrideCtx) {
  const ctx = overrideCtx || getCurrentContext();

  // Extract the key fields — most raw events follow the emitter signature:
  // { domain, entity, entityId, previousState, nextState, authority, raw }
  const {
    domain = ctx.domain || 'unknown',
    entity = 'unknown',
    entityId = null,
    previousState = null,
    nextState = null,
    authority = ctx.authority || 'unknown',
    raw: originalRaw = {},
  } = raw;

  // Try to find a matching rule
  const ruleKey = _makeRuleKey(domain, entity, previousState || null, nextState || null);
  const rule = _rules.get(ruleKey);

  const base = {
    type: 'STATE_TRANSITION',
    domain,
    entity,
    entityId: entityId ? String(entityId) : null,
    previousState: previousState || null,
    nextState: nextState || null,
    traceId: ctx.traceId,
    correlationId: ctx.correlationId || null,
    causationId: ctx.causationId || null,
    authority,
    timestamp: Date.now(),
    raw: originalRaw,
  };

  if (rule) {
    return { ...base, ...rule(originalRaw, ctx) };
  }

  // No matching rule — return the base normalized form
  return base;
}

/**
 * Normalize a signal-bus event into a canonical transition.
 *
 * signalBus emits: { topic, data } where topic is 'db:insert'
 * and data is { accountId, table, record }.
 *
 * This normalizes db:insert signals into transitions:
 *   domain: 'realtime', entity: 'db_event', entityId: `${table}:${record.id}`
 *
 * @param {string} topic — signal bus topic
 * @param {object} data — signal bus payload
 * @returns {object} canonical STATE_TRANSITION
 */
function normalizeSignal(topic, data) {
  const ctx = getCurrentContext();
  if (topic === 'db:insert') {
    const { accountId, table, record } = data || {};
    return {
      type: 'STATE_TRANSITION',
      domain: 'realtime',
      entity: 'db_event',
      entityId: record && record.id ? `${table}:${record.id}` : `${table}:unknown`,
      previousState: null,
      nextState: 'INSERTED',
      traceId: ctx.traceId,
      correlationId: ctx.correlationId || null,
      causationId: ctx.causationId || null,
      authority: 'signal-bus',
      timestamp: Date.now(),
      raw: { topic, accountId, table, record },
    };
  }

  // Unknown topic — still normalize but mark as unknown
  return {
    type: 'STATE_TRANSITION',
    domain: 'realtime',
    entity: 'signal',
    entityId: topic,
    previousState: null,
    nextState: 'EMITTED',
    traceId: ctx.traceId,
    correlationId: ctx.correlationId || null,
    causationId: ctx.causationId || null,
    authority: 'signal-bus',
    timestamp: Date.now(),
    raw: { topic, data },
  };
}

// ── Pre-register canonical normalization rules ─────────────────────────────────

// Quota state transitions
addRule('quota', 'quota', null, 'ELEVATED', (raw) => ({
  nextState: 'ELEVATED',
  raw: { ...raw, threshold: 50 },
}));
addRule('quota', 'quota', 'ELEVATED', 'CRITICAL', (raw) => ({
  nextState: 'CRITICAL',
  raw: { ...raw, threshold: 80 },
}));
addRule('quota', 'quota', 'CRITICAL', 'ELEVATED', (raw) => ({
  nextState: 'ELEVATED',
  raw: { ...raw, recovery: true },
}));
addRule('quota', 'quota', 'ELEVATED', 'NORMAL', (raw) => ({
  nextState: 'NORMAL',
  raw: { ...raw, recovery: true },
}));

// Metrics health signal transitions
addRule('metrics', 'health_signal', 'HEALTHY', 'DEGRADED', (raw) => ({
  raw: { ...raw, failureRate: raw.failureRate || 0 },
}));
addRule('metrics', 'health_signal', 'DEGRADED', 'HEALTHY', (raw) => ({
  raw: { ...raw, failureRate: raw.failureRate || 0 },
}));

// Dedup entry transitions
addRule('dedup', 'dedup_entry', 'PENDING', 'IN_FLIGHT', () => ({}));
addRule('dedup', 'dedup_entry', 'IN_FLIGHT', 'CLEARED', () => ({}));

// Buffer transitions
addRule('buffer', 'buffer', 'IDLE', 'INGESTING', (raw) => ({
  entityId: raw.accountId || 'unknown',
  raw,
}));
addRule('buffer', 'buffer', 'INGESTING', 'FLUSHING', (raw) => ({
  entityId: raw.accountId || 'unknown',
  raw,
}));
addRule('buffer', 'buffer', 'FLUSHING', 'IDLE', (raw) => ({
  entityId: raw.accountId || 'unknown',
  raw,
}));
addRule('buffer', 'buffer', null, 'DESTROYED', (raw) => ({
  entityId: raw.accountId || 'unknown',
  raw,
}));

// Cadence transitions
addRule('cadence', 'cadence', 'IDLE', 'TICKING', () => ({}));
addRule('cadence', 'cadence', 'TICKING', 'IDLE', () => ({}));
addRule('cadence', 'cadence', 'IDLE', 'STOPPED', () => ({}));

// Evaluator transitions
addRule('evaluation', 'evaluator', 'IDLE', 'EVALUATING', (raw) => ({
  entityId: raw.accountId || 'unknown',
  raw,
}));
addRule('evaluation', 'evaluator', 'EVALUATING', 'IDLE', (raw) => ({
  entityId: raw.accountId || 'unknown',
  raw,
}));
addRule('evaluation', 'evaluator', 'EVALUATING', 'EMITTING', (raw) => ({
  entityId: raw.accountId || 'unknown',
  raw,
}));

// Emission transitions
addRule('emission', 'queue_intent', 'PENDING', 'QUEUED', (raw) => ({
  entityId: raw.intentId || raw.intent_id || null,
  raw,
}));
addRule('emission', 'mutation', 'PENDING', 'APPLIED', (raw) => ({
  entityId: raw.id || null,
  raw,
}));

// DB Scanner transitions
addRule('db-scanner', 'db_scanner', 'IDLE', 'SCANNING', () => ({}));
addRule('db-scanner', 'db_scanner', 'SCANNING', 'IDLE', () => ({}));
addRule('db-scanner', 'intent', 'PENDING', 'EMITTED', (raw) => ({
  entityId: raw.intentId || null,
  raw,
}));

// Lifecycle transitions
addRule('lifecycle', 'account', 'UNKNOWN', 'ACTIVE', (raw) => ({
  entityId: raw.accountId || null,
  raw,
}));
addRule('lifecycle', 'account', 'ACTIVE', 'REMOVED', (raw) => ({
  entityId: raw.accountId || null,
  raw,
}));

// Signal intake transitions
addRule('realtime', 'realtime', 'STOPPED', 'SUBSCRIBED', () => ({}));
addRule('realtime', 'realtime', 'SUBSCRIBED', 'STOPPED', () => ({}));

// Sync substrate transitions
addRule('sync', 'sync', 'STOPPED', 'POLLING', () => ({}));
addRule('sync', 'sync', 'POLLING', 'STOPPED', () => ({}));
addRule('sync', 'sync_intent', 'POLLED', 'RECEIVED', (raw) => ({
  entityId: raw.intentId || null,
  raw,
}));

// Acquisition orchestrator transitions
addRule('acquisition', 'acquisition_intent', 'RECEIVED', 'EXECUTING', (raw) => ({
  entityId: raw.intentId || null,
  raw,
}));
addRule('acquisition', 'acquisition_intent', 'EXECUTING', 'RETRYING', (raw) => ({
  entityId: raw.intentId || null,
  raw,
}));

// Retry worker transitions
addRule('execution', 'attempt', 'PENDING', 'ATTEMPTING', (raw) => ({
  entityId: raw.intentId || null,
  raw,
}));
addRule('execution', 'attempt', 'ATTEMPTING', 'COMPLETED', (raw) => ({
  entityId: raw.intentId || null,
  raw,
}));
addRule('execution', 'attempt', 'ATTEMPTING', 'FAILED', (raw) => ({
  entityId: raw.intentId || null,
  raw,
}));
addRule('execution', 'attempt', 'PENDING', 'SKIPPED', (raw) => ({
  entityId: raw.intentId || null,
  raw,
}));

// Emission orchestrator transitions
addRule('emission', 'pipeline', 'IDLE', 'RUNNING', (raw) => ({
  entityId: raw.accountId || null,
  raw,
}));
addRule('emission', 'pipeline', 'RUNNING', 'IDLE', (raw) => ({
  entityId: raw.accountId || null,
  raw,
}));
addRule('emission', 'pipeline', 'RUNNING', 'ERROR', (raw) => ({
  entityId: raw.accountId || null,
  raw,
}));

// Degradation transitions
addRule('governance', 'runtime', 'HEALTHY', 'DEGRADED', (raw) => ({
  raw: { ...raw, substate: raw.substate || 'UNKNOWN' },
}));
addRule('governance', 'runtime', 'DEGRADED', 'HEALTHY', () => ({}));
addRule('governance', 'runtime', 'DEGRADED', 'RECOVERY', () => ({}));
addRule('governance', 'runtime', 'RECOVERY', 'HEALTHY', () => ({}));
addRule('governance', 'runtime', 'BOOTING', 'HEALTHY', () => ({}));
addRule('governance', 'runtime', 'ANY', 'HALTED', (raw) => ({
  raw: { ...raw, reason: raw.reason || null },
}));

// System alert
addRule('governance', 'alert', null, 'RAISED', (raw) => ({
  entityId: raw.alertType || 'unknown',
  raw,
}));

// Governance domain FSM transitions (generic — applies to all FSMs)
addRule('acquisition', 'fsm', null, null, (raw) => ({
  entityId: 'acquisition-fsm',
  raw,
}));
addRule('publishing', 'fsm', null, null, (raw) => ({
  entityId: 'publishing-fsm',
  raw,
}));
addRule('scheduling', 'fsm', null, null, (raw) => ({
  entityId: 'scheduling-fsm',
  raw,
}));

module.exports = { normalize, normalizeSignal, addRule };
