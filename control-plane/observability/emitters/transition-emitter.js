// control-plane/observability/emitters/transition-emitter.js
// Transition Emitter: universal runtime emission point for observability.
//
// Owns: validating and routing transition calls into the observability plane.
// Does NOT own: normalization logic, state projection, context propagation.
//
// This is the SINGLE import point for ALL subsystems in the codebase.
// Every state mutation in the runtime calls observability.transition() here.
//
// Usage:
//   const observability = require('../index')
//   observability.transition({ domain: 'execution', entity: 'attempt', ... })
//
// Architectural rules:
//   - observability.transition() NEVER throws — failures are swallowed with a console.warn
//   - Subsystems import from 'observability/emitters/transition-emitter' to avoid circular deps
//   - The emitter is write-only from subsystems' perspective — no query methods exposed

const normalizer = require('../normalizer');
const projection = require('../projection');
const { getCurrentContext } = require('../context');

/**
 * Emit a state transition into the observability plane.
 *
 * Every state mutation in the runtime should call this before mutating.
 * The emitter normalizes the raw transition, projects it into the in-memory
 * state projection, and threads context for causal chain reconstruction.
 *
 * @param {object} params — transition parameters
 * @param {string} params.domain — runtime domain, e.g. 'acquisition', 'publishing', 'execution', 'governance'
 * @param {string} params.entity — entity type, e.g. 'attempt', 'circuit_breaker', 'buffer', 'fsm'
 * @param {string} [params.entityId] — entity identifier, e.g. accountId, intentId, attemptId
 * @param {string|null} [params.previousState] — state before mutation, or null for new entities
 * @param {string} params.nextState — state after mutation
 * @param {string} [params.authority] — the subsystem initiating this mutation
 * @param {object} [params.raw] — original event data for debugging
 *
 * @example
 *   observability.transition({
 *     domain: 'execution',
 *     entity: 'attempt',
 *     entityId: intentId,
 *     previousState: 'PENDING',
 *     nextState: 'ATTEMPTING',
 *     authority: 'retry-worker',
 *     raw: { accountId, domain },
 *   });
 */
function transition(params) {
  if (!params || typeof params !== 'object') {
    console.warn('[transition-emitter] transition() called with non-object params:', typeof params);
    return;
  }

  const { domain, entity, entityId, previousState, nextState, authority, raw } = params;

  if (!domain || !entity) {
    console.warn('[transition-emitter] transition() requires domain and entity:', params);
    return;
  }

  if (!nextState) {
    console.warn('[transition-emitter] transition() requires nextState:', params);
    return;
  }

  try {
    // Auto-populate authority from context if not provided
    const ctx = getCurrentContext();
    const resolvedAuthority = authority || ctx.authority || 'unknown';

    // Build the raw transition object
    const rawTransition = {
      domain,
      entity,
      entityId,
      previousState,
      nextState,
      authority: resolvedAuthority,
      raw: raw || {},
    };

    // Normalize to canonical schema
    const normalized = normalizer.normalize(rawTransition, ctx);

    // Derive parentTransitionId from the correlation chain (Gap 5 fix).
    // If a previous transition with the same correlationId exists in the log,
    // its traceId becomes this transition's parentTransitionId.
    // This reconstructs the causal chain: correlationId groups related transitions,
    // and parentTransitionId links them in order.
    if (normalized.correlationId) {
      const parent = projection.findLastEntry(
        (entry) =>
          entry.traceId !== normalized.traceId &&
          entry.correlationId === normalized.correlationId
      );
      if (parent) {
        normalized.parentTransitionId = parent.traceId;
      }
    }

    // Project into the in-memory state store
    projection.project(normalized);
  } catch (err) {
    // Never let observability failures propagate — subsystems must not be disrupted
    console.warn('[transition-emitter] Transition emit error:', err.message);
  }
}

/**
 * Capture a signal-bus emission event.
 * Called by the signal-bus integration wrapper for every emit().
 *
 * @param {string} topic — signal bus topic, e.g. 'db:insert'
 * @param {object} data — signal bus payload
 */
function captureSignal(topic, data) {
  try {
    const normalized = normalizer.normalizeSignal(topic, data);
    projection.project(normalized);
  } catch (err) {
    console.warn('[transition-emitter] Signal capture error:', err.message);
  }
}

module.exports = { transition, captureSignal };
