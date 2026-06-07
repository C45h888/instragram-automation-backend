// dedup-kernel/orchestrator.js
// Dedup Orchestrator: constitutional coordination membrane for dedup domain.
//
// Owns: dedup substrate reference (ONLY module outside the kernel internals
//       that holds this import), CK action subscriptions, mechanical forwarding.
// Does NOT own: dedup intelligence (FSM), dedup I/O (workers), governance policy.
//
// Architectural invariant:
//   External callers → CK (event dispatch) → dedup FSM (intelligence)
//     → workers (bounded I/O) → CK (outcome) → FSM (process)
//   The orchestrator is a THIN MEMBRANE — routes mechanically, never interprets.
//
// Pattern mirrors acquisition-kernel/orchestrator.js: subscribe to CK actions,
// mechanically forward, emit observability transitions.

const dedupSubstrate = require('./substrates/dedup');

// ── Governance reference (set by wire()) ──────────────────────────────────
let _governance = null;
let _dedupFsm = null;

/**
 * Wire this orchestrator to the constitutional kernel.
 * Registers per-action-type subscribers for dedup output actions.
 *
 * @param {object} gov — constitutional kernel module
 * @param {object} dedupFsm — dedup domain FSM (for state queries)
 */
function wire(gov, dedupFsm) {
  _governance = gov;
  _dedupFsm = dedupFsm;

  // ── DEDUP_INTENT_CHECKED → observability only ───────────────────────
  // FSM emits this after CHECK_AND_MARK_DEDUP completes. The orchestrator
  // records the transition for observability. The caller (publishing
  // orchestrator) reads the result from CK's dispatch return value.
  gov.subscribeAction('DEDUP_INTENT_CHECKED', (action) => {
    _emitTransition({
      domain: 'dedup',
      entity: 'intent_check',
      entityId: action.intentId || action.resourceId,
      previousState: 'CHECKING',
      nextState: action.blocked ? 'BLOCKED' : (action.isReplay ? 'REPLAY' : 'ALLOWED'),
      authority: 'dedup-orchestrator',
      raw: {
        accountId: action.accountId,
        actionType: action.actionType,
        resourceId: action.resourceId,
        intentId: action.intentId,
        blocked: action.blocked,
        isReplay: action.isReplay,
        reason: action.reason,
        existingIntentId: action.existingIntentId,
      },
    });
  });

  // ── DEDUP_BATCH_CLOSED → observability ────────────────────────────
  gov.subscribeAction('DEDUP_BATCH_CLOSED', (action) => {
    const snapshot = dedupSubstrate.getInflightSnapshot();
    _emitTransition({
      domain: 'dedup',
      entity: 'batch',
      entityId: action.accountId || 'global',
      previousState: 'ACTIVE',
      nextState: 'CLOSED',
      authority: 'dedup-orchestrator',
      raw: {
        totalMarks: action.totalMarks,
        totalReplays: action.totalReplays,
        totalOrphans: action.totalOrphans,
        replayRate: action.replayRate,
        orphanRate: action.orphanRate,
        degradationCount: action.degradationCount,
        inflightSnapshot: snapshot,
      },
    });
  });
}

// ── Substrate delegation methods ──────────────────────────────────────────
// These are the canonical internal entry points for dedup I/O within the
// kernel. The FSM calls the substrate directly (it IS the intelligence).
// The orchestrator exposes these for external kernel callers (reconciliation).
// Workers are thin wrappers around these substrate methods.

/**
 * Check if a resource+intent is already in-flight.
 * Delegates to canonical substrate.isInFlight().
 *
 * @returns {Promise<{ blocked: boolean, reason: string|null, existingIntentId: string|null }>}
 */
async function checkDedup(accountId, actionType, resourceId, intentId) {
  return dedupSubstrate.isInFlight(accountId, actionType, resourceId, intentId);
}

/**
 * Mark a resource+intent as in-flight.
 * Delegates to canonical substrate.markInFlight().
 */
async function markInFlight(accountId, actionType, resourceId, intentId) {
  return dedupSubstrate.markInFlight(accountId, actionType, resourceId, { intentId });
}

/**
 * Clear the identity cache after each evaluation batch.
 * Delegates to canonical substrate.clearTick().
 */
function clearTick() {
  dedupSubstrate.clearTick();
}

/**
 * Return a snapshot of current substrate state for reconciliation.
 */
function getInflightSnapshot() {
  return dedupSubstrate.getInflightSnapshot();
}

// ── Internal helpers ─────────────────────────────────────────────────────

function _emitTransition(params) {
  try {
    const observability = require('../../control-plane/observability/emitters/transition-emitter');
    observability.transition(params);
  } catch (err) {
    console.warn('[dedup-orchestrator] Observability transition error:', err.message);
  }
}

module.exports = { wire, checkDedup, markInFlight, clearTick, getInflightSnapshot };
