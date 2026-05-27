// control-plane/governance/domains/dedup-fsm.js
// Dedup Domain FSM: federated state machine governing deduplication lifecycle.
//
// Owns: dedup batch lifecycle (IDLE → ACTIVE → IDLE),
//        replay detection governance (escalation decisions),
//        orphan rate monitoring, constitutional transition validation.
// Does NOT own: Redis key mechanics (SET/GET/TTL), mechanical dedup checks,
//               intent emission, evaluation policy — those belong to the
//               dedup substrate and evaluation modules respectively.
//
// Reports to: constitutional kernel for transition validation + global observability.
//
// Architectural invariant:
//   Signals UP   → ctx.dispatchGlobal(event) reports degradation to constitutional
//   Authority ↓  → ctx.validate(from, to, event) asks constitutional for approval
//   Substrate ↓  → dedup substrate performs mechanical Redis ops (async)
//                  FSM governs lifecycle meaning, substrate performs mechanics
//
// Domain FSMs emit state transitions through the observability plane.
// The lineage worker consumes from the observability plane and writes to the
// canonical lineage ledger. FSMs do NOT write to the lineage ledger directly.
//
// Local states:
//   IDLE   — no dedup batch in progress, no active evaluation window
//   ACTIVE — dedup batch in progress, evaluation window open

// Lazy import to avoid circular dependency
let _observability = null;
function _obs() {
  if (!_observability) {
    try { _observability = require('../../observability/emitters/transition-emitter'); }
    catch (_) { _observability = null; }
  }
  return _observability;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 0. Governance Policy Constants — domain-owned thresholds
// ═══════════════════════════════════════════════════════════════════════════════

const REPLAY_RATE_DEGRADATION_THRESHOLD = 0.5;   // >50% replay rate in a batch signals degradation
const ORPHAN_RATE_DEGRADATION_THRESHOLD = 0.3;    // >30% orphan rate in a batch signals degradation

// ═══════════════════════════════════════════════════════════════════════════════
// 1. Local State Registry
// ═══════════════════════════════════════════════════════════════════════════════

const STATE_REGISTRY = {
  IDLE: {
    description: 'No dedup batch in progress — ready for evaluation windows',
  },
  ACTIVE: {
    description: 'Dedup batch in progress — evaluation window open, tracking marks and replays',
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 2. Domain Transition Map — event → target + guard + action builder
// ═══════════════════════════════════════════════════════════════════════════════

const TRANSITION_MAP = {
  // ── Evaluation begins → open dedup batch window ─────────────────────────
  DEDUP_BATCH_BEGIN: {
    target: 'ACTIVE',
    guard: (event) => {
      if (_localState !== 'IDLE') {
        return { allowed: false, reason: `Cannot begin batch from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      // Reset batch-level counters
      _batchMarks = 0;
      _batchReplays = 0;
      _batchOrphans = 0;
      return [{
        type: 'DEDUP_BATCH_OPENED',
        accountId: event.accountId,
        eventCount: event.eventCount || 0,
      }];
    },
  },

  // ── Intent marked in-flight by substrate → FSM records the mark ────────
  DEDUP_INTENT_MARKED: {
    target: (event) => _localState, // stay in current state
    guard: (event) => {
      if (_localState !== 'ACTIVE') {
        return { allowed: false, reason: `Cannot mark intent outside ACTIVE batch (current: ${_localState})` };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      _batchMarks++;

      const { intentId, resourceId, isReplay, accountId } = event;
      if (isReplay) {
        _batchReplays++;
      }

      return [];
    },
  },

  // ── Replay detected (different intent touching same resource) ──────────
  DEDUP_REPLAY_DETECTED: {
    target: (event) => _localState, // stay in ACTIVE
    guard: (event) => {
      if (_localState !== 'ACTIVE') {
        return { allowed: false, reason: `Cannot process replay outside ACTIVE batch (current: ${_localState})` };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      _batchReplays++;

      const { accountId, resourceId, previousIntentId, intentId } = event;

      // Track replay for batch-end evaluation
      if (!_replayResources.has(resourceId)) {
        _replayResources.set(resourceId, []);
      }
      _replayResources.get(resourceId).push({
        intentId,
        previousIntentId,
        ts: Date.now(),
      });

      return [];
    },
  },

  // ── Evaluation complete → close dedup batch, evaluate governance signals ─
  DEDUP_BATCH_END: {
    target: 'IDLE',
    guard: (event) => {
      if (_localState !== 'ACTIVE') {
        return { allowed: false, reason: `Cannot end batch from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      const actions = [];
      const totalOps = _batchMarks || 0;
      const replayCount = _batchReplays || 0;
      const orphanCount = _batchOrphans || 0;

      // ── Evaluate replay rate ──────────────────────────────────────────
      if (totalOps > 0) {
        const replayRate = replayCount / totalOps;
        if (replayRate > REPLAY_RATE_DEGRADATION_THRESHOLD) {
          actions.push({
            type: 'LOG_DEGRADED',
            substate: 'PARTIAL_FAILURE',
            reason: `Dedup replay rate ${(replayRate * 100).toFixed(0)}% exceeds threshold ${(REPLAY_RATE_DEGRADATION_THRESHOLD * 100).toFixed(0)}% (${replayCount}/${totalOps})`,
          });
          _degradationCount++;
        }
      }

      // ── Evaluate orphan rate ──────────────────────────────────────────
      if (totalOps > 0) {
        const orphanRate = orphanCount / totalOps;
        if (orphanRate > ORPHAN_RATE_DEGRADATION_THRESHOLD) {
          actions.push({
            type: 'LOG_DEGRADED',
            substate: 'PARTIAL_FAILURE',
            reason: `Dedup orphan rate ${(orphanRate * 100).toFixed(0)}% exceeds threshold ${(ORPHAN_RATE_DEGRADATION_THRESHOLD * 100).toFixed(0)}% (${orphanCount}/${totalOps})`,
          });
          _degradationCount++;
        }
      }

      // ── Batch summary action ──────────────────────────────────────────
      actions.push({
        type: 'DEDUP_BATCH_CLOSED',
        totalMarks: totalOps,
        totalReplays: replayCount,
        totalOrphans: orphanCount,
        replayRate: totalOps > 0 ? replayCount / totalOps : 0,
        orphanRate: totalOps > 0 ? orphanCount / totalOps : 0,
        degradationCount: _degradationCount,
      });

      // ── Clean up batch-local state ────────────────────────────────────
      _replayResources.clear();

      return actions;
    },
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 3. Domain-local runtime state (private)
// ═══════════════════════════════════════════════════════════════════════════════

let _localState = 'IDLE';

// ── Batch-level counters (reset on DEDUP_BATCH_BEGIN) ────────────────────────
let _batchMarks = 0;
let _batchReplays = 0;
let _batchOrphans = 0;

// ── Persistent tracking across batches ───────────────────────────────────────
let _degradationCount = 0;             // cumulative degradation signals emitted
const _replayResources = new Map();    // resourceId → [{ intentId, previousIntentId, ts }]

// ═══════════════════════════════════════════════════════════════════════════════
// 4. Dispatch — process event, ask constitutional for validation, transition
//
// Domain FSMs emit through observability plane (not lineage ledger).
// The lineage worker consumes these transitions and writes to canonical ledger.
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Process a domain event within the dedup FSM.
 *
 * @param {{ type: string, [key: string]: any }} event — domain event
 * @param {{ validate: Function, dispatchGlobal: Function, getGlobalState: Function }} ctx — constitutional kernel context
 * @returns {{ allowed: boolean, from?: string, to?: string, actions?: Array, reason?: string }}
 */
function dispatch(event, ctx) {
  if (!event || typeof event !== 'object' || typeof event.type !== 'string') {
    return { allowed: false, reason: `event must be { type: string }, got ${typeof event}` };
  }

  const txn = TRANSITION_MAP[event.type];
  if (!txn) {
    return { allowed: false, reason: `unknown event type: ${event.type}` };
  }

  const from = _localState;

  // 1. Run per-transition guard
  if (txn.guard) {
    const result = txn.guard(event);
    if (!result.allowed) {
      return { allowed: false, reason: result.reason || 'guard blocked' };
    }
  }

  // 2. Resolve target state
  const rawTarget = txn.target;
  const target = typeof rawTarget === 'function' ? rawTarget(event) : rawTarget;

  // null target = no state change
  if (target === null) {
    return { allowed: true, from, to: from, actions: [], reason: 'no-transition' };
  }

  // 3. Ask constitutional kernel for transition approval
  if (ctx && ctx.validate) {
    const validation = ctx.validate(from, target, event);
    if (!validation.allowed) {
      return { allowed: false, reason: validation.reason || 'constitutional validation failed' };
    }
  }

  // 4. Materialize state
  _localState = target;

  // 5. Emit observability transition for domain FSM state change
  // Fire-and-forget — observability failures never affect domain FSM behavior.
  // The lineage worker consumes this transition and writes to the canonical ledger.
  try {
    const obs = _obs();
    if (obs) {
      obs.transition({
        domain: 'dedup',
        entity: 'fsm',
        entityId: 'dedup-fsm',
        previousState: from,
        nextState: target,
        authority: 'dedup-fsm',
        raw: {
          intent: event.type,
          accountId: event.accountId || null,
          intentId: event.intentId || null,
          resourceId: event.resourceId || null,
          batchMarks: _batchMarks,
          batchReplays: _batchReplays,
        },
      });
    }
  } catch (_) {}

  // 6. Build actions
  const actions = txn.buildActions ? txn.buildActions(event) : [];

  console.log(`[dedup-fsm] ${from} → ${target}  (${event.type})`);

  return {
    allowed: true,
    from,
    to: target,
    actions,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 5. Initialization — called by constitutional kernel on boot with rehydrated state
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Initialize the domain FSM with rehydrated state from lineage.
 * Called by the constitutional kernel after rehydrate() completes on boot.
 *
 * @param {string} rehydratedState — the domain state to restore (e.g., 'ACTIVE', 'IDLE')
 */
function init(rehydratedState) {
  if (rehydratedState && typeof rehydratedState === 'string') {
    _localState = rehydratedState;
    console.log(`[dedup-fsm] Initialized with rehydrated state: ${rehydratedState}`);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// 6. Observability — domain state queries
// ═══════════════════════════════════════════════════════════════════════════════

function getState() {
  return _localState;
}

function exportState() {
  return {
    state: _localState,
    batchMarks: _batchMarks,
    batchReplays: _batchReplays,
    batchOrphans: _batchOrphans,
    degradationCount: _degradationCount,
    replayResourceCount: _replayResources.size,
  };
}

function getHealth() {
  return {
    ok: _degradationCount === 0 && _batchReplays < _batchMarks * 0.5,
    signals: {
      state: _localState,
      activeBatch: _localState === 'ACTIVE',
      degradationCount: _degradationCount,
      currentReplayRate: _batchMarks > 0 ? _batchReplays / _batchMarks : 0,
    },
  };
}

// ── Reconciliation engine getters — expose domain state for three-reality comparison ──

function getBatchState() {
  return {
    marks: _batchMarks,
    replays: _batchReplays,
    orphans: _batchOrphans,
    active: _localState === 'ACTIVE',
  };
}

function getReplayResources() {
  return new Map(_replayResources);
}

function getDegradationCount() {
  return _degradationCount;
}

module.exports = {
  name: 'dedup',
  dispatch,
  init,
  getState,
  exportState,
  getHealth,
  getBatchState,
  getReplayResources,
  getDegradationCount,
};
