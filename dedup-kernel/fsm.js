// dedup-kernel/fsm.js
// Dedup Domain FSM: federated state machine governing deduplication lifecycle.
// Migrated from control-plane/governance/domains/dedup-fsm.js
//
// Owns: dedup batch lifecycle (IDLE → ACTIVE → IDLE),
//       replay detection governance (escalation decisions),
//       orphan rate monitoring, constitutional transition validation.
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
    try { _observability = require('../../control-plane/observability/emitters/transition-emitter'); }
    catch (_) { _observability = null; }
  }
  return _observability;
}

// Dedup substrate — called directly by FSM for mechanical cleanup (clearTick).
// The FSM is the intelligence layer; it owns the substrate for non-worker
// operations. Workers handle bounded I/O; substrate performs mechanics.
let _dedupSubstrate = null;
function _substrate() {
  if (!_dedupSubstrate) {
    _dedupSubstrate = require('./substrates/dedup');
  }
  return _dedupSubstrate;
}

// Mutation/emission dedup substrate — Phase 8 extension.
// Owns: mutation-layer and emission-layer dedup checks and marks.
// Separate from the intake dedup substrate to keep namespaces isolated.
let _mutationDedupSubstrate = null;
function _mutationSubstrate() {
  if (!_mutationDedupSubstrate) {
    _mutationDedupSubstrate = require('./substrates/dedup-mutation');
  }
  return _mutationDedupSubstrate;
}

// ═══════════════════════════════════════════════════════════════════════════════

// ── Governance reference (set by CK at boot) ────────────────────────────
// The FSM holds a governance ref for worker invocation and event dispatch.
// engagement-fsm pattern: set at boot, passed through execution contexts.
let _governance = null;

function setGovernance(governance) {
  if (governance && typeof governance.dispatch === 'function') {
    _governance = governance;
  }
}

function getGovernance() {
  return _governance;
}

// ── Worker registry (local) ───────────────────────────────────────────
// Each FSM holds its own worker map. CK registration happens at boot
// via constitutional.registerWorker(fsmName, workerName, worker).
// The CTX gate (ctx.invokeWorker) validates ownership through CK.
const _workers = new Map();

function registerWorker(name, worker) {
  _workers.set(name, worker);
}

function getWorker(name) {
  return _workers.get(name) || null;
}

function getWorkers() {
  return _workers;
}


// 0. Governance Policy Constants — domain-owned thresholds
// ═══════════════════════════════════════════════════════════════════════════════

const REPLAY_RATE_DEGRADATION_THRESHOLD = 0.5;   // >50% replay rate in a batch signals degradation
const ORPHAN_RATE_DEGRADATION_THRESHOLD = 0.3;    // >30% orphan rate in a batch signals degradation
const MAX_REPLAY_RESOURCES = 10000;                // cap replay tracking Map to prevent unbounded growth
const BATCH_STALENESS_MS = 300_000;               // 5min — batch stuck in ACTIVE signals unhealthy

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
  // Gated by ctx.sanityCheck. When the system is overloaded
  // (DEGRADED, write backpressure), the gate can veto opening
  // a new batch.
  DEDUP_BATCH_BEGIN: {
    target: 'ACTIVE',
    guard: (event) => {
      if (_localState !== 'IDLE') {
        return { allowed: false, reason: `Cannot begin batch from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: async (event, ctx) => {
      const gate = await _resolveSanityCheck(ctx, {
        operation: 'dedup_batch_begin',
        domain: 'dedup',
        accountId: event.accountId,
      });
      if (!gate.allowed) return [];
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

  // ── DEDUP_INTENT_MARKED  — DEPRECATED (2026-06-07) ────────────────────
  // ── DEDUP_REPLAY_DETECTED — DEPRECATED (2026-06-07) ────────────────────
  // Both replaced by CHECK_AND_MARK_DEDUP which atomically checks and marks
  // in one handler. These event types remain in CK's DOMAIN_EVENT_MAP for
  // backward compatibility but have no handler — dispatch returns "unknown
  // event type" which is harmless since nothing emits them anymore.
  //
  // ── Evaluation complete → close dedup batch, evaluate governance signals ─
  DEDUP_BATCH_END: {
    target: 'IDLE',
    guard: (event) => {
      if (_localState !== 'ACTIVE') {
        return { allowed: false, reason: `Cannot end batch from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: async (event, ctx) => {
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

      // ── Clear tick via bounded worker (CK gate) ──────────────────────
      // Fallback to substrate for non-CK contexts (tests).
      // Emit WORKER_OUTCOME_REPORTED on failure so engagement-fsm
      // can schedule a retry to restore substrate health.
      try {
        if (ctx && typeof ctx.invokeWorker === 'function') {
          await ctx.invokeWorker('clear-tick', {});
        } else {
          _substrate().clearTick();
        }
      } catch (err) {
        console.warn(`[dedup-fsm] clearTick failed: ${err.message}`);
        if (_governance) {
          _governance.dispatch({
            type: 'WORKER_OUTCOME_REPORTED',
            accountId: event.accountId,
            intentId: null,
            domain: 'dedup:redis',
            status: 'failed',
            result: null,
            error: err.message,
            errorShape: {
              category: 'transient',
              code: err.code || null,
              retryable: true,
              retryAfterSeconds: null,
            },
            params: { operation: 'clear-tick' },
          });
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

  // ── Check + Mark Dedup (replaces isInFlight + markInFlight) ──────────────
  // This is the canonical dedup entry point. External callers dispatch
  // CHECK_AND_MARK_DEDUP to CK → CK routes to dedup FSM → FSM calls
  // workers via ctx.invokeWorker → workers delegate to substrate.
  //
  // ONE CK round-trip, atomic check+mark inside FSM handler.
  // No race window between check and mark — both happen in the same handler.
  // CK gate validates ownership, contract, sanity on every worker invocation.
  // FSM gate (sanityCheck) validates system health before any dedup I/O.
  CHECK_AND_MARK_DEDUP: {
    target: (event) => _localState, // stay in current state
    guard: (event) => {
      return { allowed: true };
    },
    buildActions: async (event, ctx) => {
      const { accountId, actionType, resourceId, intentId } = event;

      // ── 0. Sanity gate — block dedup I/O when system is DEGRADED ─────
      const gate = await _resolveSanityCheck(ctx, {
        operation: 'check_and_mark_dedup',
        domain: 'dedup',
        accountId,
        actionType,
        resourceId,
      });
      if (!gate.allowed) {
        return [{
          type: 'DEDUP_INTENT_CHECKED',
          accountId, actionType, resourceId, intentId,
          blocked: false,
          reason: null,
          existingIntentId: null,
          isReplay: false,
          gateRejected: true,
          gateReason: gate.reason,
        }];
      }

      // ── 1. Check dedup via bounded worker (CK gate) ──────────────────
      // Fail-open: if worker throws, treat as non-blocked and proceed.
      // The CK gate already validated ownership/contract/sanity on invoke.
      // A throw here means the worker itself failed — not a dedup block.
      // Emit WORKER_OUTCOME_REPORTED so engagement-fsm can schedule a
      // retry to restore substrate health. The intent proceeds unblocked.
      let checkResult;
      try {
        if (ctx && typeof ctx.invokeWorker === 'function') {
          checkResult = await ctx.invokeWorker('check-dedup', {
            accountId, actionType, resourceId, intentId,
          });
        } else {
          checkResult = await _substrate().isInFlight(
            accountId, actionType, resourceId, intentId
          );
        }
      } catch (err) {
        console.warn(`[dedup-fsm] check-dedup worker failed: ${err.message}, failing open`);
        if (_governance) {
          _governance.dispatch({
            type: 'WORKER_OUTCOME_REPORTED',
            accountId, intentId,
            domain: 'dedup:redis',
            status: 'failed',
            result: null,
            error: err.message,
            errorShape: {
              category: 'transient',
              code: err.code || null,
              retryable: true,
              retryAfterSeconds: null,
            },
            params: { operation: 'check-dedup', actionType, resourceId },
          });
        }
        checkResult = { blocked: false, reason: null, existingIntentId: null };
      }

      // ── 2. If exact duplicate → blocked, no mark ──────────────────
      if (checkResult.blocked && checkResult.reason === 'duplicate') {
        _batchMarks++;
        return [{
          type: 'DEDUP_INTENT_CHECKED',
          accountId, actionType, resourceId, intentId,
          blocked: true,
          reason: 'duplicate',
          existingIntentId: checkResult.existingIntentId,
          isReplay: false,
        }];
      }

      // ── 3. Mark in-flight via bounded worker (CK gate) ────────────
      // Fail-open: mark failure does not block the intent.
      // Emit WORKER_OUTCOME_REPORTED so engagement-fsm can schedule a
      // retry to restore substrate health.
      try {
        if (ctx && typeof ctx.invokeWorker === 'function') {
          await ctx.invokeWorker('mark-in-flight', {
            accountId, actionType, resourceId, intentId,
          });
        } else {
          await _substrate().markInFlight(accountId, actionType, resourceId, { intentId });
        }
      } catch (err) {
        console.warn(`[dedup-fsm] mark-in-flight worker failed: ${err.message}, failing open`);
        if (_governance) {
          _governance.dispatch({
            type: 'WORKER_OUTCOME_REPORTED',
            accountId, intentId,
            domain: 'dedup:redis',
            status: 'failed',
            result: null,
            error: err.message,
            errorShape: {
              category: 'transient',
              code: err.code || null,
              retryable: true,
              retryAfterSeconds: null,
            },
            params: { operation: 'mark-in-flight', actionType, resourceId },
          });
        }
      }

      // ── 4. Track batch metrics ────────────────────────────────────
      _batchMarks++;
      if (checkResult.reason === 'replay') {
        _batchReplays++;
        // Cap replay resource tracking to prevent unbounded Map growth
        if (_replayResources.size < MAX_REPLAY_RESOURCES) {
          if (!_replayResources.has(resourceId)) {
            _replayResources.set(resourceId, []);
          }
          _replayResources.get(resourceId).push({
            intentId,
            previousIntentId: checkResult.existingIntentId,
            ts: Date.now(),
          });
        }
      }

      // ── 5. Return result ──────────────────────────────────────────
      return [{
        type: 'DEDUP_INTENT_CHECKED',
        accountId, actionType, resourceId, intentId,
        blocked: false,
        reason: checkResult.reason,
        existingIntentId: checkResult.existingIntentId,
        isReplay: checkResult.reason === 'replay',
      }];
    },
  },

  // ── CHECK_MUTATION_DEDUP (Phase 8) ─────────────────────────────────────────
  // Mutation-layer dedup gate: checks + marks Layer 2 (mutation namespace).
  // Belt-and-suspenders: also checks Layer 1 (intake) key — if the same intent
  // was already allowed at intake but evaluation ran twice, the mutation is
  // blocked as a replay.
  //
  // External callers dispatch CHECK_MUTATION_DEDUP to CK → CK routes to
  // dedup FSM → FSM calls mutation-dedup workers → substrate.
  // On duplicate: emits DEDUP_MUTATION_BLOCKED observability event.
  // The emission-orchestrator catches the blocked result and skips the mutation.
  CHECK_MUTATION_DEDUP: {
    target: (event) => _localState, // stay in current state
    guard: (event) => {
      return { allowed: true };
    },
    buildActions: async (event, ctx) => {
      const { accountId, actionType, resourceId, intentId } = event;

      // ── 1. Check via bounded worker (CK gate) ──────────────────────
      let checkResult;
      try {
        if (ctx && typeof ctx.invokeWorker === 'function') {
          checkResult = await ctx.invokeWorker('check-mutation-dedup', {
            accountId, actionType, resourceId, intentId,
          });
        } else {
          checkResult = await _mutationSubstrate().isInFlightMutation(
            accountId, actionType, resourceId, intentId
          );
        }
      } catch (err) {
        console.warn(`[dedup-fsm] check-mutation-dedup failed: ${err.message}, failing open`);
        if (_governance) {
          _governance.dispatch({
            type: 'WORKER_OUTCOME_REPORTED',
            accountId, intentId,
            domain: 'dedup:mutation',
            status: 'failed',
            result: null,
            error: err.message,
            errorShape: { category: 'transient', code: err.code || null, retryable: true, retryAfterSeconds: null },
            params: { operation: 'check-mutation-dedup', actionType, resourceId },
          });
        }
        checkResult = { blocked: false, reason: null, existingIntentId: null };
      }

      // ── 2. Duplicate → blocked ─────────────────────────────────────
      if (checkResult.blocked) {
        return [{
          type: 'DEDUP_MUTATION_BLOCKED',
          accountId, actionType, resourceId, intentId,
          reason: checkResult.reason,
          existingIntentId: checkResult.existingIntentId,
        }];
      }

      // ── 3. Mark in-flight via bounded worker (CK gate) ─────────────
      try {
        if (ctx && typeof ctx.invokeWorker === 'function') {
          await ctx.invokeWorker('mark-mutation-in-flight', {
            accountId, actionType, resourceId, intentId,
          });
        } else {
          await _mutationSubstrate().markInFlightMutation(accountId, actionType, resourceId, { intentId });
        }
      } catch (err) {
        console.warn(`[dedup-fsm] mark-mutation-in-flight failed: ${err.message}, failing open`);
        if (_governance) {
          _governance.dispatch({
            type: 'WORKER_OUTCOME_REPORTED',
            accountId, intentId,
            domain: 'dedup:mutation',
            status: 'failed',
            result: null,
            error: err.message,
            errorShape: { category: 'transient', code: err.code || null, retryable: true, retryAfterSeconds: null },
            params: { operation: 'mark-mutation-in-flight', actionType, resourceId },
          });
        }
      }

      return [{
        type: 'DEDUP_MUTATION_CHECKED',
        accountId, actionType, resourceId, intentId,
        blocked: false,
        reason: null,
      }];
    },
  },

  // ── CHECK_EMISSION_DEDUP (Phase 8) ─────────────────────────────────────────
  // Emission-layer dedup gate: checks + marks Layer 3 (emission namespace).
  // Belt-and-suspenders: also checks Layer 1 (intake) key.
  //
  // Prevents the same intent from triggering the IG API twice (e.g., if the
  // retry worker re-executes after a transient failure that actually succeeded).
  //
  // On duplicate: emits DEDUP_EMISSION_BLOCKED observability event.
  // The content/engagement substrates catch the blocked result and skip the call.
  CHECK_EMISSION_DEDUP: {
    target: (event) => _localState, // stay in current state
    guard: (event) => {
      return { allowed: true };
    },
    buildActions: async (event, ctx) => {
      const { accountId, actionType, resourceId, intentId } = event;

      // ── 1. Check via bounded worker (CK gate) ──────────────────────
      let checkResult;
      try {
        if (ctx && typeof ctx.invokeWorker === 'function') {
          checkResult = await ctx.invokeWorker('check-emission-dedup', {
            accountId, actionType, resourceId, intentId,
          });
        } else {
          checkResult = await _mutationSubstrate().isInFlightEmission(
            accountId, actionType, resourceId, intentId
          );
        }
      } catch (err) {
        console.warn(`[dedup-fsm] check-emission-dedup failed: ${err.message}, failing open`);
        if (_governance) {
          _governance.dispatch({
            type: 'WORKER_OUTCOME_REPORTED',
            accountId, intentId,
            domain: 'dedup:emission',
            status: 'failed',
            result: null,
            error: err.message,
            errorShape: { category: 'transient', code: err.code || null, retryable: true, retryAfterSeconds: null },
            params: { operation: 'check-emission-dedup', actionType, resourceId },
          });
        }
        checkResult = { blocked: false, reason: null, existingIntentId: null };
      }

      // ── 2. Duplicate → blocked ─────────────────────────────────────
      if (checkResult.blocked) {
        return [{
          type: 'DEDUP_EMISSION_BLOCKED',
          accountId, actionType, resourceId, intentId,
          reason: checkResult.reason,
          existingIntentId: checkResult.existingIntentId,
        }];
      }

      // ── 3. Mark in-flight via bounded worker (CK gate) ─────────────
      try {
        if (ctx && typeof ctx.invokeWorker === 'function') {
          await ctx.invokeWorker('mark-emission-in-flight', {
            accountId, actionType, resourceId, intentId,
          });
        } else {
          await _mutationSubstrate().markInFlightEmission(accountId, actionType, resourceId, { intentId });
        }
      } catch (err) {
        console.warn(`[dedup-fsm] mark-emission-in-flight failed: ${err.message}, failing open`);
        if (_governance) {
          _governance.dispatch({
            type: 'WORKER_OUTCOME_REPORTED',
            accountId, intentId,
            domain: 'dedup:emission',
            status: 'failed',
            result: null,
            error: err.message,
            errorShape: { category: 'transient', code: err.code || null, retryable: true, retryAfterSeconds: null },
            params: { operation: 'mark-emission-in-flight', actionType, resourceId },
          });
        }
      }

      return [{
        type: 'DEDUP_EMISSION_CHECKED',
        accountId, actionType, resourceId, intentId,
        blocked: false,
        reason: null,
      }];
    },
  },

  // ── Conversation repair: route to repair substrate (Phase 5) ────────────
  // Gated by ctx.sanityCheck. When the system is DEGRADED, the
  // gate can block repair operations.
  REPAIR_CONVERSATION: {
    target: (event) => _localState, // stay in current state
    guard: (event) => {
      // Always allow — repair requests are always valid
      return { allowed: true };
    },
    buildActions: async (event, ctx) => {
      const gate = await _resolveSanityCheck(ctx, {
        operation: 'repair_conversation',
        domain: 'dedup',
        accountId: event.accountId,
        threadId: event.threadId,
      });
      if (!gate.allowed) return [];
      // Track orphan — a missing conversation was detected
      _batchOrphans++;
      return [{
        type: 'EXECUTE_CONVERSATION_REPAIR',
        threadId: event.threadId,
        accountId: event.accountId,
        igUserId: event.igUserId,
        pageToken: event.pageToken,
        pageId: event.pageId,
      }];
    },
  },

  // ── Repair outcome — FSM processes worker result ──────────────────────
  REPAIR_CONVERSATION_COMPLETE: {
    target: (event) => _localState, // stay in current state
    guard: (event) => {
      return { allowed: true };
    },
    buildActions: (event) => {
      const { threadId, accountId, uuid, recovered } = event;
      if (recovered > 0) {
        _batchOrphans--; // repaired conversation reduces orphan count
      }
      return [{
        type: 'REPAIR_CONVERSATION_RESOLVED',
        threadId,
        accountId,
        uuid,
        recovered,
        timestamp: Date.now(),
      }];
    },
  },

  // ── DEDUP_RETRY_IN_PROGRESS: engagement-fsm scheduled a dedup retry ─
  // The dedup FSM stays in its current state while the retry chain
  // is in flight. Dedup retries are transparent substrate health
  // restores — the FSM state does not gate retry operations.
  // Handler is a pure state hold for observability fidelity.
  // Emitted by engagement-fsm._scheduleRetry when domain='dedup'.
  DEDUP_RETRY_IN_PROGRESS: {
    target: (event) => _localState, // no state change
    guard: (event) => {
      return { allowed: true };
    },
    buildActions: (event) => {
      return []; // pure state hold — engagement-fsm owns retry invocation
    },
  },

  // ── DEDUP_RETRY_EXHAUSTED: terminal retry failure for dedup ops ──
  // Emitted by engagement-fsm._buildExhaustedActions when the retry
  // chain exhausts for a dedup operation. Dedup transitions ACTIVE→IDLE
  // (if in ACTIVE) or stays IDLE. Logs degraded for observability.
  DEDUP_RETRY_EXHAUSTED: {
    target: (event) => _localState === 'ACTIVE' ? 'IDLE' : 'IDLE',
    guard: (event) => {
      return { allowed: true };
    },
    buildActions: (event) => {
      _degradationCount++;
      return [{
        type: 'LOG_DEGRADED',
        substate: 'DEDUP_RETRY_EXHAUSTED',
        reason: event.error || 'Dedup retry chain exhausted',
        operation: event.operation,
        retryCount: event.retryCount,
      }];
    },
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 3. Domain-local runtime state (private)
// ═══════════════════════════════════════════════════════════════════════════════

let _localState = 'IDLE';
let _lastTransitionedAt = null; // last state change timestamp for temporal alignment in reconciliation

// ── Batch-level counters (reset on DEDUP_BATCH_BEGIN) ────────────────────────
let _batchMarks = 0;
let _batchReplays = 0;
let _batchOrphans = 0;

// ── Persistent tracking across batches ────────────────────────────────────────
let _degradationCount = 0;             // cumulative degradation signals emitted
const _replayResources = new Map();    // resourceId → [{ intentId, previousIntentId, ts }]

// ── Default fail-open sanity check (universal gate pattern) ─────────────
// Same pattern as engagement-fsm. The ctx.sanityCheck is the
// universal gate; the FSM calls it during emission. For tests /
// non-CK dispatch, the default is always-allowed.
const _defaultSanityCheck = async () => ({ allowed: true });

function _resolveSanityCheck(ctx, action) {
  if (ctx && typeof ctx.sanityCheck === 'function') {
    return ctx.sanityCheck(action);
  }
  return _defaultSanityCheck(action);
}

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
async function dispatch(event, ctx) {
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
  _lastTransitionedAt = Date.now();

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
  const actions = (txn.buildActions ? await txn.buildActions(event, ctx) : []);

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
  const stalenessMs = _localState === 'ACTIVE' && _lastTransitionedAt
    ? Date.now() - _lastTransitionedAt
    : 0;
  const stale = stalenessMs > BATCH_STALENESS_MS;

  return {
    ok: !stale && _degradationCount === 0
      && (_batchMarks === 0 || _batchReplays < _batchMarks * 0.5),
    signals: {
      state: _localState,
      activeBatch: _localState === 'ACTIVE',
      batchAgeMs: stalenessMs,
      batchStale: stale,
      degradationCount: _degradationCount,
      currentReplayRate: _batchMarks > 0 ? _batchReplays / _batchMarks : 0,
      currentOrphanCount: _batchOrphans,
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

function getLastTransitionedAt() {
  return _lastTransitionedAt;
}

module.exports = {
  setGovernance,
  getGovernance,
  registerWorker,
  getWorker,
  getWorkers,
  name: 'dedup',
  dispatch,
  init,
  getState,
  exportState,
  getHealth,
  getBatchState,
  getReplayResources,
  getDegradationCount,
  getLastTransitionedAt,
};
