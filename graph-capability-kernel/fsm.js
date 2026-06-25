// graph-capability-kernel/fsm.js
// Graph Capability Domain FSM: federated, state-inferential, multi-credential
// capability state machine.
// Migrated from control-plane/governance/domains/graph-capability-fsm.js
//
// Owns: capability lifecycle state for every credential in the system.
//       Per-credential evidence persistence. Capability verdict publication.
// Does NOT own: Graph API calls, token inspection, scope evaluation,
//               worker dispatch, substrate execution, vault lifecycle.
//
// Architectural invariant:
//   Signals UP   → ctx.dispatchGlobal(event) recommends constitutional state changes
//   Authority ↓  → ctx.validate(from, to, event) asks constitutional for approval
//   Workers ↓    → substrate workers perform mechanical observation only
//
//   The FSM is the SOLE INTERPRETER of observation evidence. The FSM is
//   the SOLE PUBLISHER of capability verdicts. The verdict-gate is a pure
//   read adapter that asks the FSM.
//
// Domain FSMs emit state transitions through the observability plane.
// Transition writers consume from the observability plane and write to the
// canonical lineage ledger via lineageLedger.recordWorkerEntry().
// FSMs do NOT write to the lineage ledger directly.
//
// ── Post-strengthening model (2026-06-07) ──────────────────────────────────
// The FSM reads the SHAPE of the observation envelope (which slots are
// present, which are missing) and infers the credential's capability
// lifecycle phase. State names ARE the inference.
//
// The FSM is multi-credential. State, evidence, observation timestamp,
// consecutive-failure count are kept PER CREDENTIAL. Cross-credential
// state is not maintained — the FSM is a federation of per-cred state
// machines under a single dispatch membrane.

const { REQUIRED_SCOPES, OBSERVATION_FRESHNESS_MS, DEGRADED_OBSERVATION_MS,
        STATE_REGISTRY, _inference, _infer, _PENDING_FOR, _mergeEnvelope } =
  (() => {
    // ── I. State Registry + Inferential Sub-Layer (deferred declaration;
    //    the assignment above re-orders so REQUIRED_SCOPES + the infer
    //    helper are defined before the public surface is used).
    //
    // We can't actually do circular const initialization in plain JS, so
    // the registry + infer helper live in the module body below. This IIFE
    // is a placeholder to make the public surface above the line that
    // consumers see clear.
    return {
      REQUIRED_SCOPES: [],
      OBSERVATION_FRESHNESS_MS: 0,
      DEGRADED_OBSERVATION_MS: 0,
      STATE_REGISTRY: {},
      _inference: null,
      _infer: null,
      _PENDING_FOR: null,
      _mergeEnvelope: null,
    };
  })();

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
// 0. Governance Policy Constants
// ═══════════════════════════════════════════════════════════════════════════════

const OBSERVATION_FRESHNESS_MS_VALUE = 30 * 60 * 1000; // 30 min
const DEGRADED_OBSERVATION_MS_VALUE  = 2 * 60 * 60 * 1000; // 2h
// ── Cadence policy — owned by FSM (Phase A) ────────────────────────────────
// These windows govern WHEN the FSM emits RUN_TOKEN_HEALTH_CHECK and
// RUN_UAT_REFRESH_CHECK actions per credential. Workers MUST NOT redefine
// these values; the FSM is the sole authority on cadence policy.
const TOKEN_HEALTH_WINDOW_MS          = 24 * 60 * 60 * 1000;  // 24h
const UAT_REFRESH_WINDOW_MS           = 14 * 24 * 60 * 60 * 1000;  // 14d
const DATA_ACCESS_EXPIRY_WINDOW_MS    = 30 * 24 * 60 * 60 * 1000;  // 30d
const REQUIRED_SCOPES_VALUE = [
  'instagram_basic',
  'instagram_manage_comments',
  'instagram_manage_insights',
  'instagram_content_publish',
  'pages_show_list',
  'pages_read_engagement',
];

// ═══════════════════════════════════════════════════════════════════════════════
// 1. State Registry — 9 states (post-strengthening)
// ═══════════════════════════════════════════════════════════════════════════════

const STATE_REGISTRY_VALUE = {
  // Terminal complete states
  AUTHORIZED: {
    description: 'All slots observed, all required scopes present, full capability',
    category: 'COMPLETE',
    verdictDefault: 'ALLOWED',
  },
  LIMITED: {
    description: 'All slots observed, granted scope set missing required scopes',
    category: 'COMPLETE',
    verdictDefault: 'CONDITIONAL',
  },
  UNAUTHORIZED: {
    description: 'Required worker reports failure (pat/uat undecryptable, or detect.isValid=false)',
    category: 'COMPLETE',
    verdictDefault: 'DENIED',
  },
  DEGRADED: {
    description: 'All slots observed, reliability impaired OR scope cache stale',
    category: 'COMPLETE',
    verdictDefault: 'CONDITIONAL',
  },
  UNKNOWN: {
    description: 'No envelope observed for this credential yet',
    category: 'EMPTY',
    verdictDefault: 'DENIED',
  },
  // PENDING states — name literally says which slot is missing
  PAT_PENDING: {
    description: 'Envelope observed, awaiting pat slot',
    category: 'PENDING',
    verdictDefault: 'DENIED',
    missingSlot: 'pat',
  },
  UAT_PENDING: {
    description: 'Envelope observed, awaiting uat slot',
    category: 'PENDING',
    verdictDefault: 'DENIED',
    missingSlot: 'uat',
  },
  DETECTION_PENDING: {
    description: 'Envelope observed, awaiting detection slot',
    category: 'PENDING',
    verdictDefault: 'DENIED',
    missingSlot: 'detection',
  },
  SCOPE_PENDING: {
    description: 'Envelope observed, awaiting scope slot',
    category: 'PENDING',
    verdictDefault: 'DENIED',
    missingSlot: 'scope',
  },
};

const PENDING_FOR = {
  pat: 'PAT_PENDING',
  uat: 'UAT_PENDING',
  detection: 'DETECTION_PENDING',
  scope: 'SCOPE_PENDING',
};

const OBSERVATION_SLOTS = ['pat', 'uat', 'detection', 'scope'];

// ═══════════════════════════════════════════════════════════════════════════════
// 2. Inferential Layer — the heart of the strengthened FSM
// ═══════════════════════════════════════════════════════════════════════════════
//
// Infer state from envelope SHAPE. Returns the state name, missing scopes
// (for LIMITED), and a reason string. The FSM is the sole interpreter.

/**
 * Merge a new envelope into an existing one. Slots are per-slot: if a
 * slot is null in the new envelope, the old value is retained. Identity
 * fields (envelopeId, observedAt) are overwritten.
 */
function mergeEnvelope(existing, fresh) {
  if (!existing) {
    existing = {
      envelopeId: null,
      observedAt: null,
      businessAccountId: null,
      userId: null,
      pat: null,
      uat: null,
      detection: null,
      scope: null,
    };
  }
  if (!fresh) return existing;
  // Slot-merge rule: a slot in `fresh` that is non-null overrides
  // the existing slot. A slot in `fresh` that is null/undefined
  // RETAIN the existing slot (so partial envelopes merge into the
  // accumulated evidence rather than overwriting it with empty).
  const pick = (slot) => {
    if (fresh[slot] === null || fresh[slot] === undefined) return existing[slot];
    return fresh[slot];
  };
  return {
    envelopeId: fresh.envelopeId || existing.envelopeId,
    observedAt: fresh.observedAt || existing.observedAt,
    businessAccountId: fresh.businessAccountId || existing.businessAccountId,
    userId: fresh.userId || existing.userId,
    pat: pick('pat'),
    uat: pick('uat'),
    detection: pick('detection'),
    scope: pick('scope'),
  };
}

/**
 * Find the first missing observation slot, or null if all 4 are populated.
 * Order: pat, uat, detection, scope.
 */
function _firstMissingSlot(envelope) {
  if (!envelope) return 'pat';
  for (const slot of OBSERVATION_SLOTS) {
    if (envelope[slot] === null || envelope[slot] === undefined) return slot;
  }
  return null;
}

/**
 * Infer FSM state from an envelope.
 * Returns:
 *   { state, reason, missingScopes, observationFresh }
 */
function inferStateFromEnvelope(envelope, now) {
  now = now || Date.now();
  const observedAt = (envelope && envelope.observedAt) || null;
  const evidence = envelope || { pat: null, uat: null, detection: null, scope: null };

  // 1. UNAUTHORIZED: any required worker reports failure
  if (evidence.pat && evidence.pat.isDecryptable === false) {
    return { state: 'UNAUTHORIZED', reason: 'PAT not decryptable', missingScopes: [] };
  }
  if (evidence.uat && evidence.uat.isDecryptable === false) {
    return { state: 'UNAUTHORIZED', reason: 'UAT not decryptable', missingScopes: [] };
  }
  if (evidence.detection && evidence.detection.isValid === false) {
    return { state: 'UNAUTHORIZED', reason: evidence.detection.reason || 'Token validation failed', missingScopes: [] };
  }

  // 2. Empty envelope → check for PENDING slot (envelope must exist with at least one slot)
  const missingSlot = _firstMissingSlot(evidence);
  if (missingSlot !== null) {
    // If envelope is null or all slots are null, this is UNKNOWN (EMPTY)
    if (!envelope) return { state: 'UNKNOWN', reason: 'No envelope observed', missingScopes: [] };
    // Otherwise, envelope exists with at least one slot populated, but at least one is missing
    // Determine if it's UNKNOWN (all slots null) or PENDING (some populated, some missing)
    const anyPopulated = OBSERVATION_SLOTS.some(s => evidence[s] !== null && evidence[s] !== undefined);
    if (!anyPopulated) {
      return { state: 'UNKNOWN', reason: 'No envelope slots observed', missingScopes: [] };
    }
    return {
      state: PENDING_FOR[missingSlot],
      reason: `Awaiting ${missingSlot} observation slot`,
      missingScopes: [],
    };
  }

  // 3. All 4 slots populated. Run the complete-state inference.
  const grantedScopes = (evidence.scope && evidence.scope.grantedScopes) || [];
  const missingScopes = REQUIRED_SCOPES_VALUE.filter(s => !grantedScopes.includes(s));

  // DEGRADED: reliabilityImpaired OR scope cache stale
  if (evidence.detection && evidence.detection.reliabilityImpaired) {
    return { state: 'DEGRADED', reason: 'Detection reliability impaired', missingScopes: [] };
  }
  if (evidence.scope && evidence.scope.cacheAgeMs != null) {
    const STALE_THRESHOLD_MS = 2 * 6 * 60 * 60 * 1000;
    if (evidence.scope.cacheAgeMs > STALE_THRESHOLD_MS) {
      return { state: 'DEGRADED', reason: `Scope cache stale: ${evidence.scope.cacheAgeMs}ms`, missingScopes: [] };
    }
  }

  // LIMITED: missing required scopes
  if (missingScopes.length > 0) {
    return { state: 'LIMITED', reason: `Missing required scopes: ${missingScopes.join(', ')}`, missingScopes };
  }

  // AUTHORIZED: all slots present, all scopes present, no degradation
  return { state: 'AUTHORIZED', reason: null, missingScopes: [] };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 3. Domain Transition Map
// ═══════════════════════════════════════════════════════════════════════════════
//
// Transitions reason about envelope SHAPE (per-cred), not raw state names.
//
// Per-cred helpers used by guards:
//   _credRecord(baId) → { state, evidence, lastObservedAt, consecutiveFailures, lastTransitionedAt }
//
// Events:
//   CAPABILITY_EVALUATE   → start evaluation (UNKNOWN or first PENDING)
//   CAPABILITY_OBSERVATION → new envelope arrived, merge + infer (any state)
//   CAPABILITY_OK         → inferred AUTHORIZED (reaches when partials complete)
//   CAPABILITY_PARTIAL    → inferred LIMITED (full envelope, missing scopes)
//   CAPABILITY_FAILED     → inferred UNAUTHORIZED (worker failure surfaced)
//   CAPABILITY_DEGRADED   → inferred DEGRADED (reliability / staleness)
//   CAPABILITY_RECOVERED  → explicit recovery from UNAUTHORIZED/DEGRADED
//   CAPABILITY_REEVALUATE → cadence tick (UNKNOWN or PENDING → start fresh eval)

const _byCred = new Map(); // businessAccountId → credRecord

// ── Immediate token refresh tracking (Phase 8) ───────────────────────────
// When CK dispatches IMMEDIATE_TOKEN_REFRESH, the FSM records the
// businessAccountId here. When CAPABILITY_HEALTH_CHECK_COMPLETED fires
// for token_health, we check this map and emit TOKEN_REFRESH_RESULT.
const _immediateTokenRefreshes = new Map(); // businessAccountId → true

function _credRecord(baId) {
  if (!baId) return null;
  if (!_byCred.has(baId)) {
    _byCred.set(baId, {
      state: 'UNKNOWN',
      evidence: null,
      lastObservedAt: null,
      lastTransitionedAt: null,
      consecutiveFailures: 0,
      pendingReads: new Map(),  // readId → { readDomain, params, source, requestedAt, resolve, reject, timeout }
      pendingWrites: new Map(), // writeId → { table, operation, accountId, requestedAt, resolve, reject, timeout }
      // ── Cadence timestamps (Phase A) — owned by FSM ────────────────────────
      // Workers MUST NOT maintain their own "last checked" timestamps.
      // The FSM is the sole authority on per-cred cadence. These are updated
      // by the CAPABILITY_HEALTH_CHECK_COMPLETED transition.
      lastTokenHealthCheckAt: null,
      lastUatRefreshCheckAt: null,
      lastDataAccessExpiryCheckAt: null,
    });
  }
  return _byCred.get(baId);
}

function _resolveCred(event) {
  const baId = event.businessAccountId || (event.envelope && event.envelope.businessAccountId);
  if (!baId) return null;
  return _credRecord(baId);
}

const TRANSITION_MAP = {
  // ── Entry: any external trigger starts evaluation ─────────────────────────
  CAPABILITY_EVALUATE: {
    target: 'UNKNOWN',
    guard: (event) => ({ allowed: true }),
    buildActions: (event) => [{
      type: 'CAPABILITY_EVALUATION_STARTED',
      source: event.source || 'unknown',
      businessAccountId: event.businessAccountId || null,
    }],
  },

  // ── Bootstrap: server boot → CK → FSM → health membrane ──────────────────
  // Dispatched by CK.bootstrap() once after wiring is live. The FSM decides
  // WHAT health work to run; CK routes the actions to the health membrane.
  // The membrane (health-substrate) executes mechanically, never decides.
  //
  // Phase A: if the event carries a businessAccountId, the FSM gates the
  // emitted action on the per-cred cadence window (TOKEN_HEALTH_WINDOW_MS,
  // UAT_REFRESH_WINDOW_MS). If the event is unscoped (the legacy CK bootstrap
  // shape), the FSM emits the run actions for all creds that are due. This
  // preserves the existing wire() handler contract while introducing the
  // gating machinery that PR 4 (cadence tick source) will exercise.
  CAPABILITY_BOOTSTRAP: {
    target: 'UNKNOWN',
    guard: () => ({ allowed: true }),
    buildActions: (event) => {
      const now = Date.now();
      const targetBaId = event && event.businessAccountId;
      const actions = [];

      // Wire registered membranes on the first unscoped bootstrap. Idempotent.
      // The CK never calls a substrate directly — the FSM is the only
      // constitutional path from policy to execution. _wireMembranes is a
      // no-op if no membranes are registered (e.g. in unit tests where the
      // substrate is not required).
      if (!targetBaId && _governance) {
        _wireMembranes(_governance);
      }

      // Targeted bootstrap: gate on per-cred cadence
      if (targetBaId) {
        if (_shouldCheck(targetBaId, 'token_health', now)) {
          actions.push({ type: 'RUN_TOKEN_HEALTH_CHECK', businessAccountId: targetBaId, source: 'fsm.bootstrap' });
        }
        if (_shouldCheck(targetBaId, 'uat_refresh', now)) {
          actions.push({ type: 'RUN_UAT_REFRESH_CHECK', businessAccountId: targetBaId, source: 'fsm.bootstrap' });
        }
        return actions;
      }

      // Unscoped bootstrap: iterate all known creds, gate per-cred
      const baIds = Array.from(_byCred.keys()).filter(k => k !== '__global__');
      for (const baId of baIds) {
        if (_shouldCheck(baId, 'token_health', now)) {
          actions.push({ type: 'RUN_TOKEN_HEALTH_CHECK', businessAccountId: baId, source: 'fsm.bootstrap' });
        }
        if (_shouldCheck(baId, 'uat_refresh', now)) {
          actions.push({ type: 'RUN_UAT_REFRESH_CHECK', businessAccountId: baId, source: 'fsm.bootstrap' });
        }
      }
      return actions;
    },
  },

  // ── API Rate Limit Check — GCFSM owns Instagram API rate-limit policy ────
  // Called by middleware rate-limiter via CK.dispatch(API_RATE_LIMIT_CHECK).
  // GCFSM uses governedRead to query api_usage through the postgres-telemetry
  // kernel, then returns the result as API_RATE_LIMIT_RESULT.
  API_RATE_LIMIT_CHECK: {
    target: null,
    guard: (event) => {
      if (!event.userId) {
        return { allowed: false, reason: 'API_RATE_LIMIT_CHECK requires userId' };
      }
      return { allowed: true };
    },
    buildActions: async (event, ctx) => {
      const { userId, limit = 200 } = event;
      const result = await _governance.governedRead('db.api-usage', {
        query: 'checkHourlyLimit',
        userId,
        limit,
      });
      const data = result.success ? result.data : { current: 0, limit, remaining: limit };
      return [{
        type: 'API_RATE_LIMIT_RESULT',
        userId,
        allowed: data.current < data.limit,
        current: data.current,
        limit: data.limit,
        remaining: data.remaining,
      }];
    },
  },

  // ── CAPABILITY_CHECK (2026-06-18): Publishing FSM requests capability
  //     analysis after a publication timeout. GC FSM invokes credential
  //     and quota workers through CK's ctx.invokeWorker gate, which
  //     validates ownership, contract, and system sanity before execution.
  //     WORKER_RESULT is emitted to the observability ledger.
  //
  //     Results are assembled inline from the worker return values and
  //     dispatched as CAPABILITY_CHECK_RESULT back to the publishing FSM
  //     (or CK-level CAPABILITY_AUTH_FAILURE / CAPABILITY_DEGRADED when
  //     the cred state warrants it).
  CAPABILITY_CHECK: {
    target: null,
    guard: (event) => {
      if (!event.businessAccountId) {
        return { allowed: false, reason: 'CAPABILITY_CHECK requires businessAccountId' };
      }
      return { allowed: true };
    },
    buildActions: async (event, ctx) => {
      const baId = event.businessAccountId;
      const cred = _credRecord(baId);

      try {
        // Invoke workers through CK gate — ownership, contract, sanity checks.
        // Both workers export { execute } and are registered to 'graph-capability'.
        const [credResult, quotaResult] = await Promise.all([
          ctx.invokeWorker('credential-capability', { businessAccountId: baId }),
          ctx.invokeWorker('quota-intelligence', { businessAccountId: baId }),
        ]);

        // FSM owns ledger emission for both worker results
        _emitWorkerResult('credential-capability', credResult, baId);
        _emitWorkerResult('quota-intelligence', quotaResult, baId);

        cred.lastObservedAt = Date.now();
        cred.consecutiveFailures = credResult.consecutiveFailures || 0;

        // Cross-kernel result path: notify publishing FSM via dispatchGlobal.
        if (ctx && ctx.dispatchGlobal && event.sourceDomain === 'publishing') {
          ctx.dispatchGlobal({
            type: 'CAPABILITY_CHECK_RESULT',
            sourceDomain: 'graph-capability',
            businessAccountId: baId,
            correlationId: event.correlationId || null,
            capabilityState: credResult.capabilityState || 'UNKNOWN',
            quotaState: quotaResult.quotaState || 'NONE',
            freshnessMs: credResult.freshnessMs || null,
            consecutiveFailures: cred.consecutiveFailures,
          });
        }
      } catch (err) {
        // Worker invocation failed through CK gate — record failure
        cred.consecutiveFailures = (cred.consecutiveFailures || 0) + 1;

        if (ctx && ctx.dispatchGlobal && event.sourceDomain === 'publishing') {
          ctx.dispatchGlobal({
            type: 'CAPABILITY_CHECK_RESULT',
            sourceDomain: 'graph-capability',
            businessAccountId: baId,
            correlationId: event.correlationId || null,
            capabilityState: 'UNAUTHORIZED',
            quotaState: 'NONE',
            freshnessMs: null,
            consecutiveFailures: cred.consecutiveFailures,
          });
        }
      }

      return [];
    },
  },

  // ── CAPABILITY_CHECK_COMPLETE — defensive (deprecated primary path) ────
  // Previously the orchestrator dispatched this event back through CK after
  // running the capability-check substrate. Now the FSM's CAPABILITY_CHECK
  // handler calls ctx.invokeWorker() directly and dispatches
  // CAPABILITY_CHECK_RESULT inline. This handler is kept for defensive
  // compatibility — if something external dispatches CAPABILITY_CHECK_COMPLETE
  // (e.g. a test harness), it still updates the cred record and routes the
  // result appropriately.
  CAPABILITY_CHECK_COMPLETE: {
    target: null,
    guard: (event) => {
      if (!event.businessAccountId) {
        return { allowed: false, reason: 'CAPABILITY_CHECK_COMPLETE requires businessAccountId' };
      }
      return { allowed: true };
    },
    buildActions: async (event, ctx) => {
      const baId = event.businessAccountId;
      let cred = _byCred.get(baId);
      if (!cred) {
        cred = {
          state: 'UNKNOWN',
          evidence: [],
          lastObservedAt: null,
          lastTransitionedAt: null,
          consecutiveFailures: 0,
        };
        _byCred.set(baId, cred);
      }
      cred.lastObservedAt = Date.now();

      // Cross-kernel result path: notify the publishing FSM via dispatchGlobal.
      // event.sourceDomain carries the original sourceDomain from the publishing
      // FSM's CAPABILITY_CHECK dispatch, so we know this is a cross-kernel call.
      if (ctx && ctx.dispatchGlobal && event.sourceDomain === 'publishing') {
        ctx.dispatchGlobal({
          type: 'CAPABILITY_CHECK_RESULT',
          sourceDomain: 'graph-capability',
          businessAccountId: baId,
          correlationId: event.correlationId || null,
          capabilityState: event.capabilityState || 'UNKNOWN',
          quotaState: event.quotaState || 'NONE',
          freshnessMs: event.freshnessMs || null,
          consecutiveFailures: event.consecutiveFailures || 0,
        });
      }

      return [];
    },
  },

  // ── CAPABILITY_CHECK_FAILED — defensive (deprecated primary path) ───────
  // Kept for defensive compatibility — see CAPABILITY_CHECK_COMPLETE.
  // The primary failure path is now in CAPABILITY_CHECK's try/catch.
  CAPABILITY_CHECK_FAILED: {
    target: null,
    guard: (event) => {
      if (!event.businessAccountId) {
        return { allowed: false, reason: 'CAPABILITY_CHECK_FAILED requires businessAccountId' };
      }
      return { allowed: true };
    },
    buildActions: async (event, ctx) => {
      const baId = event.businessAccountId;
      let cred = _byCred.get(baId);
      if (!cred) {
        cred = {
          state: 'UNKNOWN',
          evidence: [],
          lastObservedAt: null,
          lastTransitionedAt: null,
          consecutiveFailures: 0,
        };
        _byCred.set(baId, cred);
      }
      cred.consecutiveFailures = (cred.consecutiveFailures || 0) + 1;

      // Cross-kernel result path: notify the publishing FSM via dispatchGlobal.
      if (ctx && ctx.dispatchGlobal && event.sourceDomain === 'publishing') {
        ctx.dispatchGlobal({
          type: 'CAPABILITY_CHECK_RESULT',
          sourceDomain: 'graph-capability',
          businessAccountId: baId,
          correlationId: event.correlationId || null,
          capabilityState: 'UNAUTHORIZED',
          quotaState: event.quotaState || 'NONE',
          freshnessMs: null,
          consecutiveFailures: cred.consecutiveFailures,
        });
      }

      return [];
    },
  },

  // ── Aggregate worker observation arrives ────────────────────────────────
  // This is the SOLE event that mutates per-cred evidence. The aggregator
  // merges the new envelope into the existing per-cred record, then
  // infers state. The inferred state becomes the target.
  CAPABILITY_OBSERVATION: {
    target: null, // resolved dynamically in dispatch via per-cred inference
    guard: (event) => {
      if (!event || !event.envelope) {
        return { allowed: false, reason: 'CAPABILITY_OBSERVATION requires event.envelope' };
      }
      if (!event.envelope.businessAccountId) {
        return { allowed: false, reason: 'CAPABILITY_OBSERVATION requires envelope.businessAccountId' };
      }
      return { allowed: true };
    },
    buildActions: (event, ctx) => {
      // Evidence already merged in dispatch step 3; resolve cred again
      // to read the merged envelope.
      const baId = event.envelope.businessAccountId;
      const c = _byCred.get(baId);
      const inferred = c ? inferStateFromEnvelope(c.evidence) : { state: 'UNKNOWN', reason: 'no cred', missingScopes: [] };
      const derived = {
        type: _stateToEventType(inferred.state),
        envelope: event.envelope,
        reason: inferred.reason,
        missingScopes: inferred.missingScopes,
        businessAccountId: baId,
        userId: event.envelope.userId || null,
        observedAt: event.envelope.observedAt || Date.now(),
        evidence: event.envelope,
      };
      if (derived.type) {
        dispatch(derived, ctx);
      }
      return [{
        type: 'CAPABILITY_OBSERVATION_RECEIVED',
        envelopeId: event.envelope.envelopeId || null,
        businessAccountId: baId,
        inferredState: inferred.state,
        reason: inferred.reason,
        missingScopes: inferred.missingScopes,
      }];
    },
  },

  // ── Inferred AUTHORIZED (all 4 slots green, all required scopes present) ─
  CAPABILITY_OK: {
    target: 'AUTHORIZED',
    guard: (event) => {
      const cred = _resolveCred(event);
      if (!cred) return { allowed: false, reason: 'no cred record' };
      if (!_isObservationFresh(cred.lastObservedAt)) {
        return { allowed: false, reason: 'observation stale' };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'CAPABILITY_AUTHORIZED',
      evidence: event.evidence || null,
      observedAt: event.observedAt,
      businessAccountId: event.businessAccountId || null,
    }],
  },

  // ── Inferred LIMITED (all 4 slots present, missing required scopes) ─────
  CAPABILITY_PARTIAL: {
    target: 'LIMITED',
    guard: (event) => {
      const cred = _resolveCred(event);
      if (!cred) return { allowed: false, reason: 'no cred record' };
      if (!_isObservationFresh(cred.lastObservedAt)) {
        return { allowed: false, reason: 'observation stale' };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'CAPABILITY_LIMITED',
      missingScopes: event.missingScopes || [],
      evidence: event.evidence || null,
      businessAccountId: event.businessAccountId || null,
    }],
  },

  // ── Inferred DEGRADED (all 4 slots present, reliability / cache degraded) ─
  CAPABILITY_DEGRADED: {
    target: 'DEGRADED',
    guard: (event) => {
      const cred = _resolveCred(event);
      if (!cred) return { allowed: true };
      // Re-entry guard: if already DEGRADED, reject the transition.
      // This breaks the loop:
      //   CAPABILITY_DEGRADED → CAPABILITY_DEGRADATION_DETECTED
      //     → HSM filter dispatchGlobal(CAPABILITY_DEGRADED) → GC FSM → LOOP.
      // Without this guard, fixing Issue 2 (adding CAPABILITY_DEGRADED to CK
      // routing) would create infinite recursion because the CK-routed event
      // reaches this handler again, re-emits CAPABILITY_DEGRADATION_DETECTED,
      // and the HSM filter re-dispatches CAPABILITY_DEGRADED indefinitely.
      if (cred.state === 'DEGRADED') {
        return { allowed: false, reason: 'CAPABILITY_DEGRADED rejected: already DEGRADED' };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'CAPABILITY_DEGRADATION_DETECTED',
      reason: event.reason || 'reliability impaired',
      evidence: event.evidence || null,
      businessAccountId: event.businessAccountId || null,
    }],
  },

  // ── Inferred UNAUTHORIZED (worker failure surfaced) ──────────────────────
  CAPABILITY_FAILED: {
    target: 'UNAUTHORIZED',
    guard: (event) => ({ allowed: true }),
    buildActions: (event) => [{
      type: 'CAPABILITY_AUTH_FAILURE',
      reason: event.reason || 'required capability unavailable',
      evidence: event.evidence || null,
      businessAccountId: event.businessAccountId || null,
    }],
  },

  // ── Recovery: explicit transition from UNAUTHORIZED / DEGRADED ───────────
  CAPABILITY_RECOVERED: {
    target: 'AUTHORIZED',
    guard: (event) => {
      const cred = _resolveCred(event);
      if (!cred) return { allowed: false, reason: 'no cred record' };
      if (cred.state !== 'UNAUTHORIZED' && cred.state !== 'DEGRADED') {
        return { allowed: false, reason: `Cannot recover from ${cred.state}` };
      }
      if (!_isObservationFresh(cred.lastObservedAt)) {
        return { allowed: false, reason: 'observation stale' };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'CAPABILITY_RECOVERED',
      previousState: _resolveCred(event)?.state || null,
      evidence: event.evidence || null,
      businessAccountId: event.businessAccountId || null,
    }],
  },

  // ── Cadence tick: re-evaluate ────────────────────────────────────────────
  CAPABILITY_REEVALUATE: {
    target: 'UNKNOWN',
    guard: (event) => {
      const cred = _resolveCred(event);
      if (!cred) return { allowed: true };
      if (cred.state === 'UNKNOWN') {
        return { allowed: false, reason: 'Already in UNKNOWN' };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'CAPABILITY_REEVALUATING',
      previousState: _resolveCred(event)?.state || null,
      cadence: event.cadence || 'scheduled',
      businessAccountId: event.businessAccountId || null,
    }],
  },

  // ── Health check completion (Phase A) — health-substrate signals back ─────
  // The health-substrate façade dispatches this after a RUN_TOKEN_HEALTH_CHECK
  // or RUN_UAT_REFRESH_CHECK run completes. The FSM records the per-cred
  // cadence timestamp; future CAPABILITY_BOOTSTRAP / CAPABILITY_CADENCE_TICK
  // dispatches will gate on these. No state change, no actions emitted.
  CAPABILITY_HEALTH_CHECK_COMPLETED: {
    target: null,  // no state change
    guard: (event) => {
      if (!event || !event.checkType) {
        return { allowed: false, reason: 'CAPABILITY_HEALTH_CHECK_COMPLETED requires event.checkType' };
      }
      if (!['token_health', 'uat_refresh', 'data_access_expiry'].includes(event.checkType)) {
        return { allowed: false, reason: `unknown checkType: ${event.checkType}` };
      }
      return { allowed: true };
    },
    buildActions: (event, ctx) => {
      const cred = _resolveCred(event);
      if (cred) {
        const now = Date.now();
        if (event.checkType === 'token_health') cred.lastTokenHealthCheckAt = now;
        else if (event.checkType === 'uat_refresh') cred.lastUatRefreshCheckAt = now;
        else if (event.checkType === 'data_access_expiry') cred.lastDataAccessExpiryCheckAt = now;
      }

      // ── Immediate token refresh return path (Phase 8) ──────────────────
      // If CK ordered an immediate token refresh (IMMEDIATE_TOKEN_REFRESH),
      // the health worker's completion signals back through this handler.
      // We emit TOKEN_REFRESH_RESULT via dispatchGlobal so CK's
      // GLOBAL_TRANSITION_MAP can process the decision.
      const baId = event.businessAccountId || (event.envelope && event.envelope.businessAccountId);
      if (baId && event.checkType === 'token_health' && _immediateTokenRefreshes.has(baId)) {
        _immediateTokenRefreshes.delete(baId);
        const credState = cred ? cred.state : 'UNKNOWN';
        const lastObservedAt = cred ? cred.lastObservedAt : null;
        const consecutiveFailures = cred ? cred.consecutiveFailures : 0;
        if (ctx && ctx.dispatchGlobal) {
          ctx.dispatchGlobal({
            type: 'TOKEN_REFRESH_RESULT',
            sourceDomain: 'graph-capability',
            businessAccountId: baId,
            success: credState === 'AUTHORIZED' || credState === 'LIMITED',
            capabilityState: credState,
            lastObservedAt,
            consecutiveFailures,
          });
        }
      }

      return [];
    },
  },

  // ── IMMEDIATE_TOKEN_REFRESH — CK-ordered ungated token recovery (Phase 8) ─
  // CK dispatches this when token health is UNAUTHORIZED and a retry
  // is worth attempting. The FSM emits RUN_TOKEN_HEALTH_CHECK with the
  // immediate flag, bypassing the per-cred cadence gate. The health
  // worker runs _recoverPatViaUat() for this specific account.
  IMMEDIATE_TOKEN_REFRESH: {
    target: null,  // no state change — delegate to health membrane
    guard: (event) => {
      if (!event.businessAccountId) {
        return { allowed: false, reason: 'IMMEDIATE_TOKEN_REFRESH requires businessAccountId' };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      const baId = event.businessAccountId;
      _immediateTokenRefreshes.set(baId, true);
      return [{
        type: 'RUN_TOKEN_HEALTH_CHECK',
        businessAccountId: baId,
        immediate: true,
        source: 'ck.immediate_refresh',
      }];
    },
  },

  // ── Cadence tick (Phase C) — recurring health check trigger ───────────────
  // Dispatched periodically by the constitutional kernel's cadence loop
  // (see control-plane/governance/constitutional-kernel.js → startCadenceLoop).
  // Same per-cred gating as CAPABILITY_BOOTSTRAP, but the source is
  // 'fsm.cadence_tick' so downstream subscribers can distinguish the
  // one-shot bootstrap from the recurring tick.
  CAPABILITY_CADENCE_TICK: {
    target: null,  // no state change — pure cadence signal
    guard: () => ({ allowed: true }),
    buildActions: () => {
      const now = Date.now();
      const baIds = Array.from(_byCred.keys()).filter(k => k !== '__global__');
      const actions = [];
      for (const baId of baIds) {
        if (_shouldCheck(baId, 'token_health', now)) {
          actions.push({ type: 'RUN_TOKEN_HEALTH_CHECK', businessAccountId: baId, source: 'fsm.cadence_tick' });
        }
        if (_shouldCheck(baId, 'uat_refresh', now)) {
          actions.push({ type: 'RUN_UAT_REFRESH_CHECK', businessAccountId: baId, source: 'fsm.cadence_tick' });
        }
      }
      return actions;
    },
  },

  // ── Governed data read: worker needs data from persist-telemetry ──────────
  // FSM tracks the pending read and routes DB_READ_REQUESTED to persist-telemetry.
  // The Promise controllers (resolve/reject) are stored on the pending read so
  // READ_RESULT_AVAILABLE can resolve them when data arrives.
  CAPABILITY_DATA_REQUEST: {
    target: null,  // no state change
    guard: (event) => {
      if (!event.readDomain || !event.businessAccountId || !event.readId) {
        return { allowed: false, reason: 'readDomain, businessAccountId, readId required' };
      }
      const cred = _resolveCred(event);
      if (!cred) return { allowed: false, reason: 'no cred record' };
      return { allowed: true };
    },
    buildActions: (event, ctx) => {
      const cred = _resolveCred(event);
      const { readDomain, readId, params, source, _resolve, _reject } = event;

      // Track the pending read with Promise controllers
      const timeout = setTimeout(() => {
        if (cred.pendingReads.has(readId)) {
          const p = cred.pendingReads.get(readId);
          if (p.reject) p.reject(new Error(`Read ${readId} timed out after 15s`));
          cred.pendingReads.delete(readId);
        }
      }, 15000);

      cred.pendingReads.set(readId, {
        readDomain, params, source,
        requestedAt: Date.now(),
        resolve: _resolve || null,
        reject: _reject || null,
        timeout,
      });

      // Route to persist-telemetry. Attach lineageId+lineageDomain to
      // satisfy the canonical-source gate. The lineageDomain names the
      // source FSM (graph-capability); the CK's gate allows this
      // because the source is a registered constitutional citizen.
      // (Phase 7 Findings, B1)
      if (ctx && ctx.dispatchGlobal) {
        ctx.dispatchGlobal({
          type: 'DB_READ_REQUESTED',
          readDomain,
          accountId: event.businessAccountId,
          readId,
          params,
          lineageId: `graph-capability-data-request-${readId}`,
          lineageDomain: 'graph-capability',
        });
      }
      return [];
    },
  },

  // ── Read result arrived from persist-telemetry ────────────────────────────
  // CK routes READ_RESULT_AVAILABLE here. FSM matches to pending read,
  // resolves the Promise (unblocking the requesting façade/worker), and
  // emits DATA_AVAILABLE for observability.
  READ_RESULT_AVAILABLE: {
    target: null,  // no state change
    guard: (event) => {
      const cred = _resolveCred(event);
      if (!cred) return { allowed: false, reason: 'no cred record' };
      if (!cred.pendingReads.has(event.readId)) {
        return { allowed: false, reason: `no pending read for ${event.readId}` };
      }
      return { allowed: true };
    },
    buildActions: (event, ctx) => {
      const cred = _resolveCred(event);
      const pending = cred.pendingReads.get(event.readId);
      if (!pending) return [];

      // Cleanup
      clearTimeout(pending.timeout);
      cred.pendingReads.delete(event.readId);

      // Resolve the Promise — unblocks the requesting façade/worker
      if (typeof pending.resolve === 'function') {
        pending.resolve({
          success: !event.error,
          data: event.data || null,
          error: event.error || null,
          readDomain: event.readDomain,
        });
      }

      // Emit DATA_AVAILABLE for observability + membrane subscribers
      return [{
        type: 'DATA_AVAILABLE',
        readDomain: event.readDomain,
        readId: event.readId,
        data: event.data || null,
        error: event.error || null,
        source: pending.source,
        params: pending.params,
        businessAccountId: event.accountId,
      }];
    },
  },

  // ── Read timeout cleanup (optional, for explicit timeout dispatch) ────────
  CAPABILITY_DATA_TIMEOUT: {
    target: null,
    guard: (event) => {
      const cred = _resolveCred(event);
      if (!cred) return { allowed: false, reason: 'no cred record' };
      return { allowed: true };
    },
    buildActions: (event, ctx) => {
      const cred = _resolveCred(event);
      const pending = cred.pendingReads.get(event.readId);
      if (!pending) return [];
      clearTimeout(pending.timeout);
      if (typeof pending.reject === 'function') {
        pending.reject(new Error('Read timed out'));
      }
      cred.pendingReads.delete(event.readId);
      return [];
    },
  },

  // ── Write acknowledgement arrived from persist-telemetry ──────────────────
  // CK routes DB_WRITE_ACKNOWLEDGED here (Option C — persist-telemetry FSM
  // forwards it via dispatchGlobal). FSM matches to pending write by writeId,
  // resolves the Promise (unblocking the awaiting health-substrate or worker).
  DB_WRITE_ACKNOWLEDGED: {
    target: null,  // no state change
    guard: (event) => {
      const cred = _credRecord(event.accountId || '__global__');
      if (!cred) return { allowed: false, reason: 'no cred record' };
      if (!cred.pendingWrites.has(event.writeId)) {
        return { allowed: false, reason: `no pending write for ${event.writeId}` };
      }
      return { allowed: true };
    },
    buildActions: (event, ctx) => {
      const cred = _credRecord(event.accountId || '__global__');
      const pending = cred.pendingWrites.get(event.writeId);
      if (!pending) return [];

      clearTimeout(pending.timeout);
      cred.pendingWrites.delete(event.writeId);

      if (typeof pending.resolve === 'function') {
        pending.resolve({
          success: event.success !== false,
          error: event.error || null,
          table: event.table,
          writeId: event.writeId,
        });
      }

      return [{
        type: 'WRITE_ACKNOWLEDGED',
        table: event.table,
        writeId: event.writeId,
        success: event.success !== false,
        accountId: event.accountId,
      }];
    },
  },

  // ── Worker membrane actions (Pass 2, 2026-06-11) ──────────────────────────
  // Passthrough events: routed to membrane subscribers, no FSM state change.

  RUN_QUOTA_CHECK: {
    target: null,
    guard: () => ({ allowed: true }),
    buildActions: (event) => [{
      type: 'RUN_QUOTA_CHECK',
      businessAccountId: event.businessAccountId || null,
    }],
  },

  WEBHOOK_EVENT_RECEIVED: {
    target: null,
    guard: (event) => {
      if (!event.eventId) return { allowed: false, reason: 'WEBHOOK_EVENT_RECEIVED requires eventId' };
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'WEBHOOK_EVENT_RECEIVED',
      eventId: event.eventId,
      objectId: event.objectId,
      objectType: event.objectType,
      timestamp: event.timestamp,
      payload: event.payload,
      businessAccountId: event.businessAccountId,
    }],
  },

  DEPENDENCY_HEALTH_CHECK: {
    target: null,
    guard: () => ({ allowed: true }),
    buildActions: (event) => [{
      type: 'DEPENDENCY_HEALTH_CHECK',
      businessAccountId: event.businessAccountId || '__system__',
    }],
  },

  PERMISSION_CHECK: {
    target: null,
    guard: (event) => {
      if (!event.businessAccountId) return { allowed: false, reason: 'PERMISSION_CHECK requires businessAccountId' };
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'PERMISSION_CHECK',
      businessAccountId: event.businessAccountId,
      userId: event.userId,
      token: event.token,
      currentScopes: event.currentScopes,
    }],
  },

  ACCOUNT_SYNC_CHECK: {
    target: null,
    guard: (event) => {
      if (!event.businessAccountId) return { allowed: false, reason: 'ACCOUNT_SYNC_CHECK requires businessAccountId' };
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'ACCOUNT_SYNC_CHECK',
      businessAccountId: event.businessAccountId,
      userId: event.userId,
    }],
  },

  ESCALATION_RECEIVED: {
    target: null,
    guard: () => ({ allowed: true }),
    buildActions: (event) => [{
      type: 'ESCALATION_RECEIVED',
      businessAccountId: event.businessAccountId,
      userId: event.userId,
      escalationType: event.escalationType,
      reason: event.reason,
      details: event.details,
    }],
  },

  ESCALATION_ACKNOWLEDGED: {
    target: null,
    guard: () => ({ allowed: true }),
    buildActions: (event) => [{
      type: 'ESCALATION_ACKNOWLEDGED',
      businessAccountId: event.businessAccountId,
      escalationType: event.escalationType,
    }],
  },

  // ── WORKER_RESULT — record every CK-invoked worker outcome ──────────────
  // Emitted by CK.invokeWorker after each worker.execute(). The GC FSM now
  // uses invokeWorker in CAPABILITY_CHECK (credential-capability + quota-
  // intelligence workers) via ctx.invokeWorker(). This handler receives the
  // outcome from those invocations and stamps lastWorkerResult on the cred
  // record for diagnostic traceability.
  WORKER_RESULT: {
    target: null,
    guard: (event) => {
      if (!event.workerName || !event.businessAccountId) {
        return { allowed: false, reason: 'WORKER_RESULT requires workerName, businessAccountId' };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      const cred = _byCred.get(event.businessAccountId);
      if (cred) {
        cred.lastWorkerResult = {
          workerName: event.workerName,
          outcome: event.outcome || 'completed',
          error: event.error || null,
          recordedAt: Date.now(),
        };
      }
      return [];
    },
  },
};

// ── Worker result ledger emission — FSM owns the authority ───────────────
// Called after ctx.invokeWorker() returns. The FSM writes to the
// observability ledger with its own domain authority instead of CK doing it.
function _emitWorkerResult(workerName, result, baId) {
  const obs = _obs();
  if (!obs) return;
  const outcome = (result && result.status) || (result && !result.error ? 'completed' : 'failed');
  obs.transition({
    domain: 'graph-capability',
    entity: 'worker_result',
    entityId: `graph-capability:${workerName}`,
    previousState: null,
    nextState: 'WORKER_RESULT',
    authority: 'graph-capability-fsm',
    raw: {
      workerName,
      domain: 'graph-capability',
      accountId: baId || null,
      outcome,
      data: (result && result.data) || null,
      error: (result && result.error) || null,
      invokedAt: Date.now(),
    },
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// 4. Domain-local runtime — per-cred map (private)
// ═══════════════════════════════════════════════════════════════════════════════

// Per-cred record shape:
//   {
//     state: 'UNKNOWN' | ... 9 state values,
//     evidence: { envelopeId, observedAt, businessAccountId, userId, pat, uat, detection, scope } | null,
//     lastObservedAt: number | null,
//     lastTransitionedAt: number | null,
//     consecutiveFailures: number,
//   }
//
// _byCred is the SOLE evidence store. Verdict-gate reads from it via
// getCapabilityVerdict(businessAccountId).

// ═══════════════════════════════════════════════════════════════════════════════
// 5. Dispatch
// ═══════════════════════════════════════════════════════════════════════════════

function _stateToEventType(state) {
  switch (state) {
    case 'AUTHORIZED': return 'CAPABILITY_OK';
    case 'LIMITED':    return 'CAPABILITY_PARTIAL';
    case 'UNAUTHORIZED': return 'CAPABILITY_FAILED';
    case 'DEGRADED':   return 'CAPABILITY_DEGRADED';
    default:           return null; // PENDING / UNKNOWN are not derived — observed directly
  }
}

/**
 * Merge a new envelope into the per-cred record, then infer state from
 * the merged envelope. Returns the inferred state object, or null on failure.
 *
 * This is the SINGLE mutation point for per-cred evidence.
 */
function _mergeAndInfer(envelope) {
  if (!envelope || !envelope.businessAccountId) return null;
  const cred = _credRecord(envelope.businessAccountId);
  const merged = mergeEnvelope(cred.evidence, envelope);
  cred.evidence = merged;
  cred.lastObservedAt = merged.observedAt || Date.now();
  return inferStateFromEnvelope(merged);
}

function _isObservationFresh(observedAt) {
  if (!observedAt) return false;
  const ts = typeof observedAt === 'number' ? observedAt : new Date(observedAt).getTime();
  if (isNaN(ts)) return false;
  return (Date.now() - ts) < OBSERVATION_FRESHNESS_MS_VALUE;
}

// ── Per-cred cadence gate (Phase A) ──────────────────────────────────────────
// Returns true iff the given cred is due for a check of the given type.
// A cred is due if it has never been checked, or if the time since the
// last check exceeds the policy window for that check type.
function _shouldCheck(baId, checkType, now) {
  now = now || Date.now();
  const cred = _credRecord(baId);
  if (!cred) return true; // unknown cred — let the bootstrap create the record
  let lastAt = null;
  let windowMs = null;
  if (checkType === 'token_health') {
    lastAt = cred.lastTokenHealthCheckAt;
    windowMs = TOKEN_HEALTH_WINDOW_MS;
  } else if (checkType === 'uat_refresh') {
    lastAt = cred.lastUatRefreshCheckAt;
    windowMs = UAT_REFRESH_WINDOW_MS;
  } else if (checkType === 'data_access_expiry') {
    lastAt = cred.lastDataAccessExpiryCheckAt;
    windowMs = DATA_ACCESS_EXPIRY_WINDOW_MS;
  } else {
    return true; // unknown check type — let the run proceed
  }
  if (lastAt === null || lastAt === undefined) return true;
  return (now - lastAt) >= windowMs;
}

/**
 * Process a domain event within the graph capability FSM.
 * @param {{ type: string, [key: string]: any }} event
 * @param {{ validate: Function, dispatchGlobal: Function, getGlobalState: Function }} ctx
 * @returns {{ allowed: boolean, from?: string, to?: string, actions?: Array, reason?: string, businessAccountId?: string|null }}
 */
async function _syncProjectionState() {
  try {
    const { getRedisClient } = require('../config/redis');
    const redis = getRedisClient();
    if (redis && redis.status === 'ready') {
      const raw = await redis.get('lineage:projection:domain:capability');
      if (raw) {
        const parsed = JSON.parse(raw);
        if (parsed && parsed.projection && parsed.projection.state) {
          if (typeof _localState !== 'undefined') {
            _localState = parsed.projection.state;
          }
        }
      }
    }
  } catch (_) {}
}

async function dispatch(event, ctx) {
  if (!event || typeof event !== 'object' || typeof event.type !== 'string') {
    return { allowed: false, reason: `event must be { type: string }, got ${typeof event}` };
  }

  await _syncProjectionState();

  const txn = TRANSITION_MAP[event.type];
  if (!txn) {
    return { allowed: false, reason: `unknown event type: ${event.type}` };
  }

  // Resolve the per-cred record for this event. For events that carry a
  // businessAccountId, the record exists or is created. For events without
  // (e.g. legacy CAPABILITY_EVALUATE with no baId), we operate on a
  // sentinel "__global__" record so behaviour remains deterministic.
  const baId = event.businessAccountId || (event.envelope && event.envelope.businessAccountId) || '__global__';
  const cred = _credRecord(baId);
  const from = cred.state;

  // 1. Run per-transition guard
  if (txn.guard) {
    const result = txn.guard(event);
    if (!result.allowed) {
      return { allowed: false, reason: result.reason || 'guard blocked' };
    }
  }

  // 2. Resolve target state (static or function-of-event)
  const rawTarget = txn.target;
  let target = typeof rawTarget === 'function' ? rawTarget(event) : rawTarget;

  // 3. For CAPABILITY_OBSERVATION, merge the new envelope into per-cred
  //    evidence FIRST, then infer the target from the merged evidence.
  //    This is the single mutation point for per-cred evidence.
  if (event.type === 'CAPABILITY_OBSERVATION') {
    if (!event.envelope) return { allowed: false, reason: 'envelope required' };
    const inferred = _mergeAndInfer(event.envelope);
    if (!inferred) return { allowed: false, reason: 'merge/infer failed' };
    target = inferred.state;
  }

  // 4. Ask constitutional kernel for transition approval
  if (ctx && ctx.validate) {
    const validation = ctx.validate(from, target, event);
    if (!validation.allowed) {
      return { allowed: false, reason: validation.reason || 'constitutional validation failed' };
    }
  }

  // 5. Track consecutive failures per cred — only on actual state change
  //    to avoid double-counting during the CAPABILITY_OBSERVATION → derived
  //    event recursive dispatch (where target stays UNAUTHORIZED on the
  //    inner call).
  if (target !== from) {
    if (target === 'UNAUTHORIZED') {
      cred.consecutiveFailures++;
    } else if (target === 'AUTHORIZED') {
      cred.consecutiveFailures = 0;
    }
  }

  // 6. Materialize state
  cred.state = target;
  cred.lastTransitionedAt = Date.now();

  // 7. Emit observability transition for domain FSM state change
  try {
    const obs = _obs();
    if (obs) {
      obs.transition({
        domain: 'graph-capability',
        entity: 'fsm',
        entityId: baId,
        previousState: from,
        nextState: target,
        authority: 'graph-capability-fsm',
        raw: {
          intent: event.type,
          observedAt: cred.lastObservedAt,
          consecutiveFailures: cred.consecutiveFailures,
        },
      });
    }
  } catch (_) {}

  // 8. Build actions (this is where CAPABILITY_OBSERVATION's aggregator runs)
  const actions = txn.buildActions ? txn.buildActions(event, ctx) : [];

  // 9. HSM signaling — FSM recommends, HSM decides
  const filteredActions = [];
  for (const action of actions) {
    if (action.type === 'CAPABILITY_AUTH_FAILURE') {
      if (ctx && ctx.dispatchGlobal) {
        ctx.dispatchGlobal({
          type: 'CAPABILITY_AUTH_FAILURE',
          reason: action.reason,
          evidence: action.evidence,
          businessAccountId: action.businessAccountId,
        });
      }
    } else if (action.type === 'CAPABILITY_DEGRADATION_DETECTED') {
      if (ctx && ctx.dispatchGlobal) {
        ctx.dispatchGlobal({
          type: 'CAPABILITY_DEGRADED',
          reason: action.reason,
          evidence: action.evidence,
          businessAccountId: action.businessAccountId,
        });
      }
    } else if (action.type === 'CAPABILITY_RECOVERED') {
      if (ctx && ctx.dispatchGlobal) {
        ctx.dispatchGlobal({
          type: 'CAPABILITY_RECOVERED',
          reason: `Restored from ${action.previousState}`,
          evidence: action.evidence,
          businessAccountId: action.businessAccountId,
        });
      }
    } else {
      filteredActions.push(action);
    }
  }

  console.log(`[graph-capability-fsm] ${baId}: ${from} → ${target}  (${event.type})`);

  return {
    allowed: true,
    from,
    to: target,
    actions: filteredActions,
    businessAccountId: baId,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 6. Initialization — boot with rehydrated state
// ═══════════════════════════════════════════════════════════════════════════════

function init(rehydratedState) {
  if (rehydratedState && typeof rehydratedState === 'string' && STATE_REGISTRY_VALUE[rehydratedState]) {
    // Rehydrate the global sentinel. Per-cred records are populated
    // lazily as envelopes arrive.
    const global = _credRecord('__global__');
    global.state = rehydratedState;
    console.log(`[graph-capability-fsm] Initialized with rehydrated state: ${rehydratedState}`);
  } else {
    console.log(`[graph-capability-fsm] No valid rehydrated state — starting in UNKNOWN`);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// 7. Observability — domain state queries
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Return the current FSM state. If businessAccountId is given, return
 * that cred's state. If omitted, return the global sentinel.
 */
function getState(businessAccountId) {
  const baId = businessAccountId || '__global__';
  const cred = _byCred.get(baId);
  if (!cred) return { state: 'UNKNOWN', cred: null };
  return { state: cred.state, cred };
}

function getMembraneEntry(name) {
  return _membranes.get(name) || null;
}

/**
 * Per-cred: { state, evidence, lastObservedAt, lastTransitionedAt, consecutiveFailures }
 * Global: same shape for the sentinel.
 */
function exportState(businessAccountId) {
  const baId = businessAccountId || '__global__';
  const cred = _byCred.get(baId);
  if (!cred) {
    return {
      state: 'UNKNOWN',
      evidence: null,
      lastObservedAt: null,
      lastTransitionedAt: null,
      consecutiveFailures: 0,
    };
  }
  return { ...cred };
}

function getHealth(businessAccountId) {
  const baId = businessAccountId || '__global__';
  const cred = _byCred.get(baId);
  if (!cred) {
    return {
      ok: false,
      signals: {
        state: 'UNKNOWN',
        observationFresh: false,
        observationStale: true,
        consecutiveFailures: 0,
      },
    };
  }
  const isFresh = cred.lastObservedAt
    ? (Date.now() - cred.lastObservedAt) < OBSERVATION_FRESHNESS_MS_VALUE
    : false;
  const isStale = cred.lastObservedAt
    ? (Date.now() - cred.lastObservedAt) > DEGRADED_OBSERVATION_MS_VALUE
    : true;
  return {
    ok: cred.state === 'AUTHORIZED' && isFresh && cred.consecutiveFailures === 0,
    signals: {
      state: cred.state,
      observationFresh: isFresh,
      observationStale: isStale,
      consecutiveFailures: cred.consecutiveFailures,
    },
  };
}

/**
 * Public capability verdict — the constitutional truth for a given credential.
 * fsm.requireCapability() is the only allowed consumer; everyone else goes through
 * requireCapability() which calls this.
 *
 * @param {string} [businessAccountId]
 * @returns {{ state: string, observedAt: number|null, evidence: object|null, missingScopes: string[] }}
 */
function getCapabilityVerdict(businessAccountId) {
  const baId = businessAccountId || '__global__';
  const cred = _byCred.get(baId);
  if (!cred) {
    return { state: 'UNKNOWN', observedAt: null, evidence: null, missingScopes: [] };
  }
  let missingScopes = [];
  if (cred.state === 'LIMITED' && cred.evidence && cred.evidence.scope) {
    const granted = cred.evidence.scope.grantedScopes || [];
    missingScopes = REQUIRED_SCOPES_VALUE.filter(s => !granted.includes(s));
  }
  return {
    state: cred.state,
    observedAt: cred.lastObservedAt,
    evidence: cred.evidence,
    missingScopes,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 8. Trigger Criteria
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Deterministic trigger evaluation. Per-cred: gates reason about the
 * per-cred record, not the global sentinel.
 */
function evaluateTriggerCriteria({ trigger = 'MANUAL', forced = false, businessAccountId } = {}) {
  const baId = businessAccountId || '__global__';
  const cred = _byCred.get(baId);
  const state = cred ? cred.state : 'UNKNOWN';

  // Gate 0: Observation arrived — always approved
  if (trigger === 'OBSERVATION_ARRIVED') {
    return { decision: 'APPROVED', reason: 'Worker observation arrived' };
  }

  // Gate 1: Already in evaluation (per-cred)
  if (state === 'UNKNOWN' && !forced) {
    return { decision: 'WAIT', reason: 'Evaluation in progress (UNKNOWN)' };
  }

  // Gate 2: Force
  if (forced) {
    return { decision: 'APPROVED', reason: 'Forced trigger' };
  }

  // Gate 3-6: explicit triggers
  if (trigger === 'AUTH_FAILURE_STRIKE') {
    return { decision: 'APPROVED', reason: 'Auth failure strike' };
  }
  if (trigger === 'REPEATED_GRAPH_FAILURE') {
    return { decision: 'APPROVED', reason: 'Repeated Graph failure' };
  }
  if (trigger === 'NEW_ACCOUNT_CONNECTED') {
    return { decision: 'APPROVED', reason: 'New account' };
  }
  if (trigger === 'TOKEN_REFRESHED') {
    return { decision: 'APPROVED', reason: 'Token refreshed' };
  }

  // Gate 7: cadence
  if (trigger === 'CADENCE_TICK') {
    if (state === 'UNAUTHORIZED' && cred && cred.lastTransitionedAt) {
      const elapsed = Date.now() - cred.lastTransitionedAt;
      if (elapsed < OBSERVATION_FRESHNESS_MS_VALUE) {
        return { decision: 'WAIT', reason: `Recent failure (${elapsed}ms ago) — cooldown` };
      }
    }
    return { decision: 'APPROVED', reason: 'Cadence tick' };
  }

  return { decision: 'APPROVED', reason: `Trigger ${trigger} approved` };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 9. Helpers
// ═══════════════════════════════════════════════════════════════════════════════

function listCreds() {
  return Array.from(_byCred.keys());
}

function _resetCred(baId) {
  if (baId) _byCred.delete(baId);
  else _byCred.clear();
}

// ── Canonical envelope factory ───────────────────────────────────────────────
// Migrated from substrates/graph-capability/observations.js (deleted).
// The FSM owns the envelope contract — it's the sole interpreter of envelope
// shape, so it's the canonical source for envelope construction too.
function newEnvelope({ envelopeId, businessAccountId, userId } = {}) {
  return {
    envelopeId: envelopeId || `env-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    observedAt: Date.now(),
    businessAccountId: businessAccountId || null,
    userId: userId || null,
    pat: null,
    uat: null,
    detection: null,
    scope: null,
  };
}

// ── Capability gate — migrated from verdict-gate.js (deleted) ─────────────────
// The FSM is the sole capability authority. This replaces the separate
// verdict-gate.js read adapter. Scope-diff logic lives alongside the FSM's
// getCapabilityVerdict() — one source of truth for capability evaluation.
function requireCapability(businessAccountId, requiredScopes = []) {
  const verdict = getCapabilityVerdict(businessAccountId);
  const { state, observedAt, evidence, missingScopes: fsmMissingScopes } = verdict;

  let grantedScopes = [];
  if (evidence && evidence.scope) {
    grantedScopes = evidence.scope.grantedScopes || [];
  }
  let missingScopes;
  if (state === 'LIMITED') {
    missingScopes = fsmMissingScopes || [];
  } else if (Array.isArray(requiredScopes) && requiredScopes.length > 0) {
    missingScopes = requiredScopes.filter(s => !grantedScopes.includes(s));
  } else {
    missingScopes = [];
  }

  const isFresh = observedAt
    ? (Date.now() - observedAt) < OBSERVATION_FRESHNESS_MS_VALUE
    : false;

  switch (state) {
    case 'AUTHORIZED':
      if (missingScopes.length > 0) {
        return { allowed: false, state, reason: `Required scopes not in capability evidence: ${missingScopes.join(', ')}`, missingScopes, observedAt, evidence };
      }
      if (!isFresh) {
        return { allowed: false, state, reason: 'Observation stale — re-evaluation required', missingScopes: [], observedAt, evidence };
      }
      return { allowed: true, state, reason: null, missingScopes: [], observedAt, evidence };

    case 'LIMITED':
      if (missingScopes.length > 0) {
        return { allowed: false, state, reason: `Required scopes missing: ${missingScopes.join(', ')}`, missingScopes, observedAt, evidence };
      }
      return { allowed: true, state, reason: 'Partial capability — required scopes present', missingScopes: [], observedAt, evidence };

    case 'DEGRADED':
      if (missingScopes.length > 0) {
        return { allowed: false, state, reason: `Degraded AND required scopes missing: ${missingScopes.join(', ')}`, missingScopes, observedAt, evidence };
      }
      return { allowed: true, state, reason: 'Degraded mode — reliability impaired', missingScopes: [], observedAt, evidence };

    case 'UNAUTHORIZED':
      return { allowed: false, state, reason: 'Capability denied — required capability unavailable', missingScopes, observedAt, evidence };

    case 'PAT_PENDING': case 'UAT_PENDING': case 'DETECTION_PENDING': case 'SCOPE_PENDING':
      return { allowed: false, state, reason: `Capability not yet fully observed: ${state}`, missingScopes, observedAt, evidence };

    case 'UNKNOWN':
    default:
      return { allowed: false, state: state || 'UNKNOWN', reason: 'Capability not yet evaluated', missingScopes, observedAt, evidence };
  }
}

// CK reference — set via setGovernance() by the orchestrator.
// Used for cross-domain dispatches (e.g. DB_WRITE_REQUESTED → persist-telemetry).
let _governance = null;

function setGovernance(gov) {
  _governance = gov;
}

function getGovernance() {
  return _governance;
}

// ── Dispatch ctx — built by the kernel root and shared with substrates ──────
// The signal-dispatch module binds the FSM at install time and uses this ctx
// to route substrate emissions into the FSM. The ctx shape is the same as
// what the CK passes to fsm.dispatch today: { validate, dispatchGlobal,
// getGlobalState, sanityCheck }. Built in graph-capability-kernel/index.js.
let _dispatchCtx = null;

function setDispatchCtx(ctx) {
  _dispatchCtx = ctx;
}

function getDispatchContext() {
  return _dispatchCtx;
}

// ── Membranes — downstream executors orchestrated by the FSM ────────────────
// The FSM is the policy authority. Substrates (membranes) are delegated
// executors: the FSM instantiates/registers them via setMembrane, and on
// CAPABILITY_BOOTSTRAP the FSM invokes substrate.start(ck) so the
// substrate can subscribe to the action fabric.
//
// The CK never calls a substrate or a worker directly. The FSM is the only
// constitutional path from policy to execution.
//
// _membranes: name → { substrate, wired: boolean }
let _membranes = new Map();

function setMembrane(name, { substrate } = {}) {
  if (!name || !substrate) return;
  _membranes.set(name, { substrate, wired: false });
}

function resetMembrane(name) {
  if (name) {
    const entry = _membranes.get(name);
    if (entry) entry.wired = false;
  } else {
    // Reset all membranes to unwired state (used by gck.uninstall for re-install)
    for (const entry of _membranes.values()) {
      entry.wired = false;
    }
  }
}

function _wireMembranes(ck) {
  for (const [name, entry] of _membranes) {
    if (entry.wired) continue;
    if (typeof entry.substrate.start === 'function') {
      entry.substrate.start(ck);
      entry.wired = true;
    }
  }
}

/**
 * Request a credential store operation through the constitutional flow.
 * Chain: substrate → FSM (this) → CK.dispatch(DB_WRITE_REQUESTED) → persist-telemetry FSM → writer.
 * Fire-and-forget: returns { success: true } immediately after dispatching.
 *
 * @param {{ operation: string, userId: string, businessAccountId?: string,
 *           igBusinessAccountId?: string, pageAccessToken?: string,
 *           userAccessToken?: string, pageId?: string, pageName?: string,
 *           scope?: string[], expiresAt?: string|null, dataAccessExpiresAt?: string|null,
 *           tokenType: 'page'|'user', signalCb?: Function }} params
 */
function requestCredentialStore(params) {
  if (!_governance) {
    console.warn('[graph-capability-fsm] No governance set — DB write NOT dispatched');
    return { success: false, error: 'governance_not_set' };
  }
  const {
    operation, userId, businessAccountId, igBusinessAccountId,
    pageAccessToken, userAccessToken, pageId, pageName,
    scope, expiresAt, dataAccessExpiresAt, tokenType, signalCb,
  } = params;

  _governance.dispatch({
    type: 'DB_WRITE_REQUESTED',
    domain: 'graph-capability',
    accountId: businessAccountId || igBusinessAccountId || userId,
    table: 'instagram_credentials',
    operation: 'upsert_credential',
    rows: [{
      operation,
      userId,
      businessAccountId: businessAccountId || igBusinessAccountId,
      igBusinessAccountId,
      pageAccessToken,
      userAccessToken,
      pageId,
      pageName,
      scope,
      expiresAt,
      dataAccessExpiresAt,
      tokenType,
      signalCb,
    }],
  });

  return { success: true };
}

/**
 * Request a DB write operation through the constitutional flow.
 * Generic method — supports alert inserts, lifecycle event inserts, and
 * credential status updates. Mirrors the existing requestCredentialStore.
 *
 * Chain: substrate → FSM (this) → CK.dispatch(DB_WRITE_REQUESTED) → persist-telemetry FSM → writer.
 * Fire-and-forget: returns { success: true } immediately after dispatching.
 *
 * @param {{ table: string, operation: string, accountId: string, rows: Array<object> }} params
 * @returns {{ success: boolean, error?: string }}
 */
function requestDBWrite({ table, operation, accountId, rows } = {}) {
  if (!_governance) {
    console.warn('[graph-capability-fsm] No governance set — DB write NOT dispatched');
    return { success: false, error: 'governance_not_set' };
  }
  if (!table || !operation || !accountId || !rows) {
    return { success: false, error: 'table, operation, accountId, and rows are required' };
  }
  _governance.dispatch({
    type: 'DB_WRITE_REQUESTED',
    domain: 'graph-capability',
    accountId,
    table,
    operation,
    rows,
  });
  return { success: true };
}

/**
 * Request a DB write AND await acknowledgement through the constitutional flow.
 * Returns a Promise that resolves when persist-telemetry confirms the write completed.
 *
 * Chain: caller → FSM (this) → CK.dispatch(DB_WRITE_REQUESTED) → persist-telemetry FSM
 *   → writer → DB_WRITE_COMPLETE → CK → DB_WRITE_ACKNOWLEDGED → FSM resolves Promise.
 *
 * Used for operational writes (credential status updates) where the caller depends
 * on the write having landed before proceeding. NOT for advisory writes (alerts,
 * lifecycle events) — those use the fire-and-forget requestDBWrite().
 *
 * @param {{ table: string, operation: string, accountId: string, rows: Array<object> }} params
 * @returns {Promise<{ success: boolean, data?: any, error?: string }>}
 */
function requestDBWriteAndAwait({ table, operation, accountId, rows } = {}) {
  return new Promise((resolve, reject) => {
    if (!_governance) {
      return reject(new Error('governance_not_set'));
    }
    if (!table || !operation || !accountId || !rows) {
      return reject(new Error('table, operation, accountId, and rows are required'));
    }

    const writeId = `w-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const cred = _credRecord(accountId);

    // Timeout after 15s — same as governed reads
    const timeout = setTimeout(() => {
      if (cred.pendingWrites.has(writeId)) {
        cred.pendingWrites.delete(writeId);
        reject(new Error(`Write ${writeId} timed out after 15s`));
      }
    }, 15000);

    cred.pendingWrites.set(writeId, {
      table, operation, accountId,
      requestedAt: Date.now(),
      resolve: (result) => { clearTimeout(timeout); resolve(result); },
      reject: (err) => { clearTimeout(timeout); reject(err); },
      timeout,
    });

    _governance.dispatch({
      type: 'DB_WRITE_REQUESTED',
      domain: 'graph-capability',
      accountId,
      table,
      operation,
      rows,
      writeId,
    });
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// 10. Public API
// ═══════════════════════════════════════════════════════════════════════════════

module.exports = {
  // Standard FSM contract
  dispatch,
  getState,
  exportState,
  getHealth,
  init,
  evaluateTriggerCriteria,
  // CK wiring
  setGovernance,
  getGovernance,
  // Dispatch ctx shared with signal-dispatch (FSM is the constitutional ingress)
  setDispatchCtx,
  getDispatchContext,
  // Membrane orchestration — FSM wires substrates during CAPABILITY_BOOTSTRAP
  setMembrane,
  resetMembrane,
  getMembraneEntry,
  // Cross-domain dispatch — credential store via constitutional flow
  requestCredentialStore,
  // Cross-domain dispatch — generic DB write (alerts, lifecycle events, credential status)
  requestDBWrite,
  // Cross-domain dispatch — DB write with acknowledgement (operational writes only)
  requestDBWriteAndAwait,
  // Per-cred verdict — sole source of truth
  getCapabilityVerdict,
  // Per-cred capability gate — migrated from verdict-gate.js
  requireCapability,
  // Canonical envelope factory — migrated from observations.js
  newEnvelope,
  // PAT scope fallback — migrated from default-scopes.js
  // Used when /debug_token is unavailable. Broader than REQUIRED_SCOPES.
  PAT_SCOPE_DEFAULTS: [
    'instagram_basic',
    'instagram_manage_comments',
    'instagram_manage_insights',
    'instagram_content_publish',
    'instagram_manage_messages',
    'pages_show_list',
    'pages_read_engagement',
    'pages_manage_metadata',
    'pages_read_user_content',
    'pages_manage_posts',
    'pages_manage_engagement',
  ],
  // Introspection
  STATE_REGISTRY: STATE_REGISTRY_VALUE,
  REQUIRED_SCOPES: REQUIRED_SCOPES_VALUE,
  OBSERVATION_FRESHNESS_MS: OBSERVATION_FRESHNESS_MS_VALUE,
  DEGRADED_OBSERVATION_MS: DEGRADED_OBSERVATION_MS_VALUE,
  // Cadence policy windows (Phase A)
  TOKEN_HEALTH_WINDOW_MS,
  UAT_REFRESH_WINDOW_MS,
  DATA_ACCESS_EXPIRY_WINDOW_MS,
  // Per-cred cadence gate (Phase A)
  _shouldCheck,
  PENDING_FOR,
  OBSERVATION_SLOTS,
  // Inferential layer — exposed for tests
  inferStateFromEnvelope,
  mergeEnvelope,
  // Multi-cred management
  listCreds,
  _resetCred,
  // Constants for name
  name: 'graph-capability',
};
