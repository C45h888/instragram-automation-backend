// acquisition-kernel/inference-engine.js
// FSM Inference Engine — deterministic state reducer.
//
// Owns: the per-(accountId, intentId) transition log, the deterministic
//        reduce function, the inferred-state cache.
// Does NOT own: dispatch, governance calls, side effects, I/O.
//
// The FSM uses this engine to answer:
//   1. "What is the inferred state for (accountId, intentId)?" — pure reduce.
//   2. "Is this transition legal from the inferred state?" — table check.
//   3. "How many intents are in flight?" — derived from the log.
//
// The engine is PURE. Same transition log → same inferred state. Always.

// ── Inferred states (the FSM's view) ───────────────────────────────────────
const INFERRED = Object.freeze({
  ABSENT:                   'ABSENT',
  INTAKE_RECEIVED:          'INTAKE_RECEIVED',
  INTAKE_CLASSIFYING:       'INTAKE_CLASSIFYING',
  WORKER_DISPATCHED:        'WORKER_DISPATCHED',
  STAGED:                   'STAGED',
  PERSIST_REQUESTED:        'PERSIST_REQUESTED',
  PERSIST_ACKNOWLEDGED:     'PERSIST_ACKNOWLEDGED',
  DB_WRITE_DISPATCHED:      'DB_WRITE_DISPATCHED',
  PERSISTED:                'PERSISTED',
  PERSIST_FAILED_GATE:      'PERSIST_FAILED_GATE',
  PERSIST_FAILED_HYDRATION: 'PERSIST_FAILED_HYDRATION',
  PERSIST_FAILED_RESOLVE:   'PERSIST_FAILED_RESOLVE',
  PERSIST_FAILED:           'PERSIST_FAILED',
});

// Terminal states — no further transitions accepted.
const TERMINAL = new Set([
  INFERRED.PERSISTED,
  INFERRED.PERSIST_FAILED_GATE,
  INFERRED.PERSIST_FAILED_HYDRATION,
  INFERRED.PERSIST_FAILED_RESOLVE,
  INFERRED.PERSIST_FAILED,
]);

// ── Transition key ─────────────────────────────────────────────────────────
// Only SUBSTRATE_STATE_TRANSITION events feed the reducer (from the
// webhook-acquisition-substrate). The FSM also records synthetic
// INTERNAL transitions for its own state bookkeeping.
//
// Workers do NOT dispatch through the inference engine. The worker state
// machine (_state-machine.js) emits WORKER_STATE_TRANSITION events, but
// they are not routed by CK's DOMAIN_EVENT_MAP to the FSM. The inference
// engine therefore only accepts SUBSTRATE and INTERNAL sources.
//
// The FSM seeds the STAGED state directly in WEBHOOK_EVENT_RECEIVED's
// buildActions via recordTransition(). This is the canonical source: the
// FSM itself records the transition because it just staged the event.
// This replaces the dead worker-pipeline path that was never wired.
//
// We normalize everything to a { source, from, to, at } tuple. The reducer
// then uses (currentState, source, from, to) to produce the next state.

// ── Reduce table (currentState + transition key → nextState) ──────────────
//
// source: 'SUBSTRATE' | 'WORKER' | 'INTERNAL'
// transition is (source, from, to).
//
// The table is exhaustive. Anything not in the table is "no change"
// (defensive: unknown transitions don't crash, they don't transition either).

const _REDUCE_TABLE = {
  [INFERRED.ABSENT]: {
    'SUBSTRATE|IDLE|PAYLOAD_INCOMING':               INFERRED.INTAKE_RECEIVED,
    'SUBSTRATE|IDLE|INTAKE_CLASSIFYING':             INFERRED.INTAKE_CLASSIFYING,  // race: intake fired first
    'SUBSTRATE|IDLE|WORKER_DISPATCHED':              INFERRED.WORKER_DISPATCHED,  // race: worker fired first
  },
  [INFERRED.INTAKE_RECEIVED]: {
    'SUBSTRATE|PAYLOAD_INCOMING|INTAKE_CLASSIFYING': INFERRED.INTAKE_CLASSIFYING,
  },
  [INFERRED.INTAKE_CLASSIFYING]: {
    'SUBSTRATE|INTAKE_CLASSIFYING|WORKER_DISPATCHED':INFERRED.WORKER_DISPATCHED,
  },
  [INFERRED.WORKER_DISPATCHED]: {
    // Seeded by the FSM in WEBHOOK_EVENT_RECEIVED buildActions. The FSM
    // just staged the event and records this transition as the canonical
    // source. Workers do NOT dispatch through the inference engine.
    'SUBSTRATE|WORKER_DISPATCHED|STAGED':             INFERRED.STAGED,
  },
  [INFERRED.STAGED]: {
    // Substrate's third transition: WORKER_DISPATCHED→PERSIST_REQUESTED.
    // May arrive before or after the FSM seeds STAGED. If it arrives
    // after, this advances STAGED→PERSIST_REQUESTED. If it arrives
    // before (while still at WORKER_DISPATCHED), it's silently dropped
    // and the PERSIST_STAGED_EVENT guard accepts STAGED directly.
    'SUBSTRATE|WORKER_DISPATCHED|PERSIST_REQUESTED': INFERRED.PERSIST_REQUESTED,
  },
  [INFERRED.PERSIST_REQUESTED]: {
    'INTERNAL|PERSIST_REQUESTED|PERSIST_ACKNOWLEDGED':     INFERRED.PERSIST_ACKNOWLEDGED,
    'SUBSTRATE|PERSIST_REQUESTED|FAILED_HYDRATION':         INFERRED.PERSIST_FAILED_HYDRATION,
    'SUBSTRATE|PERSIST_REQUESTED|FAILED_RESOLVE':           INFERRED.PERSIST_FAILED_RESOLVE,
    'SUBSTRATE|PERSIST_REQUESTED|FAILED_GATE':              INFERRED.PERSIST_FAILED_GATE,
  },
  [INFERRED.PERSIST_ACKNOWLEDGED]: {
    'INTERNAL|PERSIST_ACKNOWLEDGED|DB_WRITE_DISPATCHED':   INFERRED.DB_WRITE_DISPATCHED,
  },
  [INFERRED.DB_WRITE_DISPATCHED]: {
    // DB_WRITE_COMPLETE / DB_WRITE_FAILED are NOT in the substrate/worker
    // transition stream — they are kernel-level events. The FSM records
    // them as INTERNAL transitions when it sees them.
    'INTERNAL|DB_WRITE_DISPATCHED|PERSISTED':              INFERRED.PERSISTED,
    'INTERNAL|DB_WRITE_DISPATCHED|PERSIST_FAILED':          INFERRED.PERSIST_FAILED,
  },
  // Terminal states: no transitions. The reducer is idempotent.
  [INFERRED.PERSISTED]: {},
  [INFERRED.PERSIST_FAILED]: {},
  [INFERRED.PERSIST_FAILED_GATE]: {},
  [INFERRED.PERSIST_FAILED_HYDRATION]: {},
  [INFERRED.PERSIST_FAILED_RESOLVE]: {},
};

// ── Transition log (per (accountId, intentId)) ────────────────────────────

// key: accountId|intentId
// value: [{ source, from, to, at }, ...]
const _log = new Map();

function _key(accountId, intentId) {
  return `${accountId || '_'}::${intentId || '_'}`;
}

function _logFor(accountId, intentId) {
  const k = _key(accountId, intentId);
  let arr = _log.get(k);
  if (!arr) {
    arr = [];
    _log.set(k, arr);
  }
  return arr;
}

/**
 * Record a transition. The transition is just stored; the reducer
 * computes the inferred state by replaying the log.
 */
function recordTransition(accountId, intentId, source, from, to, at) {
  const arr = _logFor(accountId, intentId);
  arr.push({
    source: source || 'INTERNAL',
    from: from || null,
    to: to || null,
    at: at || Date.now(),
  });
}

/**
 * Reduce the transition log for (accountId, intentId) to a single
 * inferred state. Deterministic. Pure.
 */
function reduceInferredState(accountId, intentId) {
  const arr = _log.get(_key(accountId, intentId));
  if (!arr || arr.length === 0) return INFERRED.ABSENT;

  let state = INFERRED.ABSENT;
  for (const t of arr) {
    const rule = `${t.source}|${t.from || ''}|${t.to || ''}`;
    const table = _REDUCE_TABLE[state] || {};
    const next = table[rule];
    if (next) state = next;
    // Unknown rule: state unchanged (defensive).
  }
  return state;
}

/**
 * Check if a (source, from, to) transition is LEGAL from the current
 * inferred state. Used by FSM guards to reject bad events.
 *
 * @param {string} accountId
 * @param {string} intentId
 * @param {string} source  — 'SUBSTRATE' | 'INTERNAL'
 * @param {string} from    — the from state claimed by the transition
 * @param {string} to      — the to state claimed by the transition
 * @returns {boolean}
 */
function isLegalTransition(accountId, intentId, source, from, to) {
  if (isTerminalState(reduceInferredState(accountId, intentId))) return false;
  // The current inferred state must be the table-keyed state from which
  // this transition is supposed to depart. The table is keyed by state,
  // not by the transition's "from" claim — because the "from" claim
  // describes the abstract state machine, not the inferred state.
  // Example: a substrate transition SUBSTRATE|IDLE|PAYLOAD_INCOMING is
  // legal when the current inferred state is ABSENT (we haven't recorded
  // anything yet, so we're conceptually in IDLE).
  const currentState = reduceInferredState(accountId, intentId);
  const table = _REDUCE_TABLE[currentState] || {};
  const rule = `${source}|${from || ''}|${to || ''}`;
  return Object.prototype.hasOwnProperty.call(table, rule);
}

function isTerminalState(state) {
  return TERMINAL.has(state);
}

/**
 * Derive global counters from the log (instead of stored mutable state).
 */
function countByState(state) {
  let n = 0;
  for (const [, arr] of _log) {
    if (arr.length === 0) continue;
    // Reduce just the last entry to its state
    let s = INFERRED.ABSENT;
    for (const t of arr) {
      const rule = `${t.source}|${t.from || ''}|${t.to || ''}`;
      const next = (_REDUCE_TABLE[s] || {})[rule];
      if (next) s = next;
    }
    if (s === state) n++;
  }
  return n;
}

function countInFlight() {
  let n = 0;
  for (const [, arr] of _log) {
    if (arr.length === 0) continue;
    let s = INFERRED.ABSENT;
    for (const t of arr) {
      const rule = `${t.source}|${t.from || ''}|${t.to || ''}`;
      const next = (_REDUCE_TABLE[s] || {})[rule];
      if (next) s = next;
    }
    if (s === INFERRED.STAGED ||
        s === INFERRED.PERSIST_REQUESTED ||
        s === INFERRED.PERSIST_ACKNOWLEDGED ||
        s === INFERRED.DB_WRITE_DISPATCHED) n++;
  }
  return n;
}

/**
 * Get the transition log for an intent (for observability + tests).
 */
function getTransitions(accountId, intentId) {
  const arr = _log.get(_key(accountId, intentId));
  return arr ? arr.slice() : [];
}

/**
 * Drop the log entry for a terminal intent (housekeeping).
 * Called after the FSM has fully drained the staged event.
 */
function purgeIfTerminal(accountId, intentId) {
  const state = reduceInferredState(accountId, intentId);
  if (isTerminalState(state)) {
    _log.delete(_key(accountId, intentId));
    return true;
  }
  return false;
}

/**
 * Test helper: clear all log state. Do NOT use in production.
 */
function _clear() {
  _log.clear();
}

module.exports = {
  INFERRED,
  TERMINAL,
  recordTransition,
  reduceInferredState,
  isLegalTransition,
  isTerminalState,
  countByState,
  countInFlight,
  getTransitions,
  purgeIfTerminal,
  _clear,
  _REDUCE_TABLE,
};
