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
  WORKER_VALIDATING:        'WORKER_VALIDATING',
  WORKER_NORMALIZING:       'WORKER_NORMALIZING',
  WORKER_DISPATCHING:       'WORKER_DISPATCHING',
  STAGED:                   'STAGED',
  PERSIST_REQUESTED:        'PERSIST_REQUESTED',
  PERSIST_ACKNOWLEDGED:     'PERSIST_ACKNOWLEDGED',
  DB_WRITE_DISPATCHED:      'DB_WRITE_DISPATCHED',
  PERSISTED:                'PERSISTED',
  PERSIST_FAILED_GATE:      'PERSIST_FAILED_GATE',
  PERSIST_FAILED_HYDRATION: 'PERSIST_FAILED_HYDRATION',
  PERSIST_FAILED_RESOLVE:   'PERSIST_FAILED_RESOLVE',
  PERSIST_FAILED:           'PERSIST_FAILED',
  DISCARDED_VALIDATION:     'DISCARDED_VALIDATION',
  DISCARDED_NORMALIZE:      'DISCARDED_NORMALIZE',
  DISCARDED_DISPATCH:       'DISCARDED_DISPATCH',
});

// Terminal states — no further transitions accepted.
const TERMINAL = new Set([
  INFERRED.PERSISTED,
  INFERRED.PERSIST_FAILED_GATE,
  INFERRED.PERSIST_FAILED_HYDRATION,
  INFERRED.PERSIST_FAILED_RESOLVE,
  INFERRED.PERSIST_FAILED,
  INFERRED.DISCARDED_VALIDATION,
  INFERRED.DISCARDED_NORMALIZE,
  INFERRED.DISCARDED_DISPATCH,
]);

// ── Transition key ─────────────────────────────────────────────────────────
// Two streams of transitions feed the reducer:
//   WORKER_STATE_TRANSITION  (from any worker, via _state-machine.js)
//   SUBSTRATE_STATE_TRANSITION (from webhook-acquisition-substrate)
//
// Plus FSM-emitted transitions on the SAME log:
//   SUBSTRATE_STATE_TRANSITION with from=PERSIST_REQUESTED, to=PERSIST_ACKNOWLEDGED
//   SUBSTRATE_STATE_TRANSITION with from=PERSIST_ACKNOWLEDGED, to=DB_WRITE_DISPATCHED
//
// And from the FSM's own internal-emit path, the FSM records a
// SYNTHETIC transition:
//   { type: 'INTERNAL', from: PERSIST_REQUESTED, to: PERSIST_ACKNOWLEDGED, at }
//   { type: 'INTERNAL', from: PERSIST_ACKNOWLEDGED, to: DB_WRITE_DISPATCHED, at }
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
    // A worker can fire its first transition before the substrate's
    // PAYLOAD_INCOMING lands (race condition). Accept it from ABSENT.
    'WORKER|IDLE|VALIDATING':                        INFERRED.WORKER_VALIDATING,
  },
  [INFERRED.INTAKE_RECEIVED]: {
    'SUBSTRATE|PAYLOAD_INCOMING|INTAKE_CLASSIFYING': INFERRED.INTAKE_CLASSIFYING,
  },
  [INFERRED.INTAKE_CLASSIFYING]: {
    'SUBSTRATE|INTAKE_CLASSIFYING|WORKER_DISPATCHED':INFERRED.WORKER_DISPATCHED,
  },
  [INFERRED.WORKER_DISPATCHED]: {
    'WORKER|IDLE|VALIDATING':                        INFERRED.WORKER_VALIDATING,
    'WORKER|IDLE|FAILED_VALIDATION':                 INFERRED.DISCARDED_VALIDATION,
  },
  [INFERRED.WORKER_VALIDATING]: {
    'WORKER|VALIDATING|NORMALIZING':                 INFERRED.WORKER_NORMALIZING,
    'WORKER|VALIDATING|FAILED_VALIDATION':           INFERRED.DISCARDED_VALIDATION,
  },
  [INFERRED.WORKER_NORMALIZING]: {
    'WORKER|NORMALIZING|DISPATCHING':                INFERRED.WORKER_DISPATCHING,
    'WORKER|NORMALIZING|FAILED_NORMALIZE':           INFERRED.DISCARDED_NORMALIZE,
  },
  [INFERRED.WORKER_DISPATCHING]: {
    'WORKER|DISPATCHING|STAGED':                     INFERRED.STAGED,
    'WORKER|DISPATCHING|FAILED_DISPATCH':            INFERRED.DISCARDED_DISPATCH,
  },
  [INFERRED.STAGED]: {
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
  [INFERRED.DISCARDED_VALIDATION]: {},
  [INFERRED.DISCARDED_NORMALIZE]: {},
  [INFERRED.DISCARDED_DISPATCH]: {},
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
 * @param {string} source  — 'WORKER' | 'SUBSTRATE' | 'INTERNAL'
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
