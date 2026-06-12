// substrates/webhook-acquisition-substrate/workers/_state-machine.js
// Worker State Machine — bounded state emitter for each worker.
//
// Owns: per-(accountId, intentId) state tracking, transition validation,
//        WORKER_STATE_TRANSITION emission.
// Does NOT own: validation, normalization, dispatch, error analysis.
//
// External emission policy: only ENTRY/EXIT transitions are emitted to CK.
// Internal states (VALIDATING, NORMALIZING) are recorded in the worker's
// history but not broadcast. This keeps the per-event emission to 2
// (the final STAGED or FAILED_*) plus 1 entry transition (IDLE → VALIDATING).
//
// State machine:
//   IDLE              (initial)
//     └→ VALIDATING   (on execute() entry, emitted)
//          └→ NORMALIZING (internal, not emitted)
//               └→ DISPATCHING (internal, not emitted)
//                    └→ STAGED              (terminal, emitted)
//   Any ─→ FAILED_VALIDATION | FAILED_NORMALIZE | FAILED_DISPATCH  (terminal, emitted)
//
// All terminal states are absorbing.

const _LEGAL_TRANSITIONS = {
  IDLE:              new Set(['VALIDATING', 'FAILED_VALIDATION']),
  VALIDATING:        new Set(['NORMALIZING', 'FAILED_VALIDATION']),
  NORMALIZING:       new Set(['DISPATCHING', 'FAILED_NORMALIZE']),
  DISPATCHING:       new Set(['STAGED', 'FAILED_DISPATCH']),
  STAGED:            new Set(),
  FAILED_VALIDATION: new Set(),
  FAILED_NORMALIZE:  new Set(),
  FAILED_DISPATCH:   new Set(),
};

// Whether a transition should be emitted to CK.
const _EMITTABLE = new Set([
  'VALIDATING',         // entry into the worker
  'STAGED',             // terminal success
  'FAILED_VALIDATION',  // terminal failure
  'FAILED_NORMALIZE',   // terminal failure
  'FAILED_DISPATCH',    // terminal failure
]);

class WorkerStateMachine {
  /**
   * @param {object} opts
   * @param {string} opts.accountId
   * @param {string} opts.intentId
   * @param {string} opts.eventType      — canonical event type (e.g., 'comment')
   * @param {string} opts.domain         — domain string (e.g., 'webhook:comments')
   * @param {object} opts.governance     — CK reference; may be null (no emit if absent)
   * @param {string} [opts.eventId]      — canonical event id (for lineage)
   */
  constructor({ accountId, intentId, eventType, domain, governance, eventId }) {
    this._state = 'IDLE';
    this._accountId = accountId || null;
    this._intentId = intentId || null;
    this._eventType = eventType || null;
    this._domain = domain || null;
    this._governance = governance || null;
    this._eventId = eventId || null;
    this._history = [{ from: null, to: 'IDLE', at: Date.now() }];
  }

  /**
   * Transition to a new state. Validates the transition is legal, records
   * history, and emits WORKER_STATE_TRANSITION to CK if the state is
   * externally observable.
   *
   * @param {string} to        — target state
   * @param {string} [reason]  — optional human-readable reason (for failures)
   * @returns {string} the new state
   */
  transition(to, reason) {
    const allowed = _LEGAL_TRANSITIONS[this._state];
    if (!allowed) {
      throw new Error(`[worker-state-machine] illegal transition: state=${this._state} not in table`);
    }
    if (!allowed.has(to)) {
      throw new Error(
        `[worker-state-machine] illegal transition: ${this._state} → ${to} ` +
        `(accountId=${this._accountId}, intentId=${this._intentId})`
      );
    }

    const from = this._state;
    this._state = to;
    this._history.push({ from, to, reason: reason || null, at: Date.now() });

    if (_EMITTABLE.has(to) && this._governance && typeof this._governance.dispatch === 'function') {
      try {
        this._governance.dispatch({
          type: 'WORKER_STATE_TRANSITION',
          accountId: this._accountId,
          intentId: this._intentId,
          eventId: this._eventId,
          from,
          to,
          eventType: this._eventType,
          domain: this._domain,
          reason: reason || null,
          lineageId: `worker:${this._accountId}:${this._intentId}`,
          lineageDomain: 'webhook-acquisition-worker',
        });
      } catch (_) {
        // Emit failure must not break the worker.
        // The state is still recorded internally; the FSM will infer from history.
      }
    }

    return this._state;
  }

  getState() {
    return this._state;
  }

  getHistory() {
    return this._history.slice();
  }

  isTerminal() {
    return _LEGAL_TRANSITIONS[this._state] && _LEGAL_TRANSITIONS[this._state].size === 0;
  }
}

module.exports = { WorkerStateMachine, _LEGAL_TRANSITIONS, _EMITTABLE };
