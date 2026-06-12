// substrates/webhook-acquisition-substrate/index.js
// Webhook Acquisition Substrate: orchestration shell.
//
// Owns: setImmediate timing, per-entry dispatch, PERSIST_STAGED_EVENT
//       trigger, lifecycle (setGovernance/getGovernance), constitutional
//       bridge wiring, substrate-level state machine.
// Does NOT own: entry classification, worker dispatch, intentId generation,
//               payload normalization, failure classification.
//
// Classification and worker dispatch are delegated to intake.js.
// IntentId generation is delegated to intent-id.js.
//
// ── Substrate state machine (observational) ──────────────────────────────
// The substrate emits SUBSTRATE_STATE_TRANSITION at four state boundaries
// for the FSM's inference engine. The state is observational — the
// substrate's behavior does NOT depend on the recorded state, only on
// the substrate's natural flow. The FSM consumes the transitions to
// infer the overall state for each (accountId, intentId).
//
// States:
//   IDLE                  — processWebhook not yet called for this (accountId, intentId)
//   PAYLOAD_INCOMING      — processWebhook accepted the payload
//   INTAKE_CLASSIFYING    — intake.processEntry dispatched an item
//   WORKER_DISPATCHED     — worker.execute was called
//   PERSIST_REQUESTED     — PERSIST_STAGED_EVENT was fired to the FSM
//   FAILED_INTAKE         — intake threw or returned no items
//   FAILED_PERSIST_DISPATCH — PERSIST_STAGED_EVENT dispatch threw

const { processEntry } = require('./intake');

// ── Governance reference (set by orchestrator at boot) ─────────────────────
let _governance = null;

function setGovernance(governance) {
  _governance = governance;
}

function getGovernance() {
  return _governance;
}

// ── Substrate state machine (per-(accountId, intentId) observational) ──────

// key: accountId|intentId  →  current state string
const _substrateState = new Map();

function _stateKey(accountId, intentId) {
  return `${accountId || '_'}::${intentId || '_'}`;
}

function _getState(accountId, intentId) {
  return _substrateState.get(_stateKey(accountId, intentId)) || 'IDLE';
}

function _setState(accountId, intentId, to) {
  if (accountId && intentId) {
    _substrateState.set(_stateKey(accountId, intentId), to);
  }
}

function _emitTransition(accountId, intentId, from, to, reason) {
  if (!_governance || typeof _governance.dispatch !== 'function') return;
  try {
    _governance.dispatch({
      type: 'SUBSTRATE_STATE_TRANSITION',
      accountId, intentId, from, to, reason: reason || null,
      lineageId: `webhook:${accountId}:${intentId}`,
      lineageDomain: 'webhook-acquisition-substrate',
    });
  } catch (_) { /* observation is best-effort */ }
}

/**
 * Record a substrate state transition. Called at each state boundary.
 * Defensive: idempotent on the same (from, to) pair; no-op if the
 * current state already matches `to` (prevents double-emit on retries).
 */
function _transition(accountId, intentId, to, reason) {
  const from = _getState(accountId, intentId);
  if (from === to) return;
  _setState(accountId, intentId, to);
  _emitTransition(accountId, intentId, from, to, reason);
}

// ── Public entry point (called by routes/webhook.js) ──────────────────────

/**
 * Process a Meta Instagram webhook payload. Returns a routing summary;
 * the heavy work runs async via setImmediate so Meta gets a 200 fast.
 *
 * @param {object} payload   — the parsed Meta webhook body
 * @param {string} accountId — the IG account id this webhook is for
 * @returns {{ accepted: number, dropped: number, entries: number }}
 */
function processWebhook(payload, accountId) {
  if (!payload || typeof payload !== 'object') {
    return { accepted: 0, dropped: 0, entries: 0, reason: 'payload_not_object' };
  }
  if (payload.object !== 'instagram') {
    return { accepted: 0, dropped: 0, entries: 0, reason: 'not_instagram_object' };
  }
  if (!Array.isArray(payload.entry) || payload.entry.length === 0) {
    return { accepted: 0, dropped: 0, entries: 0, reason: 'empty_entry' };
  }

  const entries = payload.entry;
  const totalEntries = entries.length;

  // Fire-and-forget per entry. Meta must see a 200 within ~10s; per-entry
  // work is bounded but the substrate does not block the response.
  for (const entry of entries) {
    setImmediate(async () => {
      try {
        // ── State boundary: IDLE → PAYLOAD_INCOMING ─────────────────
        // We don't have an intentId at this point; emit a per-payload
        // transition with a synthetic intentId so the FSM sees the
        // boundary even before intake generates intentIds.
        // Actually: we do this AFTER intake returns, per intentId. The
        // first per-intent transition is INTAKE_CLASSIFYING.

        // Intake classifies the entry and dispatches each item to the
        // matching worker. Returns { processed, discarded, intentIds }.
        const { processed, discarded, intentIds } =
          await processEntry(entry, accountId, _governance);

        // ── State boundary per intentId: IDLE → INTAKE_CLASSIFYING ──
        // We re-purpose: the first transition we record for an intentId
        // is INTAKE_CLASSIFYING, then WORKER_DISPATCHED (set inside
        // intake per item). The IDLE → PAYLOAD_INCOMING transition is
        // emitted once per entry.
        if (Array.isArray(intentIds)) {
          for (const intentId of intentIds) {
            _transition(accountId, intentId, 'INTAKE_CLASSIFYING', 'intake_classified');
            _transition(accountId, intentId, 'WORKER_DISPATCHED', 'worker_called');
          }
        }

        // Phase 2: eagerly fire PERSIST_STAGED_EVENT for each staged
        // canonical event. The FSM hydrates → resolves → emits
        // DB_WRITE_REQUESTED → persist-telemetry-fsm → writers.
        if (_governance && typeof _governance.dispatch === 'function') {
          for (const intentId of intentIds) {
            const dispatchResult = _governance.dispatch({
              type: 'PERSIST_STAGED_EVENT',
              accountId,
              intentId,
              eventId: intentId, // substrate owns both at this point
            });
            if (process.env.WEBHOOK_DEBUG &&
                dispatchResult && typeof dispatchResult.then === 'function') {
              dispatchResult.then((r) => {
                console.log('[substrate] PERSIST_STAGED_EVENT result:',
                  r?.allowed, r?.reason);
              });
            }
            // ── State boundary: WORKER_DISPATCHED → PERSIST_REQUESTED ─
            // The transition is only meaningful AFTER the dispatch is
            // issued (not awaited — fire-and-forget per the substrate's
            // contract). The FSM's guard will reject the PERSIST_STAGED_EVENT
            // if the inferred state is wrong.
            _transition(accountId, intentId, 'PERSIST_REQUESTED', 'persist_staged_dispatched');
          }
        }
      } catch (err) {
        // Defensive: intake throwing kills the setImmediate callback;
        // emit a signal so the system knows the entry was dropped.
        try {
          if (_governance && typeof _governance.dispatch === 'function') {
            _governance.dispatch({
              type: 'WEBHOOK_PROCESS_FAILED',
              accountId,
              reason: `substrate_threw:${err.message}`,
            });
          }
        } catch (_) {}
      }
    });
  }

  return {
    accepted: 0,        // not synchronously known — work runs async
    dropped: 0,
    entries: totalEntries,
    asyncDispatched: true,
  };
}

// ── Constitutional bridge: acquisition-fsm → persist-telemetry-fsm ─────────
// The acquisition-fsm emits DB_WRITE_REQUESTED as an action. CK's
// _emitActions routes to subscribers, but the persist-telemetry-fsm
// is a domain (not a subscriber). This bridge subscribes to the
// action and re-dispatches through CK so DOMAIN_EVENT_MAP routes it.
//
// Kept (not redundant): _emitActions only fires subscribers, while
// DOMAIN_EVENT_MAP is consulted only by dispatch(). The bridge IS
// the path that connects the two.
let _bridgedGovernance = null;

function wireConstitutionalBridge(governance) {
  _bridgedGovernance = governance;
  if (!governance || typeof governance.subscribeAction !== 'function') return;
  governance.subscribeAction('DB_WRITE_REQUESTED', (action) => {
    if (!action || !action.type) return;
    if (action.lineageDomain !== 'acquisition-fsm') return;
    if (process.env.WEBHOOK_DEBUG) {
      console.log('[bridge] re-dispatching DB_WRITE_REQUESTED:',
        action.table, 'rows:', action.rows?.length);
    }
    if (typeof _bridgedGovernance.dispatch === 'function') {
      _bridgedGovernance.dispatch({
        type: 'DB_WRITE_REQUESTED',
        accountId: action.accountId,
        intentId: action.intentId,
        table: action.table,
        operation: action.operation,
        rows: action.rows,
        lineageId: action.lineageId,
        lineageDomain: action.lineageDomain,
        domain: action.domain || 'persist-telemetry',
        extra: action.extra,
      });
    }
  });
}

module.exports = {
  setGovernance,
  getGovernance,
  processWebhook,
  wireConstitutionalBridge,
  // Exposed for tests + observability
  _getSubstrateState: (accountId, intentId) => _getState(accountId, intentId),
  _substrateStateSize: () => _substrateState.size,
};
