// postgres-telemetry-kernel/substrates/cognition-scanner/workers/event-worker.js
// Event Worker: owns per-account FIFO pending queue and FSM dispatch decisions.
// Bound to: accountId → queue → governance dispatch.
// Does NOT own: channel lifecycle, subscription management.

/** @type {Map<string, Array<{table: string, record: object}>>} */
const _pending = new Map(); // accountId → [{ table, record }, ...]

/**
 * Push a Realtime event to the account's FIFO queue.
 * Flushes immediately only if the publishing FSM is IDLE.
 *
 * @param {string} accountId
 * @param {string} table
 * @param {object} record
 * @param {object} publishingFsm — must implement getState(): string
 * @param {object} governance — must implement dispatch()
 */
function pushEvent(accountId, table, record, publishingFsm, governance) {
  if (!_pending.has(accountId)) {
    _pending.set(accountId, []);
  }
  _pending.get(accountId).push({ table, record });

  if (publishingFsm && publishingFsm.getState && publishingFsm.getState() === 'IDLE') {
    flushQueue(accountId, governance);
  }
}

/**
 * Flush the pending queue for an account. Dispatches PUBLISHING_DATA_AVAILABLE.
 * Called on EMISSION_OBSERVATION or when FSM transitions to IDLE.
 *
 * @param {string} accountId
 * @param {object} governance
 * @returns {boolean} true if queue was flushed
 */
function flushQueue(accountId, governance) {
  if (!_pending.has(accountId) || _pending.get(accountId).length === 0) return false;

  const events = _pending.get(accountId);
  _pending.delete(accountId);

  governance.dispatch({
    type: 'PUBLISHING_DATA_AVAILABLE',
    accountId,
  });
  return true;
}

/**
 * Handle EMISSION_OBSERVATION — flush any pending events for that account.
 *
 * @param {object} event — { accountId }
 * @param {object} governance
 */
function onEmissionObservation(event, governance) {
  const { accountId } = event;
  if (accountId) flushQueue(accountId, governance);
}

/**
 * Clear all pending queues. Call on substrate stop.
 */
function clearAll() {
  _pending.clear();
}

/**
 * Remove pending queue for one account.
 *
 * @param {string} accountId
 */
function clearAccount(accountId) {
  _pending.delete(accountId);
}

/**
 * Live runtime state snapshot.
 */
function status() {
  let pendingCount = 0;
  for (const q of _pending.values()) {
    pendingCount += q.length;
  }
  return {
    pendingCount,
    accountCount: _pending.size,
  };
}

module.exports = {
  pushEvent,
  flushQueue,
  onEmissionObservation,
  clearAll,
  clearAccount,
  status,
};