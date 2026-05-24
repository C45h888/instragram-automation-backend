// control-plane/runtime/buffer.js
// Event Buffer: bounded event accumulation and debounced flush scheduling.
//
// Owns: accumulating db:insert events, debounce-per-account scheduling,
//        triggering flush callbacks when the debounce window elapses.
// Does NOT own: evaluation, emission, signal intake, worker lifecycle.
//
// Contract:
//   buffer.ingest(event)               → accumulate, schedule debounced flush
//   buffer.setDebounceMs(ms)           → configure debounce window (default 500)
//   buffer.onFlush(fn)                 → register handler: (accountId, events) => void
//   buffer.destroy(accountId)          → remove account's buffer + cancel timer
//   buffer.destroyAll()                → clear all buffers and timers

let _debounceMs = 500;

/** accountId → [{ table, record, receivedAt }] */
const _buffer = new Map();

/** accountId → Timeout */
const _debounceTimers = new Map();

let _onFlush = null;

/**
 * Configure the debounce window in milliseconds.
 */
function setDebounceMs(ms) {
  _debounceMs = ms;
}

/**
 * Register the flush handler. Called when a debounce timer elapses with
 * the accumulated events for an account.
 * @param {Function} fn — async (accountId, events) => void
 */
function onFlush(fn) {
  _onFlush = fn;
}

/**
 * Ingest a signal event. Accumulates into account buffer and resets
 * the debounce timer. When the timer elapses, the flush handler is called.
 * @param {{ accountId: string, table: string, record: object }} event
 */
function ingest(event) {
  const { accountId, table, record } = event;
  if (!accountId || !table || !record) return;

  if (!_buffer.has(accountId)) {
    _buffer.set(accountId, []);
  }
  _buffer.get(accountId).push({ table, record, receivedAt: Date.now() });

  // Reset debounce: rapid inserts on the same account batch together
  if (_debounceTimers.has(accountId)) {
    clearTimeout(_debounceTimers.get(accountId));
  }
  _debounceTimers.set(accountId, setTimeout(async () => {
    _debounceTimers.delete(accountId);
    const events = _buffer.get(accountId);
    _buffer.delete(accountId);
    if (events && events.length > 0 && _onFlush) {
      try {
        await _onFlush(accountId, events);
      } catch (err) {
        console.error(`[buffer] Flush error for account ${accountId}:`, err.message);
      }
    }
  }, _debounceMs));
}

/**
 * Destroy buffer and debounce timer for a specific account.
 */
function destroy(accountId) {
  _buffer.delete(accountId);
  const timer = _debounceTimers.get(accountId);
  if (timer) {
    clearTimeout(timer);
    _debounceTimers.delete(accountId);
  }
}

/**
 * Destroy all buffers and cancel all pending debounce timers.
 */
function destroyAll() {
  for (const timer of _debounceTimers.values()) {
    clearTimeout(timer);
  }
  _debounceTimers.clear();
  _buffer.clear();
}

module.exports = { ingest, setDebounceMs, onFlush, destroy, destroyAll };
