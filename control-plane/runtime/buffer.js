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
 * Returns live runtime state. Deterministic, no side effects.
 * @returns {{ accounts: number, pendingTimers: number, debounceMs: number }}
 */
function status() {
  return {
    accounts: _buffer.size,
    pendingTimers: _debounceTimers.size,
    debounceMs: _debounceMs,
  };
}

/**
 * Configure the debounce window in milliseconds.
 * @param {number} ms — must be > 0
 * @throws {Error} if ms is not a positive number
 */
function setDebounceMs(ms) {
  if (typeof ms !== 'number' || ms <= 0) {
    throw new Error(`[buffer] debounceMs must be > 0, got ${ms}`);
  }
  _debounceMs = ms;
}

/**
 * Register the flush handler. Called when a debounce timer elapses with
 * the accumulated events for an account.
 * @param {Function} fn — async (accountId: string, events: Array) => void
 * @throws {Error} if fn is not a function
 */
function onFlush(fn) {
  if (typeof fn !== 'function') {
    throw new Error(`[buffer] onFlush handler must be a function, got ${typeof fn}`);
  }
  _onFlush = fn;
}

/**
 * Ingest a signal event. Accumulates into account buffer and resets
 * the debounce timer. When the timer elapses, the flush handler is called.
 * @param {{ accountId: string, table: string, record: object }} event
 * @throws {Error} if event is missing required fields
 */
function ingest(event) {
  if (!event || typeof event !== 'object') {
    throw new Error(`[buffer] ingest requires an event object, got ${typeof event}`);
  }
  const { accountId, table, record } = event;
  if (!accountId || !table || !record) {
    throw new Error(`[buffer] ingest requires { accountId, table, record }, missing: ${!accountId ? 'accountId' : !table ? 'table' : 'record'}`);
  }

  // Observability: buffer ingest transition
  _emitBufferTransition(accountId, 'IDLE', 'INGESTING', { table, recordCount: 1 });

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

    // Observability: buffer flush transition
    if (events && events.length > 0) {
      _emitBufferTransition(accountId, 'INGESTING', 'FLUSHING', { eventCount: events.length });
    }

    if (events && events.length > 0 && _onFlush) {
      try {
        await _onFlush(accountId, events);
      } catch (err) {
        console.error(`[buffer] Flush error for account ${accountId}:`, err.message);
      }
    }

    // Observability: buffer return to idle after flush
    if (events && events.length > 0) {
      _emitBufferTransition(accountId, 'FLUSHING', 'IDLE', { eventCount: events.length });
    }
  }, _debounceMs));
}

/**
 * Emit observability transition for buffer state changes.
 * Wrapped in try/catch — observability failures never affect buffer behavior.
 */
function _emitBufferTransition(accountId, previousState, nextState, extraRaw = {}) {
  try {
    const observability = require('../observability/emitters/transition-emitter');
    observability.transition({
      domain: 'buffer',
      entity: 'buffer',
      entityId: accountId,
      previousState,
      nextState,
      authority: 'buffer-runtime',
      raw: extraRaw,
    });
  } catch (err) {
    console.warn('[buffer] Observability transition error:', err.message);
  }
}

/**
 * Destroy buffer and debounce timer for a specific account.
 * Idempotent — safe to call on unknown account (no-op).
 * @param {string} accountId
 */
function destroy(accountId) {
  const hadBuffer = _buffer.has(accountId) || _debounceTimers.has(accountId);
  _buffer.delete(accountId);
  const timer = _debounceTimers.get(accountId);
  if (timer) {
    clearTimeout(timer);
    _debounceTimers.delete(accountId);
  }

  // Observability: buffer destroyed transition
  if (hadBuffer) {
    try {
      const observability = require('../observability/emitters/transition-emitter');
      observability.transition({
        domain: 'buffer',
        entity: 'buffer',
        entityId: accountId,
        previousState: 'IDLE',
        nextState: 'DESTROYED',
        authority: 'buffer-runtime',
        raw: { accountId },
      });
    } catch (err) {
      console.warn('[buffer] Observability transition error:', err.message);
    }
  }
}

/**
 * Destroy all buffers and cancel all pending debounce timers.
 * Idempotent — safe to call on empty buffer (no-op).
 */
function destroyAll() {
  for (const timer of _debounceTimers.values()) {
    clearTimeout(timer);
  }
  _debounceTimers.clear();
  _buffer.clear();
}

/**
 * Return a snapshot of the current buffer state for reconciliation inspection.
 * Read-only — no mutation. Used by the reconciliation engine for ghost emission detection.
 * @returns {{ size: number, flushing: boolean, accountCount: number }}
 */
function snapshot() {
  let totalSize = 0;
  for (const events of _buffer.values()) {
    totalSize += events.length;
  }
  return {
    size: totalSize,
    flushing: _debounceTimers.size > 0,
    accountCount: _buffer.size,
  };
}

module.exports = { status, ingest, setDebounceMs, onFlush, destroy, destroyAll, snapshot };
