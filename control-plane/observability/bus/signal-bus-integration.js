// control-plane/observability/bus/signal-bus-integration.js
// Signal Bus Integration: passive wrapper that auto-captures signal-bus emissions.
//
// Owns: wrapping signalBus.emit() to feed the observability plane.
// Does NOT own: signal routing, handler dispatch, or any business logic.
//
// This integration is FULLY PASSIVE:
//   - signalBus.emit() behavior is unchanged for all existing handlers
//   - observability.capture() is called AFTER handlers dispatch, never before
//   - A throwing capture() never affects the signal bus
//
// The integration patches signalBus.emit() once at init time and is
// transparent to all other code.

const signalBus = require('../../signal-bus');
const transitionEmitter = require('../emitters/transition-emitter');

// Store the original emit function before patching
const _originalEmit = signalBus.emit.bind(signalBus);

/**
 * Patched emit — calls original emit first, then captures to observability plane.
 * The capture is fire-and-forget: if it throws, the original emit has already completed.
 */
function _patchedEmit(topic, data) {
  // Call the original emit — this dispatches to all subscribers as normal
  _originalEmit(topic, data);

  // Capture to observability plane — passive, never affects signal routing
  try {
    transitionEmitter.captureSignal(topic, data);
  } catch (err) {
    // Never let observability failures affect signal bus behavior
    console.warn('[signal-bus-integration] capture error:', err.message);
  }
}

/**
 * Initialize the signal bus integration.
 * Call once at system boot — patches signalBus.emit() in place.
 *
 * After calling this, ALL signalBus.emit() calls automatically feed
 * the observability plane via captureSignal().
 */
function init() {
  signalBus.emit = _patchedEmit;
  console.log('[signal-bus-integration] Initialized — all signalBus.emit() calls will be captured');
}

module.exports = { init };
