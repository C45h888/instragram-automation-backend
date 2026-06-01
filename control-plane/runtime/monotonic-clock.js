// control-plane/runtime/monotonic-clock.js
// Monotonic Clock: strictly-increasing ticker for constitutional timestamp ordering.
//
// Owns: generating monotonically increasing timestamps for constitutional
//        sequencing, replay determinism, and reconciliation comparison.
//        The ticker guarantees timestamp[N+1] > timestamp[N] always.
//
// Does NOT own: wall-clock time (use Date.now() for observability),
//               governance decisions, replay semantics, interpretation.
//
// Architectural invariant:
//   constitutional ordering → this ticker (monotonic, deterministic)
//   observability/debugging → Date.now() / wallClockTimestamp (non-monotonic)
//
// The ticker starts at 0 on module load and increments by 1 on every call.
// It is a simple integer counter — no Date.now() dependency, no regression
// possible, replay-safe. Same sequence of calls ALWAYS produces same sequence
// of timestamps.
//
// Dual-timestamp model (Phase 3.1):
//   timestamp           = this ticker (constitutional ordering authority)
//   wallClockTimestamp   = Date.now() (observability, preserved separately)

let _tick = 0;

/**
 * Return the next monotonically increasing timestamp.
 * Pure counter — no wall-clock dependency, no regression possible.
 *
 * @returns {number} strictly increasing integer timestamp
 */
function nextTimestamp() {
  return ++_tick;
}

/**
 * Return the current tick value without advancing.
 * Used for checkpoint snapshots and coordination boundary marking.
 *
 * @returns {number} current tick value
 */
function getCurrentTick() {
  return _tick;
}

/**
 * Reset the ticker to 0. Used in test teardown to ensure
 * deterministic tick sequences across test runs.
 */
function reset() {
  _tick = 0;
}

module.exports = { nextTimestamp, getCurrentTick, reset };
