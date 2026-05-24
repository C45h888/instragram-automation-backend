// control-plane/governance/dedup.js
// Governance substrate: deduplication and idempotency.
//
// Owns: in-flight intent dedup, idempotency key tracking.
// Does NOT own: evaluation logic, persistence, orchestration.
//
// Uses a simple in-memory Set for in-flight deduplication.
// Key format: `${accountId}:${actionType}:${resourceId}`
// Cleared after each evaluation cycle (per debounced event batch).

const _inFlight = new Set();

/**
 * Marks a resource as in-flight for the current evaluation cycle.
 */
function markInFlight(accountId, actionType, resourceId) {
  _inFlight.add(`${accountId}:${actionType}:${resourceId}`);
}

/**
 * Checks if a resource is already in-flight this evaluation cycle.
 * @returns {boolean}
 */
function isInFlight(accountId, actionType, resourceId) {
  return _inFlight.has(`${accountId}:${actionType}:${resourceId}`);
}

/**
 * Clears all in-flight entries after an evaluation cycle.
 * Called by evaluator after each batch.
 */
function clearTick() {
  _inFlight.clear();
}

module.exports = {
  markInFlight,
  isInFlight,
  clearTick,
};
