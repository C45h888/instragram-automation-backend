// scheduling-kernel/substrates/cadence/lifecycle.js
// Account Lifecycle: bounded account membership tracking.
// Kernelized from: control-plane/runtime/lifecycle.js
//
// Owns: tracking active account membership, detecting adds/removals.
// Does NOT own: governance policy, observability emission, reading data,
//               callback chains — those belong to the FSM and CK.
//
// Architectural invariant:
//   This substrate is PURELY MECHANICAL. It receives data, compares against
//   its internal Set, and returns deltas. It does NOT call governance,
//   does NOT emit observability, does NOT register callbacks.
//   The FSM governs. The worker executes. The substrate provides mechanics.
//
// Contract:
//   lifecycle.refresh(accounts)  → { added: string[], removed: string[], currentIds: string[] }
//   lifecycle.stopAll()          → clear all tracked accounts
//   lifecycle.status()           → { accounts: number }
//   lifecycle.getAccountIds()    → string[]

/** Set of currently active account IDs — the sole mutable state. */
const _activeAccounts = new Set();

/**
 * Returns live runtime state. Deterministic, no side effects.
 * @returns {{ accounts: number }}
 */
function status() {
  return { accounts: _activeAccounts.size };
}

/**
 * Returns the currently tracked active account IDs.
 * Deterministic, no side effects.
 * @returns {string[]}
 */
function getAccountIds() {
  return Array.from(_activeAccounts);
}

/**
 * Compare provided accounts against the internal membership set.
 * Returns added and removed account IDs. Pure mechanical operation —
 * no governance calls, no observability, no callbacks.
 *
 * The caller (worker, invoked by FSM) provides the account data.
 * The FSM obtained it through a governed read via CK.
 *
 * @param {Array<{id: string}>} accounts — active accounts from governed read
 * @returns {{ added: string[], removed: string[], currentIds: string[] }}
 */
function refresh(accounts) {
  if (!Array.isArray(accounts)) {
    return { added: [], removed: [], currentIds: getAccountIds() };
  }

  const currentIds = new Set(accounts.map(a => a.id));
  const added = [];
  const removed = [];

  // Discover newly added accounts
  for (const id of currentIds) {
    if (!_activeAccounts.has(id)) {
      _activeAccounts.add(id);
      added.push(id);
      console.log(`[lifecycle] Account ${id} added`);
    }
  }

  // Discover removed accounts
  for (const id of _activeAccounts) {
    if (!currentIds.has(id)) {
      _activeAccounts.delete(id);
      removed.push(id);
      console.log(`[lifecycle] Account ${id} removed`);
    }
  }

  return { added, removed, currentIds: Array.from(currentIds) };
}

/**
 * Clear all tracked accounts. Used at shutdown.
 */
function stopAll() {
  _activeAccounts.clear();
  console.log('[lifecycle] All accounts cleared');
}

module.exports = { status, getAccountIds, refresh, stopAll };
