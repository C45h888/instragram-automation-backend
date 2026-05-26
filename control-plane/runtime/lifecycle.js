// control-plane/runtime/lifecycle.js
// Account Lifecycle: bounded account discovery and removal tracking.
//
// Owns: discovering active accounts, tracking membership, signalling removal.
// Does NOT own: evaluation, emission, signal intake, operational safety,
//               worker execution (workers are deprecated — HSM governs acquisition).
//
// Workers (BRPOP loops) have been DEPRECATED. All acquisition now flows
// through the governance HSM: governance discovers intents from Redis in
// its tick() cycle, the orchestrator executes them via execution-bridge.
//
// Contract:
//   lifecycle.refresh()  → discover accounts, track membership, signal removals
//   lifecycle.stopAll()  → clear all tracked accounts
//   lifecycle.onRemove(fn) → register removal callback

const { getRedisClient } = require('../../config/redis');
const { getActiveAccounts } = require('../../substrates/persistence');

/** Set of currently active account IDs */
const _activeAccounts = new Set();

/** Called when an account is removed so other modules can clean up. */
let _onRemove = null;

/**
 * Returns live runtime state. Deterministic, no side effects.
 * @returns {{ accounts: number }}
 */
function status() {
  return { accounts: _activeAccounts.size };
}

/**
 * Register a callback invoked when an account is removed during refresh.
 * @param {Function} fn — (accountId: string) => void
 * @throws {Error} if fn is not a function
 */
function onRemove(fn) {
  if (typeof fn !== 'function') {
    throw new Error(`[lifecycle] onRemove handler must be a function, got ${typeof fn}`);
  }
  _onRemove = fn;
}

/**
 * Discover active accounts, track new ones, signal removed ones.
 * Notifies the onRemove callback for cleanup in other modules.
 *
 * Acquisition execution is now governed by the HSM — this function only
 * tracks account membership. The governance kernel's tick() discovers
 * intents from Redis queues using the account list.
 *
 * @returns {Promise<{ok: boolean, error?: string, added: number, removed: number}>}
 */
async function refresh() {
  const redis = getRedisClient();
  if (!redis || redis.status !== 'ready') {
    console.warn('[lifecycle] Redis not ready — skipping refresh');
    return { ok: false, error: 'redis unavailable', added: 0, removed: 0 };
  }

  const accounts = await getActiveAccounts();
  const currentIds = new Set(accounts.map(a => a.id));

  let added = 0;
  let removed = 0;

  // Track new accounts
  for (const accountId of currentIds) {
    if (!_activeAccounts.has(accountId)) {
      _activeAccounts.add(accountId);
      added++;
      console.log(`[lifecycle] Account ${accountId} added`);
      // Observability: account lifecycle transition
      _emitTransition(accountId, 'UNKNOWN', 'ACTIVE');
    }
  }

  // Signal removed accounts
  for (const accountId of _activeAccounts) {
    if (!currentIds.has(accountId)) {
      _activeAccounts.delete(accountId);
      removed++;
      // Observability: account lifecycle transition
      _emitTransition(accountId, 'ACTIVE', 'REMOVED');
      if (_onRemove) _onRemove(accountId);
      console.log(`[lifecycle] Account ${accountId} removed`);
    }
  }

  return { ok: true, added, removed };
}

/**
 * Emit observability transition for account lifecycle changes.
 */
function _emitTransition(accountId, previousState, nextState) {
  try {
    const observability = require('../observability/emitters/transition-emitter');
    observability.transition({
      domain: 'lifecycle',
      entity: 'account',
      entityId: accountId,
      previousState,
      nextState,
      authority: 'lifecycle-runtime',
      raw: {},
    });
  } catch (err) {
    console.warn('[lifecycle] Observability transition error:', err.message);
  }
}

/**
 * Clear all tracked accounts.
 */
function stopAll() {
  _activeAccounts.clear();
  console.log('[lifecycle] All accounts cleared');
}

module.exports = { status, refresh, stopAll, onRemove };

