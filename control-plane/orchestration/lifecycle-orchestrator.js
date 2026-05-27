// control-plane/orchestration/lifecycle-orchestrator.js
// Lifecycle Orchestrator: constitutional coordination membrane.
//
// Owns: routing account lifecycle events between the lifecycle module,
//        buffer, and governance.
// Does NOT own: lifecycle policy, account discovery logic,
//               governance decisions, disconnect policy.
//
// Constitutional purity: this orchestrator mechanically wires
// lifecycle events without interpreting what account changes mean.
// It never decides which accounts to keep or remove.

const lifecycle = require('../runtime/lifecycle');
const buffer = require('../runtime/buffer');
const { clearCredentialCache } = require('../../helpers/agent-helpers');

/**
 * Wire this orchestrator to the governance kernel.
 * Registers lifecycle event handlers.
 *
 * @param {object} governance — governance kernel module
 */
function wire(governance) {
  // ── Account removal → buffer cleanup ───────────────────────────────────
  lifecycle.onRemove((accountId) => {
    buffer.destroy(accountId);
  });

  // ── DISCONNECT_ACCOUNT → lifecycle module (governance-ordered) ─────────
  governance.subscribeAction('DISCONNECT_ACCOUNT', (action) => {
    console.warn(`[lifecycle-orchestrator] Governance ordered disconnect for ${action.accountId}: ${action.reason}`);
    // Lifecycle module handles the actual disconnect mechanics
    // The governance kernel already updated its internal state
  });

  // ── CLEAR_CREDENTIAL_CACHE → credential cache (governance-ordered) ─────
  // The engagement-telemetry-interpreter detects rate limit pressure via
  // retry substrate polling and dispatches CLEAR_CREDENTIAL_CACHE upward
  // through governance. This ensures credential cache invalidation is a
  // governance decision, not a direct substrate mutation.
  governance.subscribeAction('CLEAR_CREDENTIAL_CACHE', (action) => {
    if (action.accountId) {
      clearCredentialCache(action.accountId, action.reason || 'governance_dispatch');
      console.warn(`[lifecycle-orchestrator] CLEAR_CREDENTIAL_CACHE for ${action.accountId} (${action.reason})`);
    }
  });
}

module.exports = { wire };
