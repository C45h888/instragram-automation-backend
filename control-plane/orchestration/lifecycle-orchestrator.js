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
}

module.exports = { wire };
