// control-plane/orchestration/degradation-orchestrator.js
// Degradation Orchestrator: constitutional coordination membrane.
//
// Owns: routing degradation/ recovery/ halt log actions to structured logging,
//        forwarding system alert creation to observability systems.
// Does NOT own: degradation detection, recovery policy, halt decisions,
//               alert classification.
//
// Constitutional purity: this orchestrator mechanically logs and routes
// degradation observations. It NEVER determines degradation state.
// Governance alone decides when to degrade, recover, or halt.

const { logAudit } = require('../../config/supabase');

/**
 * Wire this orchestrator to the governance kernel.
 * Registers per-action-type subscribers for degradation actions.
 *
 * @param {object} governance — governance kernel module
 */
function wire(governance) {
  // ── LOG_DEGRADED → structured logging ──────────────────────────────────
  governance.subscribeAction('LOG_DEGRADED', (action) => {
    console.warn(`[degradation-orchestrator] Runtime DEGRADED.${action.substate}: ${action.reason}`);
  });

  // ── LOG_RECOVERY → structured logging ──────────────────────────────────
  governance.subscribeAction('LOG_RECOVERY', (action) => {
    console.warn(`[degradation-orchestrator] Runtime RECOVERY.${action.substate}`);
  });

  // ── LOG_HALT → structured logging ──────────────────────────────────────
  governance.subscribeAction('LOG_HALT', (action) => {
    console.error(`[degradation-orchestrator] Runtime HALTED: ${action.reason}`);
  });

  // ── CREATE_SYSTEM_ALERT → observability persistence ────────────────────
  governance.subscribeAction('CREATE_SYSTEM_ALERT', (action) => {
    const { alertType, accountId, message, details } = action;
    console.error(`[degradation-orchestrator] System alert [${alertType}]: ${message}`);
    // Persist to Supabase system_alerts (fire-and-forget)
    logAudit({
      event_type: alertType,
      action: 'system_alert',
      resource_type: 'instagram_business_account',
      resource_id: accountId || null,
      details: { ...details, message },
      success: false,
    }).catch(err => {
      console.error(`[degradation-orchestrator] Failed to persist system alert:`, err.message);
    });
  });
}

module.exports = { wire };
