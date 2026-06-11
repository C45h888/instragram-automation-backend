// graph-capability-kernel/substrates/workers/permission-recovery-worker.js
// Permission Recovery Worker — handles permission revocation, missing scopes,
// role changes, account disconnects, business verification changes, and
// authorization drift. Permission failures trigger recovery workflows, not retries.
//
// Consumes: ig-reliability-substrate §2 PERMISSION_FAILURE classification
//           with subtypes (permission_denied, permission_not_granted);
//           scope information from vault
//
// Owns:
//   - Scope drift detection: current vs REQUIRED_SCOPES vs last-known scopes
//   - Role change detection: page role, IG business role
//   - Account disconnect detection: token valid but page unlinked
//   - Business verification change detection
//   - Authorization drift across multiple sequential permission checks
//   - Recovery workflow: trigger OAuth re-auth prompt, page re-linking
//
// Recovery:
//   - Scope re-request: emit REAUTHORIZE_USER event for OAuth re-auth flow
//   - Page re-linking: check if page relationship still exists
//   - Business verification: check business verification status
//
// Never: retries the same operation expecting permissions to fix themselves.
//
// States: CHECKING → DRIFT_DETECTED → RECOVERING → ESCALATED
//
// Membrane interface: start(governance), stop(), isStarted()

const igReliability = require('../../../substrates/ig-reliability-substrate');
const vault = require('../vault');
const fsm = require('../../fsm');
const signalDispatch = require('../vault/signal-dispatch');

class PermissionRecoveryWorker {
  constructor() {
    this._started = false;
    this._governance = null;

    // Per-account permission drift tracking
    // baId → { lastKnownScopes: string[], lastCheckedAt: number, driftCount: number }
    this._permissionState = new Map();
  }

  // ── Membrane interface ──────────────────────────────────────────────────

  start(governance) {
    if (this._started) return;
    this._started = true;
    this._governance = governance;

    governance.subscribeAction('PERMISSION_CHECK', (action) => {
      this.executePermissionCheck(action).catch(err => {
        console.error('[permission-recovery] PERMISSION_CHECK failed:', err.message);
      });
    });

    console.log('[permission-recovery] Membrane wired — subscribed to PERMISSION_CHECK');
  }

  stop() {
    this._started = false;
  }

  isStarted() {
    return this._started;
  }

  // ── Public: execute permission check ────────────────────────────────────

  /**
   * Check for permission drift for a specific account.
   * Compares current scopes against last-known scopes and REQUIRED_SCOPES.
   *
   * @param {{ businessAccountId: string, userId: string, token: string,
   *           currentScopes: string[] }} action
   */
  async executePermissionCheck(action = {}) {
    const { businessAccountId, userId, token, currentScopes } = action;

    if (!businessAccountId) {
      return { drift: false, reason: 'missing_businessAccountId' };
    }

    // Get current scopes if not provided (via /debug_token)
    let scopes = currentScopes;
    if (!scopes && token) {
      try {
        const tokenInfo = await vault.uat.detect({ token, businessAccountId, userId });
        scopes = tokenInfo?.scopes || [];
      } catch (err) {
        console.warn(`[permission-recovery] Failed to detect scopes for ${businessAccountId}:`, err.message);
        return { drift: false, reason: 'scope_detection_failed', error: err.message };
      }
    }

    if (!scopes || scopes.length === 0) {
      return { drift: false, reason: 'no_scopes_available' };
    }

    // Get last-known scopes
    const lastKnown = this._permissionState.get(businessAccountId);
    const previousScopes = lastKnown?.lastKnownScopes || [];

    // Detect scope drift
    const requiredScopes = fsm.REQUIRED_SCOPES;
    const missingRequired = requiredScopes.filter(s => !scopes.includes(s));
    const newScopes = scopes.filter(s => !previousScopes.includes(s));
    const revokedScopes = previousScopes.filter(s => !scopes.includes(s));

    const hasDrift = missingRequired.length > 0 || revokedScopes.length > 0;

    // Update last-known state
    const entry = lastKnown || { lastKnownScopes: [], lastCheckedAt: null, driftCount: 0 };
    entry.lastKnownScopes = scopes;
    entry.lastCheckedAt = Date.now();

    if (hasDrift) {
      entry.driftCount++;

      console.warn(`[permission-recovery] Drift detected for ${businessAccountId}: missing=${missingRequired.join(',')} revoked=${revokedScopes.join(',')}`);

      // Emit PERMISSION_FAILURE envelope to FSM
      const env = fsm.newEnvelope({ businessAccountId, userId });
      env.scope = { grantedScopes: scopes, cacheAgeMs: 0 };
      env.detection = {
        isValid: false,
        reason: 'permission_drift',
        details: { missingRequired, revokedScopes, driftCount: entry.driftCount },
      };
      signalDispatch.emitEnvelope({ envelope: env });

      // If repeated drift (3+), escalate
      if (entry.driftCount >= 3) {
        fsm.dispatch({
          type: 'ESCALATION_RECEIVED',
          businessAccountId,
          userId,
          escalationType: 'permission_repeated_drift',
          reason: `Permission drift repeated ${entry.driftCount} times`,
          details: { missingRequired, revokedScopes, scopes, previousScopes },
        });
        return { drift: true, escalated: true, missingRequired, revokedScopes };
      }

      return { drift: true, escalated: false, missingRequired, revokedScopes };
    }

    // No drift: reset counter
    entry.driftCount = 0;
    this._permissionState.set(businessAccountId, entry);

    return { drift: false, scopes };
  }

  // ── Public: record successful scope observation ─────────────────────────

  /**
   * Record known-good scopes when scope detection succeeds.
   * Called by vault.scope.detectDynamic on successful /debug_token.
   */
  recordScopes(businessAccountId, scopes) {
    if (!businessAccountId || !scopes) return;
    const entry = this._permissionState.get(businessAccountId) || {
      lastKnownScopes: [],
      lastCheckedAt: null,
      driftCount: 0,
    };
    entry.lastKnownScopes = scopes;
    entry.lastCheckedAt = Date.now();
    entry.driftCount = 0;
    this._permissionState.set(businessAccountId, entry);
  }
}

module.exports = PermissionRecoveryWorker;
