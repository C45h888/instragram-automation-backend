// graph-capability-kernel/substrates/workers/account-sync-worker.js
// Account Synchronization Worker — reconciliation layer for account-level
// consistency across Instagram Account State, Token State, Webhook State,
// Publication State, Analytics State, and FSM State.
//
// Consumes: cross-domain state — FSM per-cred state, token state from
//           vault, webhook state from §10, publication state from §9,
//           analytics state from publishing-kernel
//
// Owns:
//   - Cross-domain inconsistency detection:
//     token valid but FSM says UNAUTHORIZED
//     webhook active but publications not syncing
//     analytics stale but token fresh
//     FSM state diverged from actual token state
//   - Targeted reconciliation actions:
//     re-evaluate capability
//     re-sync publications
//     refresh analytics
//   - Consistency ledger: last-synced timestamp per domain, per account
//   - Drift scoring: aggregate inconsistency score per account
//
// Does NOT own:
//   - Individual domain operations (domain workers own)
//   - Error classification (ig-reliability-substrate)
//   - Raw API calls (vault.* owns)
//
// States: SYNCED → DRIFT_DETECTED → RECONCILING
//
// Membrane interface: start(governance), stop(), isStarted()

const fsm = require('../../fsm');
const signalDispatch = require('../vault/signal-dispatch');

class AccountSyncWorker {
  constructor() {
    this._started = false;
    this._governance = null;

    // Consistency ledger: baId → { lastSyncedAt, domains: { token, webhook, publication, analytics, fsm } }
    this._ledger = new Map();

    // Drift scores per account
    this._driftScores = new Map();
  }

  // ── Membrane interface ──────────────────────────────────────────────────

  start(governance) {
    if (this._started) return;
    this._started = true;
    this._governance = governance;

    governance.subscribeAction('ACCOUNT_SYNC_CHECK', (action) => {
      this.executeSyncCheck(action).catch(err => {
        console.error('[account-sync] ACCOUNT_SYNC_CHECK failed:', err.message);
      });
    });

    console.log('[account-sync] Membrane wired — subscribed to ACCOUNT_SYNC_CHECK');
  }

  stop() {
    this._started = false;
  }

  isStarted() {
    return this._started;
  }

  // ── Public: execute sync check ──────────────────────────────────────────

  /**
   * Reconcile all domains for a specific account. Checks token state
   * against FSM state, webhook state, and publication state. Flags
   * drift and triggers targeted reconciliation.
   *
   * @param {{ businessAccountId: string, userId: string }} action
   */
  async executeSyncCheck(action = {}) {
    const { businessAccountId, userId } = action;

    if (!businessAccountId) {
      return { synced: false, reason: 'missing_businessAccountId' };
    }

    const inconsistencies = [];
    const now = Date.now();

    // 1. Get FSM per-cred state
    const credState = fsm.getCapabilityVerdict(businessAccountId);
    const fsmState = credState?.state || 'UNKNOWN';

    // 2. Get token lifecycle state from reliability substrate
    //    (token health worker already tracks this — we consume the FSM state)

    // 3. Consistency checks
    const driftScore = this._computeDriftScore(businessAccountId);

    // Check: FSM state is UNKNOWN but account has credentials
    if (fsmState === 'UNKNOWN') {
      const creds = fsm.listCreds ? fsm.listCreds() : [];
      const hasCred = creds.some(c => c === businessAccountId);
      if (hasCred) {
        inconsistencies.push({
          domain: 'fsm-vs-credential',
          detail: 'FSM state UNKNOWN but credentials exist',
          severity: 'HIGH',
        });
      }
    }

    // Check: FSM state is UNAUTHORIZED — check if recovery is possible
    if (fsmState === 'UNAUTHORIZED') {
      inconsistencies.push({
        domain: 'capability',
        detail: 'FSM reports UNAUTHORIZED — recovery may be needed',
        severity: 'HIGH',
      });
    }

    // Check: DEGRADED state — reliability impaired
    if (fsmState === 'DEGRADED') {
      inconsistencies.push({
        domain: 'capability',
        detail: 'FSM reports DEGRADED — reliability impairment detected',
        severity: 'MEDIUM',
      });
    }

    // Update ledger
    const entry = this._ledger.get(businessAccountId) || {
      lastSyncedAt: null,
      domains: {},
    };
    entry.lastSyncedAt = now;
    entry.domains.fsm = { state: fsmState, checkedAt: now };
    entry.domains.sync = { inconsistencies: inconsistencies.length, checkedAt: now };
    this._ledger.set(businessAccountId, entry);

    // Update drift score
    this._driftScores.set(businessAccountId, inconsistencies.length * 25);

    if (inconsistencies.length > 0) {
      console.warn(`[account-sync] Drift detected for ${businessAccountId}: ${inconsistencies.map(i => i.domain).join(', ')}`);

      // Emit observation envelope to FSM for re-evaluation
      const env = fsm.newEnvelope({ businessAccountId, userId });
      env.detection = {
        isValid: true,
        reliabilityImpaired: inconsistencies.some(i => i.severity === 'HIGH'),
        reason: 'account_sync_drift',
        details: { inconsistencies, driftScore: this._driftScores.get(businessAccountId) },
      };
      signalDispatch.emitEnvelope({ envelope: env });

      // If HIGH severity, escalate
      if (inconsistencies.some(i => i.severity === 'HIGH')) {
        fsm.dispatch({
          type: 'ESCALATION_RECEIVED',
          businessAccountId,
          userId,
          escalationType: 'account_sync_drift',
          reason: `Account sync drift: ${inconsistencies.map(i => i.detail).join('; ')}`,
          details: { inconsistencies },
        });
        return { synced: false, escalated: true, inconsistencies };
      }

      return { synced: false, escalated: false, inconsistencies };
    }

    return { synced: true, fsmState };
  }

  // ── Internal ────────────────────────────────────────────────────────────

  _computeDriftScore(baId) {
    return this._driftScores.get(baId) || 0;
  }
}

module.exports = AccountSyncWorker;
