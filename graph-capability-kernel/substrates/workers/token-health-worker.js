// graph-capability-kernel/substrates/workers/token-health-worker.js
// Token Health Worker — highest authority operational worker within the
// Instagram subsystem. No Instagram execution occurs without token validation.
//
// Consumes: ig-reliability-substrate §3 token lifecycle analysis
//           (tokenAge, daysUntilExpiry, refreshEligible, refreshWindowActive,
//            authorizationState, permissionScopes, isLongLived, recommendation)
//
// Owns:
//   - Token age analysis
//   - Expiration monitoring
//   - Permission verification
//   - Scope validation against REQUIRED_SCOPES
//   - Refresh eligibility checks
//   - Refresh execution (proactive, before expiry)
//   - Token lineage tracking (parent → child → refresh chain)
//   - Token health scoring (0-100 composite)
//   - PAT validation via /debug_token
//   - UAT→PAT silent recovery (absorbed from recovery-worker)
//   - UAT proactive refresh
//   - Data access expiry detection + deduplicated warnings
//
// Does NOT own:
//   - DB reads (dispatched through CK.governedRead)
//   - DB writes (dispatched through fsm.requestDBWrite / requestDBWriteAndAwait)
//   - Raw API calls (delegated to vault.* workers)
//   - Error classification (ig-reliability-substrate owns)
//   - Retry policy (FSM owns)
//   - Credential storage (vault owns)
//
// Membrane interface:
//   start(governance) — subscribes to FSM actions
//   stop() — unsubscribes
//   isStarted() — boolean
//
// Migration origin:
//   health-substrate/index.js — runTokenHealthCheck(), runUATRefreshCheck()
//   health-substrate/workers/recovery-worker.js — execute() → _recoverPatViaUat()

const vault = require('../vault');
const fsm = require('../../fsm');
const signalDispatch = require('../vault/signal-dispatch');
const { getSupabaseAdmin, logAudit } = require('../../../config/supabase');
const { clearCredentialCache } = require('../../../helpers/credential-cache');
const igReliability = require('../../../substrates/ig-reliability-substrate');

// ── Private helpers ─────────────────────────────────────────────────────────

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function _ageDays(issuedAt) {
  if (!issuedAt) return null;
  return Math.floor((Date.now() - new Date(issuedAt).getTime()) / 86400000);
}

// ── Token Health Worker ────────────────────────────────────────────────────

class TokenHealthWorker {
  constructor() {
    this._started = false;
    this._governance = null;
  }

  // ── Membrane interface ──────────────────────────────────────────────────

  start(governance) {
    if (this._started) return;
    this._started = true;
    this._governance = governance;

    governance.subscribeAction('RUN_TOKEN_HEALTH_CHECK', (action) => {
      // ── Immediate token refresh (Phase 8) — CK-ordered ungated recovery ──
      // When action.immediate is true, run targeted recovery for a single
      // account with no cadence gate. Used by the acquisition→CK→GCK
      // cross-kernel failure recovery flow.
      if (action.immediate && action.businessAccountId) {
        console.log(`[token-health] Immediate token refresh for account ${action.businessAccountId}`);
        this.executeImmediateRecovery(action.businessAccountId).catch(err => {
          console.error('[token-health] Immediate token refresh failed:', err.message);
        });
        return;
      }
      console.log('[token-health] Membrane received RUN_TOKEN_HEALTH_CHECK — executing');
      this.executeTokenHealth().catch(err => {
        console.error('[token-health] RUN_TOKEN_HEALTH_CHECK failed:', err.message);
      });
    });
    governance.subscribeAction('RUN_UAT_REFRESH_CHECK', (action) => {
      console.log('[token-health] Membrane received RUN_UAT_REFRESH_CHECK — executing');
      this.executeUatRefresh().catch(err => {
        console.error('[token-health] RUN_UAT_REFRESH_CHECK failed:', err.message);
      });
    });
    console.log('[token-health] Membrane wired — subscribed to RUN_TOKEN_HEALTH_CHECK, RUN_UAT_REFRESH_CHECK');
  }

  stop() {
    if (!this._started) return;
    this._started = false;
  }

  isStarted() {
    return this._started;
  }

  // ── Internal: lifecycle event write (fire-and-forget) ───────────────────

  _writeLifecycleEvent({ credential_id, business_account_id, event_type, token_age_days, details }) {
    fsm.requestDBWrite({
      table: 'token_lifecycle_events',
      operation: 'insert_lifecycle_event',
      accountId: business_account_id || credential_id,
      rows: [{
        credential_id,
        business_account_id: business_account_id || null,
        event_type,
        token_age_days: token_age_days ?? null,
        details: details || {},
      }],
    });
  }

  _writeAlert({ alert_type, business_account_id, message, details, resolved }) {
    fsm.requestDBWrite({
      table: 'system_alerts',
      operation: 'insert_alert',
      accountId: business_account_id,
      rows: [{
        alert_type,
        business_account_id,
        message,
        details: details || {},
        resolved: typeof resolved === 'boolean' ? resolved : false,
      }],
    });
  }

  async _updateCredentialStatus({ credentialId, debugTokenChecked, isActive, businessAccountId }) {
    return fsm.requestDBWriteAndAwait({
      table: 'instagram_credentials',
      operation: 'update_credential_status',
      accountId: businessAccountId || credentialId,
      rows: [{ credentialId, debugTokenChecked, isActive }],
    });
  }

  _dispatchCompletion(checkType, baIds) {
    for (const baId of baIds) {
      if (!baId) continue;
      fsm.dispatch({
        type: 'CAPABILITY_HEALTH_CHECK_COMPLETED',
        checkType,
        businessAccountId: baId,
      });
    }
  }

  // ── Recovery: UAT→PAT silent recovery (absorbed from recovery-worker) ───

  /**
   * Attempt silent PAT recovery via stored UAT for a single invalid credential.
   * Absorbed from health-substrate/workers/recovery-worker.js.
   *
   * Chain: vault.uat.retrieve → vault.pat.exchange → vault.scope.detectDynamic
   *        → vault.pat.store → clearCredentialCache
   *
   * @param {{ id: string, user_id: string, business_account_id: string }} cred
   * @returns {Promise<{ success: boolean, error: string|null, newIgBusinessAccountId: string|null, newPageId: string|null, newPageName: string|null }>}
   */
  async _recoverPatViaUat(cred) {
    if (!cred || !cred.user_id || !cred.business_account_id) {
      return { success: false, error: 'cred with user_id + business_account_id is required', newIgBusinessAccountId: null, newPageId: null, newPageName: null };
    }

    try {
      const uatData = await vault.uat.retrieve({ userId: cred.user_id, businessAccountId: cred.business_account_id });
      const exchangeResult = await vault.pat.exchange({ userAccessToken: uatData.token });

      if (!exchangeResult.success || exchangeResult.requiresSelection) {
        return {
          success: false,
          error: exchangeResult.requiresSelection ? 'exchange returned requiresSelection' : (exchangeResult.error || 'exchange failed'),
          newIgBusinessAccountId: null,
          newPageId: null,
          newPageName: null,
        };
      }

      const newScope = await vault.scope.detectDynamic({
        token: exchangeResult.pageAccessToken,
        credentialId: cred.id,
      });

      await vault.pat.store({
        userId: cred.user_id,
        igBusinessAccountId: exchangeResult.igBusinessAccountId,
        pageAccessToken: exchangeResult.pageAccessToken,
        pageId: exchangeResult.pageId,
        pageName: exchangeResult.pageName,
        scope: newScope,
      });

      clearCredentialCache(cred.business_account_id);

      return {
        success: true,
        error: null,
        newIgBusinessAccountId: exchangeResult.igBusinessAccountId,
        newPageId: exchangeResult.pageId,
        newPageName: exchangeResult.pageName,
      };
    } catch (err) {
      return {
        success: false,
        error: err.message,
        newIgBusinessAccountId: null,
        newPageId: null,
        newPageName: null,
      };
    }
  }

  // ── Token Health Score ─────────────────────────────────────────────────

  /**
   * Compute a 0-100 composite health score for a credential.
   * Factors: token age, proximity to expiry, refresh eligibility, scope
   * coverage against REQUIRED_SCOPES, authorization state, detection validity.
   */
  _computeHealthScore({ cred, tokenInfo, scopes }) {
    let score = 100;

    // Token age penalty: >45 days = -20, >30 days = -10
    const age = _ageDays(cred.issued_at);
    if (age != null) {
      if (age > 45) score -= 20;
      else if (age > 30) score -= 10;
    }

    // Expiry proximity: <=3 days = -30, <=7 days = -20, <=14 days = -10
    if (cred.expires_at) {
      const daysLeft = Math.ceil((new Date(cred.expires_at) - Date.now()) / 86400000);
      if (daysLeft <= 3) score -= 30;
      else if (daysLeft <= 7) score -= 20;
      else if (daysLeft <= 14) score -= 10;
    }

    // Valid detection: invalid = -50
    if (tokenInfo && !tokenInfo.isValid) score -= 50;

    // Scope coverage: each missing required scope = -10
    const requiredScopes = igReliability.REQUIRED_SCOPES_VALUE ||
      ['instagram_basic','instagram_manage_comments','instagram_manage_insights','instagram_content_publish','pages_show_list','pages_read_engagement'];
    if (scopes && Array.isArray(scopes)) {
      const missing = requiredScopes.filter(s => !scopes.includes(s));
      score -= missing.length * 10;
    }

    return Math.max(0, Math.min(100, score));
  }

  // ── Immediate Token Recovery (Phase 8) ──────────────────────────────────
  // CK-ordered ungated token recovery for a single account. Runs
  // _recoverPatViaUat() immediately — no cadence gate, no scan loop.
  // Used by the acquisition→CK→GCK cross-kernel failure recovery flow.
  // On completion, dispatches CAPABILITY_HEALTH_CHECK_COMPLETED so the
  // GC FSM can emit TOKEN_REFRESH_RESULT back to CK.
  async executeImmediateRecovery(businessAccountId) {
    console.log(`[token-health] executeImmediateRecovery for account ${businessAccountId}`);
    const startTime = Date.now();
    const governance = this._governance;

    if (!governance || !businessAccountId) {
      console.warn('[token-health] executeImmediateRecovery: missing governance or businessAccountId');
      return { success: false, recovered: false, error: 'missing_params' };
    }

    try {
      // Read the specific credential for this business account
      const result = await governance.governedRead('db.credential', {
        query: 'scanActivePageCredentials',
        businessAccountId,
      });
      const creds = result.success ? (result.data || []) : [];

      if (creds.length === 0) {
        console.log(`[token-health] No active credential for account ${businessAccountId}`);
        this._dispatchCompletion('token_health', [businessAccountId]);
        return { success: false, recovered: false, error: 'no_credential' };
      }

      const cred = creds[0];
      const userId = cred.user_id;
      const baId = cred.business_account_id;

      // Validate token
      let token, tokenInfo;
      try {
        token = await vault.pat.retrieve({ userId, businessAccountId: baId });
        tokenInfo = await vault.uat.detect({ token, businessAccountId: baId, userId });
      } catch (err) {
        console.warn(`[token-health] Token validation failed for account ${baId}: ${err.message}`);
        this._dispatchCompletion('token_health', [baId]);
        return { success: false, recovered: false, error: 'token_validation_failed' };
      }

      if (tokenInfo && tokenInfo.isValid) {
        // Token is valid — no recovery needed. Stamp + emit envelope.
        await this._updateCredentialStatus({
          credentialId: cred.id, debugTokenChecked: true, businessAccountId: baId,
        });
        const healthyEnv = fsm.newEnvelope({ businessAccountId: baId, userId });
        healthyEnv.pat = { isDecryptable: true, source: 'health.immediate' };
        signalDispatch.emitEnvelope({ envelope: healthyEnv });
        signalDispatch.emitEvaluate({ businessAccountId: baId, userId, source: 'health.immediate' });
        this._dispatchCompletion('token_health', [baId]);
        return { success: true, recovered: false, state: 'healthy' };
      }

      // Token invalid — attempt recovery
      const recoveryResult = await this._recoverPatViaUat(cred);

      if (recoveryResult.success) {
        this._writeLifecycleEvent({
          credential_id: cred.id,
          business_account_id: baId,
          event_type: 'pat_auto_recovered',
          token_age_days: _ageDays(cred.issued_at),
          details: { source: 'token_health.immediate' },
        });
        const recoveredEnv = fsm.newEnvelope({ businessAccountId: baId, userId });
        recoveredEnv.pat = { isDecryptable: true, source: 'health.immediate_recovery' };
        signalDispatch.emitEnvelope({ envelope: recoveredEnv });
        signalDispatch.emitEvaluate({ businessAccountId: baId, userId, source: 'health.immediate_recovery' });
        await this._updateCredentialStatus({
          credentialId: cred.id, debugTokenChecked: true, businessAccountId: baId,
        });
        this._dispatchCompletion('token_health', [baId]);
        return { success: true, recovered: true, state: 'recovered' };
      }

      // Recovery failed — emit failure envelope
      const failedEnv = fsm.newEnvelope({ businessAccountId: baId, userId });
      failedEnv.pat = { isDecryptable: false, reason: 'immediate_recovery_failed' };
      signalDispatch.emitEnvelope({ envelope: failedEnv });
      console.warn(`[token-health] Immediate recovery failed for account ${baId}: ${recoveryResult.error}`);
      this._dispatchCompletion('token_health', [baId]);
      return { success: false, recovered: false, error: recoveryResult.error || 'recovery_failed' };
    } catch (err) {
      console.error('[token-health] executeImmediateRecovery fatal:', err.message);
      this._dispatchCompletion('token_health', [businessAccountId]);
      return { success: false, recovered: false, error: err.message };
    }
  }

  // ── Token Health Check (migrated from health-substrate runTokenHealthCheck) ──

  /**
   * Validate all active page tokens. For invalid ones, attempt silent PAT
   * recovery via stored UAT. Marks credentials inactive + writes auth_failure
   * alert if recovery fails.
   *
   * Migrated from health-substrate/index.js runTokenHealthCheck().
   */
  async executeTokenHealth({ scanWindowHours = 24, interCallDelayMs = 200 } = {}) {
    console.log('[token-health] executeTokenHealth starting...');
    const startTime = Date.now();
    const governance = this._governance;
    if (!governance) {
      console.warn('[token-health] Governance not available, skipping');
      return { scanned: 0, valid: 0, invalid: 0, skipped: 0, recovered: 0 };
    }

    const supabase = getSupabaseAdmin();
    if (!supabase) {
      console.warn('[token-health] Supabase not available, skipping');
      return { scanned: 0, valid: 0, invalid: 0, skipped: 0, recovered: 0 };
    }

    try {
      const result = await governance.governedRead('db.credential', { query: 'scanActivePageCredentials' });
      const creds = result.success ? (result.data || []) : [];
      const scanStats = { total: creds.length };

      logAudit({
        event_type: 'token_health_run_started',
        action: 'token_health_check',
        details: { credentials_count: scanStats.total, node_env: process.env.NODE_ENV },
      }).catch(() => {});

      if (scanStats.total === 0) {
        console.log('[token-health] No active page credentials to check');
        this._dispatchCompletion('token_health', []);
        return { scanned: 0, valid: 0, invalid: 0, skipped: 0, recovered: 0 };
      }

      let valid = 0, invalid = 0, skipped = 0, recovered = 0;
      const statusUpdates = [];

      for (const cred of creds) {
        const baId = cred.business_account_id;
        const userId = cred.user_id;

        // Gate on FSM-owned per-cred cadence
        if (!fsm._shouldCheck(baId, 'token_health')) {
          skipped++;
          await delay(interCallDelayMs);
          continue;
        }

        // Per-cred single-call: retrieve the PAT (decrypt + expiry check)
        let token;
        try {
          token = await vault.pat.retrieve({ userId, businessAccountId: baId });
        } catch (retrieveErr) {
          this._writeLifecycleEvent({
            credential_id: cred.id,
            business_account_id: baId,
            event_type: 'pat_invalid',
            token_age_days: _ageDays(cred.issued_at),
            details: { source: 'token_health', error: retrieveErr.message },
          });
          skipped++;
          await delay(interCallDelayMs);
          continue;
        }

        // Per-cred single-call: validate via /debug_token
        let tokenInfo;
        try {
          tokenInfo = await vault.uat.detect({ token, businessAccountId: baId, userId });
        } catch (apiErr) {
          this._writeLifecycleEvent({
            credential_id: cred.id,
            business_account_id: baId,
            event_type: 'pat_invalid',
            token_age_days: _ageDays(cred.issued_at),
            details: { source: 'token_health', error: apiErr.message },
          });
          skipped++;
          await delay(interCallDelayMs);
          continue;
        }

        // Compute health score using the reliability substrate's token lifecycle
        const tokenLifecycle = igReliability._analyzeTokenLifecycle(
          { tokenMetadata: { issuedAt: cred.issued_at, expiresAt: cred.expires_at, isLongLived: true, scopes: tokenInfo?.scopes || [], refreshHistory: [] } },
          { category: tokenInfo?.isValid ? 'UNKNOWN' : 'TOKEN_INVALID' }
        );
        const healthScore = this._computeHealthScore({ cred, tokenInfo, scopes: tokenInfo?.scopes || [] });

        if (tokenInfo && tokenInfo.isValid) {
          // VALID
          statusUpdates.push({
            credentialId: cred.id,
            debugTokenChecked: true,
            businessAccountId: baId,
          });
          this._writeLifecycleEvent({
            credential_id: cred.id,
            business_account_id: baId,
            event_type: 'pat_validated',
            token_age_days: _ageDays(cred.issued_at),
            details: { source: 'token_health', healthScore, tokenLifecycle },
          });
          valid++;
        } else {
          // INVALID — attempt recovery via stored UAT
          const recoveryResult = await this._recoverPatViaUat(cred);

          if (recoveryResult.success) {
            this._writeLifecycleEvent({
              credential_id: cred.id,
              business_account_id: baId,
              event_type: 'pat_auto_recovered',
              token_age_days: _ageDays(cred.issued_at),
              details: { source: 'token_health' },
            });
            this._writeAlert({
              alert_type: 'pat_auto_recovered',
              business_account_id: baId,
              message: 'Your Instagram access token was automatically recovered using stored credentials.',
              details: { user_id: userId, old_credential_id: cred.id, source: 'token_health' },
              resolved: true,
            });
            console.log(`[token-health] PAT auto-recovered for cred ${cred.id} via stored UAT`);

            const recoveredEnv = fsm.newEnvelope({ businessAccountId: baId, userId });
            recoveredEnv.pat = { isDecryptable: true, source: 'health.recovery' };
            signalDispatch.emitEnvelope({ envelope: recoveredEnv });
            signalDispatch.emitEvaluate({ businessAccountId: baId, userId, source: 'health.recovery' });

            recovered++;
            invalid++;
            statusUpdates.push({
              credentialId: cred.id,
              debugTokenChecked: true,
              businessAccountId: baId,
            });
          } else {
            // Recovery failed
            statusUpdates.push({
              credentialId: cred.id,
              isActive: false,
              businessAccountId: baId,
            });
            this._writeLifecycleEvent({
              credential_id: cred.id,
              business_account_id: baId,
              event_type: 'pat_recovery_failed',
              token_age_days: _ageDays(cred.issued_at),
              details: { source: 'token_health', error: 'uat_unavailable_or_exchange_failed' },
            });
            this._writeAlert({
              alert_type: 'auth_failure',
              business_account_id: baId,
              message: 'Instagram access token is no longer valid. Please reconnect your account.',
              details: { user_id: userId, credential_id: cred.id, source: 'token_health' },
              resolved: false,
            });

            const failedEnv = fsm.newEnvelope({ businessAccountId: baId, userId });
            failedEnv.pat = { isDecryptable: false, reason: 'recovery_failed' };
            signalDispatch.emitEnvelope({ envelope: failedEnv });

            console.warn(`[token-health] Token invalid for cred ${cred.id} (user ${userId}), marked inactive`);
            invalid++;
          }
        }

        await delay(interCallDelayMs);
      }

      // Batch dispatch credential status stamps
      if (statusUpdates.length > 0) {
        await Promise.all(statusUpdates.map(s => this._updateCredentialStatus(s)));
      }

      const finalStats = { scanned: scanStats.total, valid, invalid, skipped, recovered };

      console.log(`[token-health] executeTokenHealth complete — valid: ${finalStats.valid}, invalid: ${finalStats.invalid}, skipped: ${finalStats.skipped}, recovered: ${finalStats.recovered}`);
      logAudit({
        event_type: 'token_health_run_completed',
        action: 'token_health_check',
        details: { ...finalStats, duration_ms: Date.now() - startTime },
        success: finalStats.invalid - finalStats.recovered === 0,
      }).catch(() => {});

      this._dispatchCompletion('token_health', creds.map(c => c.business_account_id).filter(Boolean));

      return finalStats;
    } catch (err) {
      console.error('[token-health] executeTokenHealth fatal:', err.message);
      logAudit({
        event_type: 'token_health_run_error',
        action: 'token_health_check',
        details: { error: err.message, duration_ms: Date.now() - startTime },
        success: false,
      }).catch(() => {});
      throw err;
    }
  }

  // ── UAT Refresh Check (migrated from health-substrate runUATRefreshCheck) ──

  /**
   * Proactive UAT refresh: find UATs expiring within windowDays, attempt refresh.
   * Then scan data_access_expires_at within dataAccessWindowDays, write dedup'd warnings.
   *
   * Migrated from health-substrate/index.js runUATRefreshCheck().
   */
  async executeUatRefresh({ windowDays = 14, interCallDelayMs = 1000, dataAccessWindowDays = 30, businessAccountId = null } = {}) {
    console.log('[token-health] executeUatRefresh starting...');
    const governance = this._governance;
    if (!governance) {
      console.warn('[token-health] Governance not available, skipping');
      return { refreshed: 0, refreshFailed: 0, dataAccessWarnings: 0 };
    }

    const supabase = getSupabaseAdmin();
    if (!supabase) {
      console.warn('[token-health] Supabase not available, skipping');
      return { refreshed: 0, refreshFailed: 0, dataAccessWarnings: 0 };
    }

    try {
      // Phase 1: 14-day expiring UAT scan + refresh
      const refreshResult = await governance.governedRead('db.credential', { query: 'scanExpiringUATs', windowDays, businessAccountId });
      const uats = refreshResult.success ? (refreshResult.data || []) : [];
      const refreshScanStats = { total: uats.length };

      let refreshed = 0, failed = 0;

      if (refreshScanStats.total === 0) {
        console.log('[token-health] No UATs need refresh');
      } else {
        console.log(`[token-health] Found ${refreshScanStats.total} UAT(s) expiring within ${windowDays} days`);

        for (const uat of uats) {
          const baId = uat.business_account_id;
          const userId = uat.user_id;
          const daysLeft = Math.ceil((new Date(uat.expires_at) - Date.now()) / (24 * 60 * 60 * 1000));

          // Use token lifecycle analysis from substrate for refresh eligibility
          const tokenLifecycle = igReliability._analyzeTokenLifecycle(
            { tokenMetadata: { issuedAt: uat.issued_at, expiresAt: uat.expires_at, isLongLived: true, scopes: uat.scope || [], refreshHistory: uat.refresh_history || [] } },
            { category: 'UNKNOWN' }
          );

          if (!tokenLifecycle.refreshEligible) {
            this._writeAlert({
              alert_type: 'uat_expiry_warning',
              business_account_id: baId,
              message: `Your access token expires in ${daysLeft} days and refresh budget exhausted. Please reconnect your Instagram account.`,
              details: { expires_at: uat.expires_at, days_remaining: daysLeft, reason: 'refresh_budget_exhausted' },
              resolved: false,
            });
            failed++;
            await delay(interCallDelayMs);
            continue;
          }

          let refreshOutcome;
          try {
            refreshOutcome = await vault.uat.refresh({ userId, businessAccountId: baId });
          } catch (refreshErr) {
            refreshOutcome = { success: false, error: refreshErr.message, expiresAt: null };
          }

          if (refreshOutcome && refreshOutcome.success) {
            this._writeAlert({
              alert_type: 'uat_auto_refreshed',
              business_account_id: baId,
              message: `Your access token was automatically refreshed. New expiry: ${refreshOutcome.expiresAt}`,
              details: { old_expires_at: uat.expires_at, new_expires_at: refreshOutcome.expiresAt, days_remaining_at_refresh: daysLeft },
              resolved: true,
            });
            this._writeLifecycleEvent({
              credential_id: uat.id,
              business_account_id: baId,
              event_type: 'uat_refreshed',
              details: { source: 'token_health', old_expires_at: uat.expires_at, new_expires_at: refreshOutcome.expiresAt, days_remaining: daysLeft },
            });
            refreshed++;
          } else {
            this._writeAlert({
              alert_type: 'uat_expiry_warning',
              business_account_id: baId,
              message: `Your access token expires in ${daysLeft} days and auto-refresh failed. Please reconnect your Instagram account.`,
              details: { expires_at: uat.expires_at, error: (refreshOutcome && refreshOutcome.error) || 'unknown', days_remaining: daysLeft },
              resolved: false,
            });
            this._writeLifecycleEvent({
              credential_id: uat.id,
              business_account_id: baId,
              event_type: 'uat_refresh_failed',
              details: { source: 'token_health', error: (refreshOutcome && refreshOutcome.error) || 'unknown', expires_at: uat.expires_at, days_remaining: daysLeft },
            });

            const refreshFailEnv = fsm.newEnvelope({ businessAccountId: baId, userId });
            refreshFailEnv.uat = { isDecryptable: false, reason: 'uat_refresh_failed', daysRemaining: daysLeft };
            signalDispatch.emitEnvelope({ envelope: refreshFailEnv });
            failed++;
          }
          await delay(interCallDelayMs);
        }
      }

      // Phase 2: 30-day data_access_expires_at scan + dedup'd warning
      const daeResult = await governance.governedRead('db.credential', { query: 'scanDataAccessExpiry', windowDays: dataAccessWindowDays, businessAccountId });
      const expiringDataAccess = daeResult.success ? (daeResult.data || []) : [];

      const candidates = [];
      let daeDeduped = 0;

      for (const uat of expiringDataAccess) {
        const daysLeft = Math.ceil((new Date(uat.data_access_expires_at) - Date.now()) / (24 * 60 * 60 * 1000));

        const dedupResult = await governance.governedRead('db.alerts', {
          query: 'checkExistingWarning',
          businessAccountId: uat.business_account_id,
          alertType: 'data_access_expiry_warning',
        });
        if (dedupResult.success && dedupResult.data) {
          daeDeduped++;
          continue;
        }

        candidates.push({ uat, daysLeft });
      }

      for (const c of candidates) {
        const { uat, daysLeft } = c;
        this._writeAlert({
          alert_type: 'data_access_expiry_warning',
          business_account_id: uat.business_account_id,
          message: `Instagram data access expires in ${daysLeft} day${daysLeft !== 1 ? 's' : ''}. Reconnect your account to renew access to messages and comments.`,
          details: { data_access_expires_at: uat.data_access_expires_at, days_remaining: daysLeft, note: 'Cannot be refreshed via fb_exchange_token — requires fresh OAuth consent' },
          resolved: false,
        });
        this._writeLifecycleEvent({
          credential_id: uat.id,
          business_account_id: uat.business_account_id,
          event_type: 'data_access_expiry_warning',
          details: { source: 'token_health', data_access_expires_at: uat.data_access_expires_at, days_remaining: daysLeft },
        });
      }

      console.log(`[token-health] executeUatRefresh complete — refreshed: ${refreshed}, failed: ${failed}, daeWarnings: ${candidates.length}, daeDeduped: ${daeDeduped}`);

      this._dispatchCompletion('uat_refresh', uats.map(u => u.business_account_id).filter(Boolean));

      return { refreshed, refreshFailed: failed, dataAccessWarnings: candidates.length };
    } catch (err) {
      console.error('[token-health] executeUatRefresh fatal:', err.message);
      throw err;
    }
  }
}

module.exports = TokenHealthWorker;
