// graph-capability-kernel/substrates/health-substrate/index.js
// Health substrate façade — orchestrator for the health workers.
//
// Constitutional role:
//   - credential scan: governedRead('db.credential', { query: 'scanActivePageCredentials' })
//     → CK → persist-telemetry FSM → read-credential-worker (batch SELECT, constitutional)
//   - UAT expiry scan: governedRead('db.credential', { query: 'scanExpiringUATs' })
//     → CK → persist-telemetry FSM → read-credential-worker (date-filtered SELECT)
//   - data-access scan + dedup: governedRead('db.credential', { query: 'scanDataAccessExpiry' })
//     → CK → persist-telemetry FSM → read-credential-worker
//     + governedRead('db.alerts', { query: 'checkExistingWarning' }) per cred for dedup
//   - recovery-worker: UAT→PAT side effect (only on INVALID)
//
// This façade owns:
//   - composing the workers
//   - 200ms rate-limit pacing between /debug_token calls
//   - 1000ms rate-limit pacing between UAT refresh attempts
//   - run-level audit log events (started / completed / error)
//   - signal dispatch (emitEnvelope, emitEvaluate) post-worker
//
// This façade does NOT own:
//   - DB reads (dispatched through CK.governedRead → persist-telemetry FSM → reading-substrate)
//   - DB writes (dispatched through fsm.requestDBWrite() → CK → persist-telemetry FSM → writer)
//   - /debug_token calls (vault.uat.detect — single-call worker)
//   - recovery I/O (recovery-worker)
//   - refresh I/O (vault.uat.refresh — single-call worker)
//   - alert dedup logic (delegated to read-alerts-worker via governedRead)
//
// Constitutional wiring:
//   All DB reads flow through: CK.governedRead() → persist-telemetry FSM → reading-substrate → worker.
//   All DB writes flow through: fsm.requestDBWrite() → CK.dispatch(DB_WRITE_REQUESTED) →
//   persist-telemetry FSM → writer. Fire-and-forget, matches existing
//   best-effort semantics (no retry, console.warn on failure handled by writer).

const { getSupabaseAdmin, logAudit } = require('../../../config/supabase');
const signalDispatch = require('../vault/signal-dispatch');
const fsm = require('../../fsm');
const vault = require('../vault');

const RecoveryWorker = require('./workers/recovery-worker');

// Private delay — local to this substrate, no cross-import from helpers
function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ── Public lifecycle ─────────────────────────────────────────────────────────
// ── Public lifecycle ─────────────────────────────────────────────────────────

let _started = false;
let _governance = null;

function start(governance) {
  if (_started) return;
  _started = true;
  _governance = governance;

  governance.subscribeAction('RUN_TOKEN_HEALTH_CHECK', (action) => {
    console.log('[health] Membrane received RUN_TOKEN_HEALTH_CHECK — executing');
    runTokenHealthCheck().catch(err => {
      console.error('[health] RUN_TOKEN_HEALTH_CHECK failed:', err.message);
    });
  });
  governance.subscribeAction('RUN_UAT_REFRESH_CHECK', (action) => {
    console.log('[health] Membrane received RUN_UAT_REFRESH_CHECK — executing');
    runUATRefreshCheck().catch(err => {
      console.error('[health] RUN_UAT_REFRESH_CHECK failed:', err.message);
    });
  });
  console.log('[health] Membrane wired — subscribed to RUN_TOKEN_HEALTH_CHECK, RUN_UAT_REFRESH_CHECK');
}

function stop() {
  if (!_started) return;
  _started = false;
}

function isStarted() {
  return _started;
}

// ── Internal helpers ─────────────────────────────────────────────────────────

function _ageDays(issuedAt) {
  if (!issuedAt) return null;
  return Math.floor((Date.now() - new Date(issuedAt).getTime()) / 86400000);
}

/**
 * Dispatch a lifecycle event write through the constitutional flow.
 * Fire-and-forget — matches existing best-effort semantics.
 */
function _writeLifecycleEvent({ credential_id, business_account_id, event_type, token_age_days, details }) {
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

/**
 * Dispatch an alert write through the constitutional flow.
 * Fire-and-forget — matches existing best-effort semantics.
 */
function _writeAlert({ alert_type, business_account_id, message, details, resolved }) {
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

/**
 * Stamp credential debug_token_checked_at or set is_active.
 * Uses requestDBWriteAndAwait — operational write. The cadence loop awaits the
 * acknowledgement so the stamp lands before the loop proceeds to the next credential.
 * This prevents re-scanning credentials whose stamps haven't yet committed.
 */
async function _updateCredentialStatus({ credentialId, debugTokenChecked, isActive, businessAccountId }) {
  return fsm.requestDBWriteAndAwait({
    table: 'instagram_credentials',
    operation: 'update_credential_status',
    accountId: businessAccountId || credentialId,
    rows: [{ credentialId, debugTokenChecked, isActive }],
  });
}

/**
 * Batch-dispatch CAPABILITY_HEALTH_CHECK_COMPLETED per credential.
 * The FSM stamps per-cred cadence timestamps so the next periodic tick
 * doesn't redundantly re-trigger for credentials that just completed.
 */
function _dispatchCompletion(checkType, baIds) {
  for (const baId of baIds) {
    if (!baId) continue;
    fsm.dispatch({
      type: 'CAPABILITY_HEALTH_CHECK_COMPLETED',
      checkType,
      businessAccountId: baId,
    });
  }
}

// ── Public operations ───────────────────────────────────────────────────────

/**
 * Validate all active page tokens. For invalid ones, attempt silent PAT recovery
 * via stored UAT. Marks credentials inactive + writes auth_failure alert if
 * recovery fails.
 */
async function runTokenHealthCheck({ scanWindowHours = 24, interCallDelayMs = 200 } = {}) {
  console.log('[health] runTokenHealthCheck starting...');
  const startTime = Date.now();
  const supabase = getSupabaseAdmin();
  if (!supabase) {
    console.warn('[health] Supabase not available, skipping');
    return { scanned: 0, valid: 0, invalid: 0, skipped: 0, recovered: 0 };
  }

  try {
    const result = await _governance.governedRead('db.credential', { query: 'scanActivePageCredentials' });
    const creds = result.success ? (result.data || []) : [];
    const scanStats = { total: creds.length };

    logAudit({
      event_type: 'token_health_run_started',
      action: 'token_health_check',
      details: { credentials_count: scanStats.total, node_env: process.env.NODE_ENV },
    }).catch(() => {});

    if (scanStats.total === 0) {
      console.log('[health] No active page credentials to check');
      _dispatchCompletion('token_health', []);
      return { scanned: 0, valid: 0, invalid: 0, skipped: 0, recovered: 0 };
    }

    let valid = 0, invalid = 0, skipped = 0, recovered = 0;
    const recoveryWorker = new RecoveryWorker();
    const statusUpdates = [];  // batch: collect all credential status stamps

    for (const cred of creds) {
      const baId = cred.business_account_id;
      const userId = cred.user_id;

      // Phase D: gate on FSM-owned per-cred cadence. The worker no longer
      // owns the skip-if-fresh policy; the FSM does. This replaces the
      // worker's old SKIPPED_FRESH classification.
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
        _writeLifecycleEvent({
          credential_id: cred.id,
          business_account_id: baId,
          event_type: 'pat_invalid',
          token_age_days: _ageDays(cred.issued_at),
          details: { source: 'daily_health_check', error: retrieveErr.message },
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
        _writeLifecycleEvent({
          credential_id: cred.id,
          business_account_id: baId,
          event_type: 'pat_invalid',
          token_age_days: _ageDays(cred.issued_at),
          details: { source: 'daily_health_check', error: apiErr.message },
        });
        skipped++;
        await delay(interCallDelayMs);
        continue;
      }

      if (tokenInfo && tokenInfo.isValid) {
        // VALID — collect stamp for batch dispatch
        statusUpdates.push({
          credentialId: cred.id,
          debugTokenChecked: true,
          businessAccountId: baId,
        });
        _writeLifecycleEvent({
          credential_id: cred.id,
          business_account_id: baId,
          event_type: 'pat_validated',
          token_age_days: _ageDays(cred.issued_at),
          details: { source: 'daily_health_check' },
        });
        valid++;
      } else {
        // INVALID — attempt recovery
        const result = await recoveryWorker.execute({ cred });

        if (result.success) {
          _writeLifecycleEvent({
            credential_id: cred.id,
            business_account_id: baId,
            event_type: 'pat_auto_recovered',
            token_age_days: _ageDays(cred.issued_at),
            details: { source: 'daily_health_check' },
          });
          _writeAlert({
            alert_type: 'pat_auto_recovered',
            business_account_id: baId,
            message: 'Your Instagram access token was automatically recovered using stored credentials.',
            details: { user_id: userId, old_credential_id: cred.id, source: 'token_health_check' },
            resolved: true,
          });

          console.log(`[health] PAT auto-recovered for cred ${cred.id} via stored UAT`);

          // Emit observation envelope — recovered PAT is decryptable
          const recoveredEnv = fsm.newEnvelope({ businessAccountId: baId, userId });
          recoveredEnv.pat = { isDecryptable: true, source: 'health.recovery' };
          signalDispatch.emitEnvelope({ envelope: recoveredEnv });
          signalDispatch.emitEvaluate({
            businessAccountId: baId,
            userId,
            source: 'health.recovery',
          });

          recovered++;
          invalid++;
          // Stamp recovered credential so next cadence tick doesn't re-process it
          statusUpdates.push({
            credentialId: cred.id,
            debugTokenChecked: true,
            businessAccountId: baId,
          });
        } else {
          // Recovery failed — collect stamp for batch dispatch
          statusUpdates.push({
            credentialId: cred.id,
            isActive: false,
            businessAccountId: baId,
          });
          _writeLifecycleEvent({
            credential_id: cred.id,
            business_account_id: baId,
            event_type: 'pat_recovery_failed',
            token_age_days: _ageDays(cred.issued_at),
            details: { source: 'daily_health_check', error: 'uat_unavailable_or_exchange_failed' },
          });
          _writeAlert({
            alert_type: 'auth_failure',
            business_account_id: baId,
            message: 'Instagram access token is no longer valid. Please reconnect your account.',
            details: { user_id: userId, credential_id: cred.id, source: 'token_health_check' },
            resolved: false,
          });

          const failedEnv = fsm.newEnvelope({ businessAccountId: baId, userId });
          failedEnv.pat = { isDecryptable: false, reason: 'recovery_failed' };
          signalDispatch.emitEnvelope({ envelope: failedEnv });

          console.warn(`[health] Token invalid for cred ${cred.id} (user ${userId}), marked inactive`);
          invalid++;
        }
      }

      await delay(interCallDelayMs);
    }

    // Batch dispatch: all credential status stamps fire in parallel.
    // This reduces N sequential round-trips (CK→FSM→writer→ACK) to one
    // parallel wave. Total time = max(single write latency), not sum.
    if (statusUpdates.length > 0) {
      await Promise.all(statusUpdates.map(s => _updateCredentialStatus(s)));
    }

    const finalStats = {
      scanned: scanStats.total,
      valid,
      invalid,
      skipped,
      recovered,
    };

    console.log(`[health] runTokenHealthCheck complete — valid: ${finalStats.valid}, invalid: ${finalStats.invalid}, skipped: ${finalStats.skipped}, recovered: ${finalStats.recovered}`);
    logAudit({
      event_type: 'token_health_run_completed',
      action: 'token_health_check',
      details: { ...finalStats, duration_ms: Date.now() - startTime },
      success: finalStats.invalid - finalStats.recovered === 0,
    }).catch(() => {});

    // Phase C: signal FSM per-cred cadence completion
    _dispatchCompletion('token_health', creds.map(c => c.business_account_id).filter(Boolean));

    return finalStats;
  } catch (err) {
    console.error('[health] runTokenHealthCheck fatal:', err.message);
    logAudit({
      event_type: 'token_health_run_error',
      action: 'token_health_check',
      details: { error: err.message, duration_ms: Date.now() - startTime },
      success: false,
    }).catch(() => {});
    throw err;
  }
}

/**
 * Proactive UAT refresh: find UATs expiring within 14 days, attempt refresh.
 * Then scan data_access_expires_at within 30 days, write dedup'd warning alerts.
 */
async function runUATRefreshCheck({ windowDays = 14, interCallDelayMs = 1000, dataAccessWindowDays = 30, businessAccountId = null } = {}) {
  console.log('[health] runUATRefreshCheck starting...');
  const supabase = getSupabaseAdmin();
  if (!supabase) {
    console.warn('[health] Supabase not available, skipping');
    return { refreshed: 0, refreshFailed: 0, dataAccessWarnings: 0 };
  }

  try {
    // ── Phase 1: 14-day expiring UAT scan + refresh ──
    const refreshResult = await _governance.governedRead('db.credential', { query: 'scanExpiringUATs', windowDays, businessAccountId });
    const uats = refreshResult.success ? (refreshResult.data || []) : [];
    const refreshScanStats = { total: uats.length };

    let refreshed = 0;
    let failed = 0;

    if (refreshScanStats.total === 0) {
      console.log('[health] No UATs need refresh');
    } else {
      console.log(`[health] Found ${refreshScanStats.total} UAT(s) expiring within ${windowDays} days`);

      for (const uat of uats) {
        const baId = uat.business_account_id;
        const userId = uat.user_id;
        const daysLeft = Math.ceil((new Date(uat.expires_at) - Date.now()) / (24 * 60 * 60 * 1000));

        // Per-cred single-call: vault.uat.refresh is the canonical cross-substrate
        // façade call. It internally chains retrieve → exchange-refresh → detect →
        // store; the façade here treats it as a single bounded user-intent
        // ("refresh this UAT") with one outcome.
        let refreshResult;
        try {
          refreshResult = await vault.uat.refresh({ userId, businessAccountId: baId });
        } catch (refreshErr) {
          refreshResult = { success: false, error: refreshErr.message, expiresAt: null };
        }

        if (refreshResult && refreshResult.success) {
          _writeAlert({
            alert_type: 'uat_auto_refreshed',
            business_account_id: baId,
            message: `Your access token was automatically refreshed. New expiry: ${refreshResult.expiresAt}`,
            details: {
              old_expires_at: uat.expires_at,
              new_expires_at: refreshResult.expiresAt,
              days_remaining_at_refresh: daysLeft,
            },
            resolved: true,
          });
          _writeLifecycleEvent({
            credential_id: uat.id,
            business_account_id: baId,
            event_type: 'uat_refreshed',
            details: { source: 'uat_refresh_check', old_expires_at: uat.expires_at, new_expires_at: refreshResult.expiresAt, days_remaining: daysLeft },
          });
          refreshed++;
        } else {
          _writeAlert({
            alert_type: 'uat_expiry_warning',
            business_account_id: baId,
            message: `Your access token expires in ${daysLeft} days and auto-refresh failed. Please reconnect your Instagram account.`,
            details: { expires_at: uat.expires_at, error: (refreshResult && refreshResult.error) || 'unknown', days_remaining: daysLeft },
            resolved: false,
          });
          _writeLifecycleEvent({
            credential_id: uat.id,
            business_account_id: baId,
            event_type: 'uat_refresh_failed',
            details: { source: 'uat_refresh_check', error: (refreshResult && refreshResult.error) || 'unknown', expires_at: uat.expires_at, days_remaining: daysLeft },
          });

          // Emit observation envelope — refresh failed
          const refreshFailEnv = fsm.newEnvelope({ businessAccountId: baId, userId });
          refreshFailEnv.uat = { isDecryptable: false, reason: 'uat_refresh_failed', daysRemaining: daysLeft };
          signalDispatch.emitEnvelope({ envelope: refreshFailEnv });
          failed++;
        }
        await delay(interCallDelayMs);
      }
    }

    // ── Phase 2: 30-day data_access_expires_at scan + dedup'd warning ──
    const daeResult = await _governance.governedRead('db.credential', { query: 'scanDataAccessExpiry', windowDays: dataAccessWindowDays, businessAccountId });
    const expiringDataAccess = daeResult.success ? (daeResult.data || []) : [];

    const candidates = [];
    let daeDeduped = 0;

    for (const uat of expiringDataAccess) {
      const daysLeft = Math.ceil((new Date(uat.data_access_expires_at) - Date.now()) / (24 * 60 * 60 * 1000));

      // Dedup: skip if an unresolved data_access_expiry_warning already exists
      const dedupResult = await _governance.governedRead('db.alerts', {
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

    const dedupStats = { alertable: candidates.length, deduped: daeDeduped, total: expiringDataAccess.length };

    for (const c of candidates) {
      const { uat, daysLeft } = c;

      _writeAlert({
        alert_type: 'data_access_expiry_warning',
        business_account_id: uat.business_account_id,
        message: `Instagram data access expires in ${daysLeft} day${daysLeft !== 1 ? 's' : ''}. Reconnect your account to renew access to messages and comments.`,
        details: {
          data_access_expires_at: uat.data_access_expires_at,
          days_remaining: daysLeft,
          note: 'Cannot be refreshed via fb_exchange_token — requires fresh OAuth consent',
        },
        resolved: false,
      });
      _writeLifecycleEvent({
        credential_id: uat.id,
        business_account_id: uat.business_account_id,
        event_type: 'data_access_expiry_warning',
        details: { source: 'uat_refresh_check', data_access_expires_at: uat.data_access_expires_at, days_remaining: daysLeft },
      });

      // Emit observation envelope — data access expiring soon
      const daeEnv = fsm.newEnvelope({ businessAccountId: uat.business_account_id, userId: uat.user_id });
      daeEnv.uat = {
        isDecryptable: true,
        dataAccessExpiresAt: uat.data_access_expires_at,
        daysRemaining: daysLeft,
        reason: 'data_access_expiry_warning',
        reliabilityImpaired: true,
      };
      signalDispatch.emitEnvelope({ envelope: daeEnv });

      console.log(`[health] data_access_expiry_warning created for account ${uat.business_account_id} (${daysLeft} days left)`);
    }

    const finalStats = {
      refreshed,
      refreshFailed: failed,
      dataAccessWarnings: dedupStats.alertable,
      dataAccessDeduped: dedupStats.deduped,
    };

    console.log(`[health] runUATRefreshCheck complete — refreshed: ${finalStats.refreshed}, failed: ${finalStats.refreshFailed}, data_access warnings: ${finalStats.dataAccessWarnings} (deduped: ${finalStats.dataAccessDeduped})`);

    // Phase C: signal FSM per-cred cadence completion for both phases.
    const uatRefreshBaIds = uats.map(u => u.business_account_id).filter(Boolean);
    const dataAccessBaIds = candidates.map(c => c.uat && c.uat.business_account_id).filter(Boolean);
    _dispatchCompletion('uat_refresh', uatRefreshBaIds);
    _dispatchCompletion('data_access_expiry', dataAccessBaIds);

    return finalStats;
  } catch (err) {
    console.error('[health] runUATRefreshCheck fatal:', err.message);
    throw err;
  }
}

module.exports = {
  start,
  stop,
  isStarted,
  runTokenHealthCheck,
  runUATRefreshCheck,
};
