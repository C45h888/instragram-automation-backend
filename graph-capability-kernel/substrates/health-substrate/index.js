// graph-capability-kernel/substrates/health-substrate/index.js
// Health substrate façade — orchestrator for the four health workers.
//
// Constitutional role:
//   - scan-credentials-worker: detect + classify
//   - recovery-worker: UAT→PAT side effect (only on INVALID)
//   - uat-refresh-worker: 14d expiring-UAT scan + refresh
//   - data-access-expiry-worker: 30d data_access_expires_at scan + dedup
//
// This façade owns:
//   - composing the four workers
//   - the per-cred alert + token_lifecycle_events writes (left inline per "steady
//     migration" directive — alert dedup is the explicit deferred item, see
//     migration report §7a)
//   - 200ms rate-limit pacing between /debug_token calls
//   - 1000ms rate-limit pacing between UAT refresh attempts
//   - run-level audit log events (started / completed / error)
//
// This façade does NOT own:
//   - /debug_token calls (scan-credentials-worker)
//   - recovery I/O (recovery-worker)
//   - refresh I/O (uat-refresh-worker)
//   - dedup pre-check (data-access-expiry-worker)
//
// Constitutional wiring:
//   - Valid classifications → no signal (vault.pat.retrieve / vault.uat.detect
//     already emit CAPABILITY_EVALUATE via their own signal-dispatch).
//   - Recovery success → recovery-worker emits CAPABILITY_EVALUATE explicitly
//     (preserves the legacy "post-recovery FSM evaluation cycle" behavior).
//   - Recovery failure → no signal from substrate (alert + event row inserted;
//     FSM observes the corresponding capability failure via downstream routes
//     when user attempts an op and verdict-gate sees is_active=false).
//   - UAT refresh success → vault.uat.refresh emits TOKEN_REFRESHED internally
//     (legacy never emitted this on this path — bug fixed by migration).
//   - UAT refresh failure → no signal from substrate.
//
// Future pass: server.js → CK → graph-capability-FSM boot choreography. Out of
//   scope for this migration. The boot wrapper in services/sync/index.js
//   continues to call this façade directly.

const { getSupabaseAdmin, logAudit, fireAndForgetInsert } = require('../../../config/supabase');
const triggerBridge = require('../graph-capability/trigger-bridge');
const signalDispatch = require('../vault/signal-dispatch');

const ScanCredentialsWorker = require('./workers/scan-credentials-worker');
const RecoveryWorker = require('./workers/recovery-worker');
const UatRefreshWorker = require('./workers/uat-refresh-worker');
const DataAccessExpiryWorker = require('./workers/data-access-expiry-worker');

// Private delay — local to this substrate, no cross-import from helpers
function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ── Public lifecycle ─────────────────────────────────────────────────────────

let _started = false;

function start() {
  if (_started) return;
  _started = true;
  console.log('[health] Substrate started — workers armed (scan / recovery / refresh / data-access-expiry)');
}

function stop() {
  if (!_started) return;
  _started = false;
  console.log('[health] Substrate stopped');
}

function isStarted() {
  return _started;
}

// ── Internal: per-cred token_lifecycle_events writer ────────────────────────

async function _writeLifecycleEvent(supabase, { credential_id, business_account_id, event_type, token_age_days, details }) {
  const { error } = await fireAndForgetInsert(supabase.from('token_lifecycle_events').insert({
    credential_id,
    business_account_id,
    event_type,
    token_age_days: token_age_days ?? null,
    details: details || {},
  }));
  if (error) console.warn(`[health] ${event_type} insert failed:`, error.message);
}

function _ageDays(issuedAt) {
  if (!issuedAt) return null;
  return Math.floor((Date.now() - new Date(issuedAt).getTime()) / 86400000);
}

// ── Public operations ───────────────────────────────────────────────────────

/**
 * Validate all active page tokens. For invalid ones, attempt silent PAT recovery
 * via stored UAT. Marks credentials inactive + writes auth_failure alert if
 * recovery fails.
 *
 * Mirrors legacy services/sync/token-health.js → runTokenHealthCheck().
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
    const scanWorker = new ScanCredentialsWorker();
    const { scanned, stats } = await scanWorker.execute({ scanWindowHours });

    logAudit({
      event_type: 'token_health_run_started',
      action: 'token_health_check',
      details: { credentials_count: stats.total, node_env: process.env.NODE_ENV },
    }).catch(() => {});

    if (stats.total === 0) {
      console.log('[health] No active page credentials to check');
      return { scanned: 0, valid: 0, invalid: 0, skipped: 0, recovered: 0 };
    }

    let recovered = 0;
    const recoveryWorker = new RecoveryWorker();
    const tb = triggerBridge; // optional override via process env not needed; default bridge

    for (const entry of scanned) {
      const { cred, tokenInfo, classification, skipReason } = entry;

      if (classification === 'VALID') {
        // Stamp the check time so next 24h skips
        await supabase
          .from('instagram_credentials')
          .update({ debug_token_checked_at: new Date().toISOString() })
          .eq('id', cred.id);

        await _writeLifecycleEvent(supabase, {
          credential_id: cred.id,
          business_account_id: cred.business_account_id,
          event_type: 'pat_validated',
          token_age_days: _ageDays(cred.issued_at),
          details: { source: 'daily_health_check' },
        });
        await delay(interCallDelayMs);
        continue;
      }

      if (classification === 'INVALID') {
        // Attempt recovery
        const result = await recoveryWorker.execute({ cred, triggerBridge: tb });

        if (result.success) {
          await _writeLifecycleEvent(supabase, {
            credential_id: cred.id,
            business_account_id: cred.business_account_id,
            event_type: 'pat_auto_recovered',
            token_age_days: _ageDays(cred.issued_at),
            details: { source: 'daily_health_check' },
          });

          const { error: alertErr } = await fireAndForgetInsert(supabase.from('system_alerts').insert({
            alert_type: 'pat_auto_recovered',
            business_account_id: cred.business_account_id,
            message: 'Your Instagram access token was automatically recovered using stored credentials.',
            details: { user_id: cred.user_id, old_credential_id: cred.id, source: 'token_health_check' },
            resolved: true,
          }));
          if (alertErr) console.warn('[health] pat_auto_recovered alert insert failed:', alertErr.message);

          console.log(`[health] PAT auto-recovered for cred ${cred.id} via stored UAT`);
          recovered++;
        } else {
          // Recovery failed — mark inactive, alert user, log failure
          await supabase
            .from('instagram_credentials')
            .update({ is_active: false, updated_at: new Date().toISOString() })
            .eq('id', cred.id);

          await _writeLifecycleEvent(supabase, {
            credential_id: cred.id,
            business_account_id: cred.business_account_id,
            event_type: 'pat_recovery_failed',
            token_age_days: _ageDays(cred.issued_at),
            details: { source: 'daily_health_check', error: 'uat_unavailable_or_exchange_failed' },
          });

          const { error: alertErr } = await fireAndForgetInsert(supabase.from('system_alerts').insert({
            alert_type: 'auth_failure',
            business_account_id: cred.business_account_id,
            message: 'Instagram access token is no longer valid. Please reconnect your account.',
            details: { user_id: cred.user_id, credential_id: cred.id, source: 'token_health_check' },
            resolved: false,
          }));
          if (alertErr) console.warn('[health] auth_failure alert insert failed:', alertErr.message);

          console.warn(`[health] Token invalid for cred ${cred.id} (user ${cred.user_id}), marked inactive`);
        }

        await delay(interCallDelayMs);
        continue;
      }

      // SKIPPED_* — log the skip reason only
      if (classification === 'SKIPPED_API_ERROR' || classification === 'SKIPPED_RETRIEVE_FAILED') {
        await _writeLifecycleEvent(supabase, {
          credential_id: cred.id,
          business_account_id: cred.business_account_id,
          event_type: 'pat_invalid',
          token_age_days: _ageDays(cred.issued_at),
          details: { source: 'daily_health_check', error: skipReason },
        });
      }
      await delay(interCallDelayMs);
    }

    const finalStats = {
      scanned: stats.total,
      valid: stats.valid,
      invalid: stats.invalid,
      skipped: stats.skipped,
      recovered,
    };

    console.log(`[health] runTokenHealthCheck complete — valid: ${finalStats.valid}, invalid: ${finalStats.invalid}, skipped: ${finalStats.skipped}, recovered: ${recovered}`);
    logAudit({
      event_type: 'token_health_run_completed',
      action: 'token_health_check',
      details: { ...finalStats, duration_ms: Date.now() - startTime },
      success: finalStats.invalid - finalStats.recovered === 0,
    }).catch(() => {});

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
 *
 * Mirrors legacy services/sync/token-health.js → runUATRefreshCheck().
 */
async function runUATRefreshCheck({ windowDays = 14, interCallDelayMs = 1000, dataAccessWindowDays = 30 } = {}) {
  console.log('[health] runUATRefreshCheck starting...');
  const supabase = getSupabaseAdmin();
  if (!supabase) {
    console.warn('[health] Supabase not available, skipping');
    return { refreshed: 0, refreshFailed: 0, dataAccessWarnings: 0 };
  }

  try {
    // ── Phase 1: 14-day expiring UAT scan + refresh ──
    const tb = triggerBridge;
    const refreshWorker = new UatRefreshWorker();
    const { results, stats } = await refreshWorker.execute({ triggerBridge: tb, windowDays });

    if (stats.total === 0) {
      console.log('[health] No UATs need refresh');
    } else {
      console.log(`[health] Found ${stats.total} UAT(s) expiring within ${windowDays} days`);

      for (const r of results) {
        if (r.success) {
          const { error: alertErr } = await fireAndForgetInsert(supabase.from('system_alerts').insert({
            alert_type: 'uat_auto_refreshed',
            business_account_id: r.uat.business_account_id,
            message: `Your access token was automatically refreshed. New expiry: ${r.newExpiresAt}`,
            details: {
              old_expires_at: r.uat.expires_at,
              new_expires_at: r.newExpiresAt,
              days_remaining_at_refresh: r.daysLeft,
            },
            resolved: true,
          }));
          if (alertErr) console.warn('[health] uat_auto_refreshed alert insert failed:', alertErr.message);

          await _writeLifecycleEvent(supabase, {
            credential_id: r.uat.id,
            business_account_id: r.uat.business_account_id,
            event_type: 'uat_refreshed',
            details: { source: 'uat_refresh_check', old_expires_at: r.uat.expires_at, new_expires_at: r.newExpiresAt, days_remaining: r.daysLeft },
          });
        } else {
          const { error: alertErr } = await fireAndForgetInsert(supabase.from('system_alerts').insert({
            alert_type: 'uat_expiry_warning',
            business_account_id: r.uat.business_account_id,
            message: `Your access token expires in ${r.daysLeft} days and auto-refresh failed. Please reconnect your Instagram account.`,
            details: { expires_at: r.uat.expires_at, error: r.error, days_remaining: r.daysLeft },
            resolved: false,
          }));
          if (alertErr) console.warn('[health] uat_expiry_warning alert insert failed:', alertErr.message);

          await _writeLifecycleEvent(supabase, {
            credential_id: r.uat.id,
            business_account_id: r.uat.business_account_id,
            event_type: 'uat_refresh_failed',
            details: { source: 'uat_refresh_check', error: r.error, expires_at: r.uat.expires_at, days_remaining: r.daysLeft },
          });
        }
        await delay(interCallDelayMs);
      }
    }

    // ── Phase 2: 30-day data_access_expires_at scan + dedup'd warning ──
    const dedupWorker = new DataAccessExpiryWorker();
    const { candidates, stats: dedupStats } = await dedupWorker.execute({ windowDays: dataAccessWindowDays });

    for (const c of candidates) {
      const { uat, daysLeft } = c;

      const { error: alertErr } = await fireAndForgetInsert(supabase.from('system_alerts').insert({
        alert_type: 'data_access_expiry_warning',
        business_account_id: uat.business_account_id,
        message: `Instagram data access expires in ${daysLeft} day${daysLeft !== 1 ? 's' : ''}. Reconnect your account to renew access to messages and comments.`,
        details: {
          data_access_expires_at: uat.data_access_expires_at,
          days_remaining: daysLeft,
          note: 'Cannot be refreshed via fb_exchange_token — requires fresh OAuth consent',
        },
        resolved: false,
      }));
      if (alertErr) console.warn('[health] data_access_expiry_warning alert insert failed:', alertErr.message);

      await _writeLifecycleEvent(supabase, {
        credential_id: uat.id,
        business_account_id: uat.business_account_id,
        event_type: 'data_access_expiry_warning',
        details: { source: 'uat_refresh_check', data_access_expires_at: uat.data_access_expires_at, days_remaining: daysLeft },
      });

      console.log(`[health] data_access_expiry_warning created for account ${uat.business_account_id} (${daysLeft} days left)`);
    }

    const finalStats = {
      refreshed: stats.refreshed,
      refreshFailed: stats.failed,
      dataAccessWarnings: dedupStats.alertable,
      dataAccessDeduped: dedupStats.deduped,
    };

    console.log(`[health] runUATRefreshCheck complete — refreshed: ${finalStats.refreshed}, failed: ${finalStats.refreshFailed}, data_access warnings: ${finalStats.dataAccessWarnings} (deduped: ${finalStats.dataAccessDeduped})`);
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
