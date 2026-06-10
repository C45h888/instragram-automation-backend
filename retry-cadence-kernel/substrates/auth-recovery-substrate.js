// retry-cadence-kernel/substrates/auth-recovery-substrate.js
// Auth Recovery Substrate — bounded authentication recovery logic.
//
// CONSTITUTIONAL CONTRACT:
//   Owns: token refresh, credential validation, session restoration.
//
//   Does NOT own: auth failure detection (persistence-failure-substrate),
//                 recommendation selection (FSM),
//                 token storage (credential-store substrate).
//
// Worker beneath: authentication-recovery-worker
//
// Flow:
//   FSM → REFRESH_AUTHENTICATION_AUTHORIZED → auth-recovery-substrate.execute()
//     → authentication-recovery-worker refreshes the token
//     → on success: re-injects the failed operation with the new token
//     → on failure: escalates to operator
//     → emits AUTH_REFRESHED or AUTH_REFRESH_FAILED

const authenticationRecoveryWorker = require('../workers/authentication-recovery-worker');

async function execute(event, governance) {
  const startTime = Date.now();
  const { domain, accountId, intentId, analysis } = event;

  if (!governance || (typeof governance.dispatchGlobal !== 'function' && typeof governance.dispatch !== 'function')) {
    return { success: false, error: 'governance required', durationMs: Date.now() - startTime };
  }

  const result = await authenticationRecoveryWorker.execute({
    domain, accountId, intentId, analysis,
  }, governance);

  const durationMs = Date.now() - startTime;

  if (result.success) {
    (governance?.dispatchGlobal || governance?.dispatch)({
      type: 'AUTH_REFRESHED',
      domain: domain || 'persist-telemetry',
      accountId: accountId || '*',
      intentId,
      workerName: 'authentication-recovery-worker',
      durationMs,
    });

    // Re-inject the original failed operation with the new token.
    // The connection-recovery-worker will pick this up on the next
    // RETRY_OPERATION cycle.
    (governance?.dispatchGlobal || governance?.dispatch)({
      type: 'RETRY_OPERATION_AUTHORIZED',
      domain: domain || 'persist-telemetry',
      accountId: accountId || '*',
      intentId,
      analysis: { ...analysis, retryable: true, category: 'NETWORK' },  // reclassify after auth refresh
      idempotencyKey: analysis?.idempotencyKey || `${intentId}-${Date.now()}`,
    });
  } else {
    (governance?.dispatchGlobal || governance?.dispatch)({
      type: 'AUTH_REFRESH_FAILED',
      domain: domain || 'persist-telemetry',
      accountId: accountId || '*',
      intentId,
      workerName: 'authentication-recovery-worker',
      error: result.error,
      durationMs,
    });

    // Escalate on auth recovery failure
    (governance?.dispatchGlobal || governance?.dispatch)({
      type: 'ESCALATE_TO_OPERATOR_AUTHORIZED',
      domain: domain || 'persist-telemetry',
      accountId: accountId || '*',
      intentId,
      category: analysis?.category || 'AUTHENTICATION',
      subtype: analysis?.subtype || 'jwt_expired',
      severity: analysis?.severity || 'MEDIUM',
      analysis,
    });
  }

  return { ...result, durationMs };
}

module.exports = { execute };
