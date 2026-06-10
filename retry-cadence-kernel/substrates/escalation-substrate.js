// retry-cadence-kernel/substrates/escalation-substrate.js
// Escalation Substrate — bounded operator notification and incident management.
//
// CONSTITUTIONAL CONTRACT:
//   Owns: operator notification, incident generation, severity-driven
//         quarantine decisions, suspension logic for exhausted recovery.
//
//   Does NOT own: classification (persistence-failure-substrate),
//                 recommendation selection (FSM),
//                 actual alert delivery (escalation-worker).
//
// Worker beneath: escalation-worker
//
// Flow:
//   FSM → ESCALATE_TO_OPERATOR_AUTHORIZED → escalation-substrate.execute()
//     → escalation-worker generates notification
//     → for CRITICAL: emits SYSTEM_QUARANTINE to suspend affected domain
//     → for HIGH: emits OPERATOR_NOTIFIED
//     → for MEDIUM/LOW: logs only
//     → emits ESCALATION_COMPLETE

const escalationWorker = require('../workers/escalation-worker');

async function execute(event, governance) {
  const startTime = Date.now();
  const { domain, accountId, intentId, category, subtype, severity, table, analysis } = event;

  if (!governance || (typeof governance.dispatchGlobal !== 'function' && typeof governance.dispatch !== 'function')) {
    return { success: false, error: 'governance required', durationMs: Date.now() - startTime };
  }

  const result = await escalationWorker.execute({
    domain, accountId, intentId, category, subtype, severity, table, analysis,
  }, governance);

  const durationMs = Date.now() - startTime;
  const effectiveSeverity = severity || analysis?.severity || 'MEDIUM';

  (governance?.dispatchGlobal || governance?.dispatch)({
    type: 'ESCALATION_COMPLETE',
    domain: domain || 'persist-telemetry',
    accountId: accountId || '*',
    intentId,
    category, subtype, severity: effectiveSeverity,
    notified: result.notified,
    quarantined: result.quarantined,
    workerName: 'escalation-worker',
    durationMs,
  });

  // For CRITICAL: suspend the affected domain
  if (effectiveSeverity === 'CRITICAL') {
    (governance?.dispatchGlobal || governance?.dispatch)({
      type: 'SYSTEM_QUARANTINE',
      domain: domain || 'persist-telemetry',
      accountId: accountId || '*',
      reason: `CRITICAL: ${category}/${subtype} on ${table || 'unknown'}`,
      suspendedUntil: Date.now() + 300000,  // 5min quarantine
      analysis,
    });
  }

  return { ...result, durationMs };
}

module.exports = { execute };
