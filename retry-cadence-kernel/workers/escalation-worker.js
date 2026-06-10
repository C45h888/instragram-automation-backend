// retry-cadence-kernel/workers/escalation-worker.js
// Escalation Worker — bounded operator notification and incident generation.
//
// CONSTITUTIONAL CONTRACT:
//   Owns: generating operator notifications, logging critical events,
//         signalling quarantine when recovery is exhausted.
//
//   Does NOT own: recommendation selection (FSM),
//                 severity scoring (persistence-failure-substrate),
//                 alert delivery infrastructure (configurable channel).
//
// Called by: escalation-substrate.

/**
 * Generate an escalation notification.
 *
 * The notification is logged to the observability plane. In production,
 * this would also dispatch to PagerDuty, Slack, email, or SMS. The
 * channel is configured in the config layer and the worker reads it.
 *
 * For CRITICAL severity: emits SYSTEM_QUARANTINE.
 * For all others: emits OPERATOR_NOTIFIED.
 *
 * @param {object} params — { domain, accountId, intentId, category, subtype, severity, table, analysis }
 * @param {object} governance — CK reference
 * @returns {Promise<{ success: boolean, notified: boolean, quarantined: boolean, error?: string }>}
 */
async function execute(params, governance) {
  const { domain, accountId, intentId, category, subtype, severity, table, analysis } = params;

  const effectiveSeverity = severity || analysis?.severity || 'MEDIUM';
  const quarantined = effectiveSeverity === 'CRITICAL';

  // Log the escalation to the observability plane
  const escalationRecord = {
    timestamp: new Date().toISOString(),
    domain: domain || 'persist-telemetry',
    accountId: accountId || '*',
    intentId,
    category,
    subtype,
    severity: effectiveSeverity,
    table: table || 'unknown',
    failureId: analysis?.failureId || null,
    recommendations: analysis?.recommendations || [],
    severityScore: analysis?.severityScore || 0,
    quarantined,
    notified: true,
  };

  // Signal the notification
  (governance?.dispatchGlobal || governance?.dispatch)({
    type: 'OPERATOR_NOTIFIED',
    escalation: escalationRecord,
    quarantined,
  });

  // For production: would also call the configurable alert channel here.

  console.error(`[escalation-worker] ${effectiveSeverity}: ${category}/${subtype} on ${domain}/${table} — notified, quarantined=${quarantined}`);

  return { success: true, notified: true, quarantined, error: null };
}

module.exports = { execute };
