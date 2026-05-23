// substrates/telemetry.js
// Bounded substrate: structured acquisition telemetry and execution lineage.
//
// Owns: recording acquisition results, execution lineage, domain-scoped API usage.
// Does NOT own: Instagram API calls, orchestration, persistence writes.

const { getSupabaseAdmin, logAudit, logApiRequest } = require('../config/supabase');

// ── Per-domain API usage logger ──────────────────────────────────────────────

/**
 * Tags an api_usage log row with a domain identifier.
 * Enables instant domain-scoped failure queries:
 *   SELECT * FROM api_usage WHERE domain = 'ugc' AND success = false ORDER BY created_at DESC
 */
async function logWithDomain(domain, payload) {
  return logApiRequest({ ...payload, domain }).catch(() => {});
}

// ── Acquisition lineage ──────────────────────────────────────────────────────

/**
 * Records a structured acquisition execution event to audit_log.
 * One event per intent execution — provides full lineage for debugging.
 *
 * @param {string} domain - 'comments' | 'messages' | 'media' | 'insights' | 'ugc'
 * @param {string} accountId - business account UUID
 * @param {string} intentId - acquisition intent ID
 * @param {'completed'|'failed'} status
 * @param {number} count - items acquired
 * @param {number} latencyMs - execution time in ms
 * @param {string|null} [error] - error message if failed
 */
async function recordAcquisition(domain, accountId, intentId, status, count, latencyMs, error) {
  try {
    await logAudit({
      event_type: 'acquisition_executed',
      action: `acquisition_${domain}`,
      resource_type: domain,
      resource_id: accountId,
      details: {
        domain,
        intent_id: intentId,
        status,
        count,
        latency_ms: latencyMs,
        error: error || null,
        timestamp: new Date().toISOString(),
      },
      success: status === 'completed',
    });
  } catch (err) {
    console.warn(`[telemetry] Failed to record acquisition for ${domain}/${accountId}:`, err.message);
  }
}

// ── Re-exports ───────────────────────────────────────────────────────────────

module.exports = {
  logWithDomain,
  recordAcquisition,
  logAudit,
};
