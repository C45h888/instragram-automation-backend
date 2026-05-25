// control-plane/orchestration/acquisition-orchestrator.js
// Acquisition Orchestrator: constitutional coordination membrane.
//
// Owns: routing acquisition execution actions downward,
//        forwarding acquisition observations upward.
// Does NOT own: governance policy, domain semantics, retry decisions,
//               execution intelligence, credential resolution logic.
//
// Constitutional purity: this orchestrator is a PACKET ROUTER.
// It mechanically dispatches EXECUTE_ACQUISITION to the domain registry
// and execution bridge. It NEVER interprets what a domain means.
// All execution intelligence lives in governance + domain registry.

const { getRedisClient } = require('../../config/redis');
const governance = require('../governance/governance-kernel');
const domainRegistry = require('../execution/domain-registry');
const executionBridge = require('../execution-bridge');
const persistence = require('../../substrates/persistence');
const syncSubstrate = require('../../substrates/sync-substrate');

/**
 * Execute a governed acquisition for a single intent.
 * Pure mechanical routing: lookup domain → call bridge → forward observation.
 * NEVER interprets domain semantics, retry policy, or error meaning.
 *
 * @param {string} accountId
 * @param {string} domain
 * @param {string} intentId
 * @param {object} params
 */
async function executeGovernedAcquisition(accountId, domain, intentId, params) {
  const routing = domainRegistry.lookup(domain);
  if (!routing) {
    console.error(`[acquisition-orchestrator] Unknown acquisition domain: ${domain}`);
    governance.dispatch({
      type: 'ACQUISITION_COMPLETE', accountId, domain, intentId,
      result: { status: 'failed', count: 0, error: `unknown domain: ${domain}` },
    });
    return;
  }

  governance.dispatch({ type: 'ACQUISITION_EXECUTING', accountId, domain, intentId });

  // Mechanical execution — no policy interpretation
  const outcome = await executionBridge.executeWithRetry(
    accountId, intentId, domain,
    async (acctId, execParams) => {
      const creds = await persistence.resolveAccountCredentials(acctId);
      const rawData = await routing.fetch(acctId, execParams, creds);
      if (!rawData.success) return rawData;
      const persistResult = await routing.persist(acctId, rawData, execParams);
      return {
        success: true,
        count: persistResult?.count || rawData.count || 0,
        _usagePct: rawData._usagePct,
        instagram_id: rawData.instagram_id,
      };
    },
    params
  );

  governance.dispatch({
    type: 'ACQUISITION_COMPLETE', accountId, domain, intentId,
    result: { status: outcome.status, count: outcome.count, error: outcome.error || null },
  });
}

/**
 * Write acquisition result to Redis for agent consumption.
 * Pure mechanical routing — no policy interpretation.
 */
async function writeAcquisitionResult(accountId, domain, intentId, result) {
  const redis = getRedisClient();
  if (!redis || redis.status !== 'ready') return;

  const resultKey = `supervisor:acquisition_results:${accountId}:${intentId}`;
  try {
    await redis.set(resultKey, JSON.stringify({
      intent_id: intentId,
      account_id: accountId,
      domain,
      status: result.status,
      result: { count: result.count },
      error: result.error,
      completed_at: new Date().toISOString(),
    }), 'EX', 3600);
  } catch (err) {
    console.error(`[acquisition-orchestrator] Failed to write result key ${resultKey}:`, err.message);
  }
}

/**
 * Wire this orchestrator to the governance kernel.
 * Registers per-action-type subscribers for acquisition actions.
 *
 * @param {object} gov — governance kernel module
 */
function wire(gov) {
  // ── EXECUTE_ACQUISITION → domain registry → execution bridge ───────────
  gov.subscribeAction('EXECUTE_ACQUISITION', (action) => {
    executeGovernedAcquisition(action.accountId, action.domain, action.intentId, action.params);
  });

  // ── WRITE_ACQUISITION_RESULT → Redis ───────────────────────────────────
  gov.subscribeAction('WRITE_ACQUISITION_RESULT', (action) => {
    writeAcquisitionResult(action.accountId, action.domain, action.intentId, action.result);
  });

  // ── RETRY_ACQUISITION → re-execute with delay ──────────────────────────
  gov.subscribeAction('RETRY_ACQUISITION', (action) => {
    const { accountId, domain, intentId, params, delayMs } = action;
    setTimeout(() => {
      executeGovernedAcquisition(accountId, domain, intentId, params);
    }, delayMs || 30000);
  });

  // ── MARK_PERMANENT_FAILURE → write failed result to Redis ──────────────
  gov.subscribeAction('MARK_PERMANENT_FAILURE', (action) => {
    writeAcquisitionResult(action.accountId, action.domain, action.intentId, {
      status: 'failed', count: 0, error: action.error || 'permanent_failure',
    });
  });

  // ── ENGAGE_CIRCUIT_BREAKER → no routing needed (governance owns state) ─
  gov.subscribeAction('ENGAGE_CIRCUIT_BREAKER', (action) => {
    console.warn(`[acquisition-orchestrator] Circuit breaker engaged for ${action.accountId}/${action.domain || 'all'}, cooldown ${(action.cooldownMs || 3600000) / 1000}s`);
  });

  // ── START_INTENT_DISCOVERY → sync substrate ────────────────────────────
  gov.subscribeAction('START_INTENT_DISCOVERY', (action) => {
    const redis = getRedisClient();
    if (redis && redis.status === 'ready') {
      syncSubstrate.start(redis, gov);
    }
  });

  // ── STOP_INTENT_DISCOVERY → sync substrate ─────────────────────────────
  gov.subscribeAction('STOP_INTENT_DISCOVERY', (action) => {
    syncSubstrate.stop();
  });
}

module.exports = { wire };
