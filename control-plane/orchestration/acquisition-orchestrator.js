// control-plane/orchestration/acquisition-orchestrator.js
// Acquisition Orchestrator: constitutional coordination membrane.
//
// Owns: routing acquisition execution actions downward,
//        forwarding acquisition observations upward.
// Does NOT own: governance policy, domain semantics, retry decisions,
//               execution intelligence, credential resolution logic.
//
// Constitutional purity: this orchestrator is a PACKET ROUTER.
// It mechanically dispatches EXECUTE_ACQUISITION / RETRY_ACQUISITION to the
// retry worker. It NEVER interprets what a domain means.
// All execution intelligence lives in governance + domain registry.

const { getRedisClient } = require('../../config/redis');
const domainRegistry = require('../execution/domain-registry');
const retryWorker = require('./retry-worker');
const persistence = require('../../substrates/persistence');
const syncSubstrate = require('../../substrates/sync-substrate');
const retrySubstrate = require('../../substrates/retry');

/**
 * Execute a single bounded acquisition attempt via retry worker.
 * Governance evaluates EXECUTION_OBSERVATION and decides next action.
 *
 * @param {string} accountId
 * @param {string} domain
 * @param {string} intentId
 * @param {object} params
 */
async function executeAcquisition(gov, accountId, domain, intentId, params) {
  const routing = domainRegistry.lookup(domain);
  if (!routing) {
    console.error(`[acquisition-orchestrator] Unknown acquisition domain: ${domain}`);
    gov.dispatch({
      type: 'ACQUISITION_COMPLETE', accountId, domain, intentId,
      result: { status: 'failed', count: 0, error: `unknown domain: ${domain}` },
    });
    return;
  }

  gov.dispatch({ type: 'ACQUISITION_EXECUTING', accountId, domain, intentId });

  // Wire up fetch + persist via domain registry
  const wiredRouting = {
    fetch: async (acctId, execParams) => {
      const creds = await persistence.resolveAccountCredentials(acctId);
      return routing.fetch(acctId, execParams, creds);
    },
    persist: async (acctId, rawData) => {
      return routing.persist(acctId, rawData);
    },
  };

  // Bounded single attempt via retry worker — governance sees every attempt
  await retryWorker.executeSingle(accountId, domain, params, intentId, gov, wiredRouting);
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
 * Wire this orchestrator to the constitutional kernel.
 * Registers per-action-type subscribers for acquisition actions.
 *
 * @param {object} gov — constitutional kernel module
 * @param {object} [acquisitionFsm] — acquisition domain FSM (for state queries)
 */
function wire(gov, acquisitionFsm) {
  // ── EXECUTE_ACQUISITION → retry worker (first attempt) ─────────────────
  gov.subscribeAction('EXECUTE_ACQUISITION', (action) => {
    // Observability: acquisition intent state transition
    _emitTransition({
      domain: 'acquisition',
      entity: 'acquisition_intent',
      entityId: action.intentId,
      previousState: 'RECEIVED',
      nextState: 'EXECUTING',
      authority: 'acquisition-orchestrator',
      raw: { accountId: action.accountId, domain: action.domain },
    });
    executeAcquisition(gov, action.accountId, action.domain, action.intentId, action.params);
  });

  // ── WRITE_ACQUISITION_RESULT → Redis ───────────────────────────────────
  gov.subscribeAction('WRITE_ACQUISITION_RESULT', (action) => {
    writeAcquisitionResult(action.accountId, action.domain, action.intentId, action.result);
  });

  // ── RETRY_ACQUISITION → retry worker (subsequent attempts) ──────────────
  // Each retry is a separate governance action. The acquisition domain FSM
  // evaluates EXECUTION_OBSERVATION and decides whether to retry or fail permanently.
  gov.subscribeAction('RETRY_ACQUISITION', (action) => {
    const { accountId, domain, intentId, params, delayMs } = action;
    // Observability: acquisition intent retry transition
    _emitTransition({
      domain: 'acquisition',
      entity: 'acquisition_intent',
      entityId: intentId,
      previousState: 'EXECUTING',
      nextState: 'RETRYING',
      authority: 'acquisition-orchestrator',
      raw: { accountId, domain, delayMs },
    });
    setTimeout(() => {
      executeAcquisition(gov, accountId, domain, intentId, params);
    }, delayMs || 30000);
  });

  // ── MARK_PERMANENT_FAILURE → write failed result to Redis ──────────────
  gov.subscribeAction('MARK_PERMANENT_FAILURE', (action) => {
    writeAcquisitionResult(action.accountId, action.domain, action.intentId, {
      status: 'failed', count: 0, error: action.error || 'permanent_failure',
    });
  });

  // ── ENGAGE_CIRCUIT_BREAKER → retry substrate (mechanical state write) ───
  gov.subscribeAction('ENGAGE_CIRCUIT_BREAKER', (action) => {
    const { accountId, cooldownMs = 3600000 } = action;
    // Engagement FSM authorizes the breaker; retry substrate performs the mechanical state write
    const retryAfterSeconds = Math.ceil((cooldownMs || 3600000) / 1000);
    retrySubstrate.markAccountRateLimited(accountId, retryAfterSeconds);
    console.warn(`[acquisition-orchestrator] Circuit breaker engaged for ${accountId}, cooldown ${retryAfterSeconds}s`);
  });

  // ── START_INTENT_DISCOVERY → sync substrate ────────────────────────────
  gov.subscribeAction('START_INTENT_DISCOVERY', (action) => {
    const redis = getRedisClient();
    if (redis && redis.status === 'ready') {
      syncSubstrate.start(redis, (event) => {
        gov.dispatch(event);
      });
    }
  });

  // ── STOP_INTENT_DISCOVERY → sync substrate ─────────────────────────────
  gov.subscribeAction('STOP_INTENT_DISCOVERY', (action) => {
    syncSubstrate.stop();
  });

  // ── UPDATE_ACCOUNT_LIST → sync substrate (authority flows DOWN) ────────
  gov.subscribeAction('UPDATE_ACCOUNT_LIST', (action) => {
    syncSubstrate.onKernelSignal({ type: 'UPDATE_ACCOUNT_LIST', accountIds: action.accountIds });
  });
}

/**
 * Emit observability transition. Wrapped in try/catch — never disrupts routing.
 */
function _emitTransition(params) {
  try {
    const observability = require('../observability/emitters/transition-emitter');
    observability.transition(params);
  } catch (err) {
    console.warn('[acquisition-orchestrator] Observability transition error:', err.message);
  }
}

module.exports = { wire };
