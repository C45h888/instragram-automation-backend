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
// All execution intelligence lives in governance + substrate registry.

const { getRedisClient } = require('../config/redis');
const substrateRegistry = require('./substrate-registry');
const retryWorker = require('./retry-worker');
// Note (Step 7): the orchastrator no longer imports the
// credential-resolver. The substrate resolves credentials
// internally. The orchastrator's role is dispatch only.
const syncSubstrate = require('../substrates/sync-substrate');
const retrySubstrate = require('../substrates/retry');
const rateLimiter = require('../substrates/rate-limiter');

/**
 * Execute a single bounded acquisition attempt via retry worker.
 * Governance evaluates WORKER_OUTCOME_REPORTED and decides next action.
 *
 * @param {string} accountId
 * @param {string} domain
 * @param {string} intentId
 * @param {object} params
 */
async function executeAcquisition(gov, accountId, domain, intentId, params) {
  const substrate = substrateRegistry.lookup(domain);
  if (!substrate) {
    console.error(`[acquisition-orchestrator] Unknown acquisition domain: ${domain}`);
    gov.dispatch({
      type: 'ACQUISITION_COMPLETE', accountId, domain, intentId,
      result: { status: 'failed', count: 0, error: `unknown domain: ${domain}` },
    });
    return;
  }

  gov.dispatch({ type: 'ACQUISITION_EXECUTING', accountId, domain, intentId });

  // Wire substrate fetch with credential resolution for retry-worker.
  // retry-worker handles ONE bounded I/O call (fetch) and dispatches
  // to parsing substrate for parse→normalize→persist. It is
  // semantically blind — no error classification, no engagement
  // state mutation, no lifecycle emission.
  //
  // Step 7: the substrate resolves credentials internally. The
  // routing binding is now a direct pass-through (no creds wrapper).
  const wiredRouting = {
    fetch: async (acctId, execParams) => {
      return substrate.fetch(acctId, execParams);
    },
  };

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
  gov.subscribeAction('EXECUTE_ACQUISITION', (action) => {
    _emitTransition({
      domain: 'acquisition', entity: 'acquisition_intent', entityId: action.intentId,
      previousState: 'RECEIVED', nextState: 'EXECUTING',
      authority: 'acquisition-orchestrator',
      raw: { accountId: action.accountId, domain: action.domain },
    });
    executeAcquisition(gov, action.accountId, action.domain, action.intentId, action.params);
  });

  gov.subscribeAction('WRITE_ACQUISITION_RESULT', (action) => {
    writeAcquisitionResult(action.accountId, action.domain, action.intentId, action.result);
  });

  // MARK_PERMANENT_FAILURE is emitted by engagement-fsm (RETRY_EXHAUSTED path)
  // and routed through CK's _emitActions → _actionSubscribers. The orchestrator
  // is the mechanical subscriber — it writes to Redis. The semantic decision
  // (terminal failure) is owned by engagement-fsm. This is the canonical
  // subscriber path: CK routes, FSM decides, orchestrator executes.
  gov.subscribeAction('MARK_PERMANENT_FAILURE', (action) => {
    writeAcquisitionResult(action.accountId, action.domain, action.intentId, {
      status: 'failed', count: 0, error: action.error || 'permanent_failure',
    });
  });

  gov.subscribeAction('ENGAGE_CIRCUIT_BREAKER', (action) => {
    const { accountId, cooldownMs = 3600000, substrate, affectedDomains } = action;

    // Account-level mechanical mark (existing)
    const retryAfterSeconds = Math.ceil((cooldownMs || 3600000) / 1000);
    retrySubstrate.markAccountRateLimited(accountId, retryAfterSeconds);

    // Per-domain marks in rate-limiter (new — substrate-aware)
    if (affectedDomains && Array.isArray(affectedDomains) && affectedDomains.length > 0) {
      for (const d of affectedDomains) {
        rateLimiter.recordRateLimit(d, accountId, null, retryAfterSeconds);
      }
    }

    console.warn(`[acquisition-orchestrator] Circuit breaker engaged for ${accountId}` +
      (substrate ? `, substrate: ${substrate}` : '') +
      (affectedDomains?.length ? `, domains: ${affectedDomains.join(',')}` : '') +
      `, cooldown ${retryAfterSeconds}s`);
  });

  gov.subscribeAction('START_INTENT_DISCOVERY', (action) => {
    const redis = getRedisClient();
    if (redis && redis.status === 'ready') {
      syncSubstrate.start(redis, (event) => gov.dispatch(event));
    }
  });

  gov.subscribeAction('CIRCUIT_BREAKER_CLEARED', (action) => {
    // When account-level circuit breaker clears, all per-domain rate limits are stale
    rateLimiter.clearSubstrate('engagement', action.accountId);
    rateLimiter.clearSubstrate('ugc', action.accountId);
    rateLimiter.clearSubstrate('content', action.accountId);
    rateLimiter.clearSubstrate('insights', action.accountId);
    console.log(`[acquisition-orchestrator] Circuit breaker cleared for ${action.accountId} — all substrate rate limits flushed`);
  });

  gov.subscribeAction('STOP_INTENT_DISCOVERY', (action) => {
    syncSubstrate.stop();
  });

  gov.subscribeAction('UPDATE_ACCOUNT_LIST', (action) => {
    syncSubstrate.onKernelSignal({ type: 'UPDATE_ACCOUNT_LIST', accountIds: action.accountIds });
  });

  gov.subscribeAction('UPDATE_DOMAIN_LIST', (action) => {
    syncSubstrate.onKernelSignal({ type: 'UPDATE_DOMAIN_LIST', domains: action.domains });
  });
}

function _emitTransition(params) {
  try {
    const observability = require('../control-plane/observability/emitters/transition-emitter');
    observability.transition(params);
  } catch (err) {
    console.warn('[acquisition-orchestrator] Observability transition error:', err.message);
  }
}

module.exports = { wire };
