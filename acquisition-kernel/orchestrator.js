// control-plane/orchestration/acquisition-orchestrator.js
// Acquisition Orchestrator: constitutional coordination membrane.
//
// Owns: routing acquisition execution actions downward,
//        forwarding acquisition observations upward.
// Does NOT own: governance policy, domain semantics, retry decisions,
//               execution intelligence, credential resolution logic.
//
// Constitutional purity: this orchestrator is a PACKET ROUTER.
// It mechanically forwards EXECUTE_ACQUISITION to CK as RETRY_REQUESTED
// — the engagement-fsm is the sole execution authority. The orchestrator
// never invokes workers directly.

const { getRedisClient } = require('../config/redis');
const substrateRegistry = require('./substrate-registry');
// Phase 1 (base): retry cadence routing goes through the
// acquisition-kernel stub. The stub translates local
// actions (EXECUTE_ACQUISITION) to the canonical RETRY_REQUESTED
// that the retry-cadence-kernel (engagement-fsm) consumes.
const retryStub = require('./retry-state-transition-stub');
const syncSubstrate = require('../substrates/sync-substrate');
const rateLimiter = require('../substrates/rate-limiter');

// Note: acquisition-fsm is imported for future state query hooks.
// Currently the FSM is dispatched via CK's domain FSM routing (constitutional-kernel.js).
// setGovernance wire removed — FSM emits through observability plane directly.

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
  // EXECUTE_ACQUISITION → RETRY_REQUESTED via the
  // acquisition-kernel retry stub. The stub preserves the
  // kernel boundary: the orchestrator emits a local action,
  // the stub translates it to the canonical RETRY_REQUESTED,
  // and the retry-cadence-kernel (engagement-fsm) owns the
  // decision. This replaces the prior direct forwarder that
  // was a constitutional bypass.
  gov.subscribeAction('EXECUTE_ACQUISITION', (action) => {
    _emitTransition({
      domain: 'acquisition', entity: 'acquisition_intent', entityId: action.intentId,
      previousState: 'RECEIVED', nextState: 'EXECUTING',
      authority: 'acquisition-orchestrator',
      raw: { accountId: action.accountId, domain: action.domain },
    });
    retryStub.forwardExecuteAcquisition({
      accountId: action.accountId,
      intentId: action.intentId,
      domain: action.domain,
      params: action.params || {},
    });
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

    // engagement-fsm is the canonical owner of circuit-breaker state
    // (retry-cadence-kernel/fsm.js _circuitBreakers Map). It has already
    // written the account-level mark before emitting this action. The
    // orchestrator's only mechanical job is the per-domain mark, which is
    // a separate substrate (substrates/rate-limiter) tracking per-domain
    // state — not the account-level circuit breaker.
    const retryAfterSeconds = Math.ceil((cooldownMs || 3600000) / 1000);

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
