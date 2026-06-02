// substrates/retry-cadence/workers/ugc-retry-worker.js
// UGC retry worker: hashtag search + tagged media retry logic.
//
// Owns: domain-specific fetch, error classification, retry scheduling
//        for UGC endpoints (hashtag search, tagged media).
// Does NOT own: circuit breaker gate (engagement-fsm), parsing, auth escalation.
//
// Policy: maxRetries=1, baseDelay=60s, maxDelay=10min, backoff=2x.
// Error handling:
//   - IG code 4, 613 (app-level): retry 1x, 60s base
//   - ETIMEDOUT / 5xx: retry 1x, 60s base
//   - auth_failure / permanent: no retry → exhaust immediately
//   - Hashtag not found: no retry (permanent — hashtag doesn't exist)

const retry = require('../../retry');
const { getPolicy } = require('../policy');
const ugcTransport = require('../../ugc/transport');
const persistence = require('../../persistence');
const parsing = require('../../parsing');

/**
 * Schedule a retry for the UGC domain.
 */
function schedule(domain, accountId, intentId, params, retryCount, maxRetries, governance) {
  const policy = getPolicy(domain);
  const delayMs = Math.min(
    policy.baseDelayMs * Math.pow(policy.backoffMultiplier, retryCount - 1),
    policy.maxDelayMs
  );
  const timeoutId = setTimeout(() => _execute(domain, accountId, intentId, params, retryCount, maxRetries, governance), delayMs);
  return timeoutId;
}

async function _execute(domain, accountId, intentId, params, retryCount, maxRetries, governance) {
  try {
    const creds = await persistence.resolveAccountCredentials(accountId);

    let result;
    if (params.hashtag) {
      result = await ugcTransport.fetchHashtagMedia(accountId, params.hashtag, params.limit, creds);
    } else {
      result = await ugcTransport.fetchTaggedMedia(accountId, params.limit, creds);
    }

    if (result.success) {
      parsing.dispatch(domain, result, accountId, intentId, {});
      if (governance) {
        governance.dispatch({
          type: 'EXECUTION_OBSERVATION',
          accountId, intentId, domain,
          status: 'completed', error_category: null,
          retryable: false, count: 0, latencyMs: 0, error: null,
        });
      }
      return;
    }

    // Hashtag not found → permanent, no retry
    if (result.error && result.error.startsWith('Hashtag not found')) {
      if (governance) {
        governance.dispatch({
          type: 'RETRY_EXHAUSTED',
          accountId, domain, intentId,
          error: result.error,
          error_category: 'permanent',
        });
      }
      return;
    }

    const classification = retry.handleFetchError(result, accountId);

    if (!classification.retryable || retryCount >= maxRetries) {
      if (governance) {
        governance.dispatch({
          type: 'RETRY_EXHAUSTED',
          accountId, domain, intentId,
          error: result.error || (retryCount >= maxRetries ? 'max_retries_exceeded' : 'permanent_failure'),
          error_category: result.error_category,
          retryCount,
        });
      }
      return;
    }

    schedule(domain, accountId, intentId, params, retryCount + 1, maxRetries, governance);

  } catch (err) {
    console.error(`[ugc-retry-worker] Retry execution failed for ${domain}/${accountId}:`, err.message);
    if (governance) {
      governance.dispatch({ type: 'RETRY_EXHAUSTED', accountId, domain, intentId, error: err.message });
    }
  }
}

module.exports = { schedule };
