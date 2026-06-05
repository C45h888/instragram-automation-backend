// substrates/retry-cadence/workers/content-retry-worker.js
// Content retry worker: business media posts retry logic.
//
// Owns: domain-specific fetch, error classification, retry scheduling
//        for content endpoints (business media posts).
// Does NOT own: circuit breaker gate (engagement-fsm), parsing, auth escalation.
//
// Policy: maxRetries=1, baseDelay=45s, maxDelay=5min, backoff=2x.
// Error handling:
//   - IG code 4 (app-level): retry 1x, 45s base
//   - ETIMEDOUT / 5xx: retry 1x, 45s base
//   - auth_failure / permanent: no retry → exhaust immediately
//   - Posts API is simpler — fewer error modes than comments/messages

const retry = require('../../retry');
const { getPolicy } = require('../policy');
const contentTransport = require('../../content/transport');
const { resolveAccountCredentials } = require('../../../helpers/agent-helpers');
const parsing = require('../../parsing');

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
    const creds = await resolveAccountCredentials(accountId);
    const result = await contentTransport.fetchPosts(accountId, params.limit || 50, creds);

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
    console.error(`[content-retry-worker] Retry execution failed for ${domain}/${accountId}:`, err.message);
    if (governance) {
      governance.dispatch({ type: 'RETRY_EXHAUSTED', accountId, domain, intentId, error: err.message });
    }
  }
}

module.exports = { schedule };
