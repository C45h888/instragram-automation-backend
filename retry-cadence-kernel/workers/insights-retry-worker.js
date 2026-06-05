// substrates/retry-cadence/workers/insights-retry-worker.js
// Insights retry worker: account insights + media insights retry logic.
//
// Owns: domain-specific fetch, error classification, retry scheduling
//        for insights endpoints (account insights, per-media insights batch).
// Does NOT own: circuit breaker gate (engagement-fsm), parsing, auth escalation.
//
// Policy: maxRetries=1, baseDelay=60s, maxDelay=10min, backoff=2x.
// Error handling:
//   - IG code 4, 17 (app-level): retry 1x, 60s base
//   - ETIMEDOUT / 5xx: retry 1x, 60s base
//   - auth_failure / permanent: no retry → exhaust immediately
//   - Individual media insight fetch failures: ignored (batch handles internally)
//   - Media feed step fails → retry whole pipeline
//   - Insights batch step fails → retry from batch step only

const retry = require('../../substrates/retry');
const { getPolicy } = require('../policy');
const insightsTransport = require('../../acquisition-kernel/substrates/insights/transport');
const { resolveAccountCredentials } = require('../../helpers/agent-helpers');
const parsing = require('../../acquisition-kernel/parsing');

/**
 * Schedule a retry for the insights domain.
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
    const creds = await resolveAccountCredentials(accountId);
    const sevenDaysAgo = params.since || Math.floor((Date.now() - 7 * 24 * 3600000) / 1000);
    const now = params.until || Math.floor(Date.now() / 1000);

    // Step 1: media feed — if failed, retry from here
    let feedResult;
    if (!params._feedDone) {
      feedResult = await insightsTransport.fetchMediaFeed(accountId, sevenDaysAgo, now, creds);
      if (!feedResult || !feedResult.success) {
        _handleFailure(feedResult || { success: false, error: 'feed_fetch_failed' }, domain, accountId, intentId,
          { ...params, _feedDone: false }, retryCount, maxRetries, governance);
        return;
      }
    }

    // Step 2: insights batch — if feed succeeded, fetch insights
    const mediaList = feedResult.mediaList || params._mediaList || [];
    if (mediaList.length === 0) {
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

    const insights = await insightsTransport.fetchMediaInsightsBatch(mediaList, creds.pageToken);

    // Success — both steps complete
    const result = { success: true, insights, mediaList, _usagePct: feedResult._usagePct };
    parsing.dispatch(domain, result, accountId, intentId, {
      igUserId: feedResult.igUserId, pageToken: creds.pageToken,
    });
    if (governance) {
      governance.dispatch({
        type: 'EXECUTION_OBSERVATION',
        accountId, intentId, domain,
        status: 'completed', error_category: null,
        retryable: false, count: 0, latencyMs: 0, error: null,
      });
    }

  } catch (err) {
    console.error(`[insights-retry-worker] Retry execution failed for ${domain}/${accountId}:`, err.message);
    if (governance) {
      governance.dispatch({ type: 'RETRY_EXHAUSTED', accountId, domain, intentId, error: err.message });
    }
  }
}

/**
 * Handle failure — classify and decide retry or exhaust.
 */
function _handleFailure(result, domain, accountId, intentId, params, retryCount, maxRetries, governance) {
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
}

module.exports = { schedule };
