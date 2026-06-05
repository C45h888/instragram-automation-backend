// substrates/retry-cadence/workers/engagement-retry-worker.js
// Engagement retry worker: comments + messages domain retry logic.
//
// Owns: domain-specific fetch, error classification, retry scheduling
//        for engagement endpoints (comments, conversations, messages).
// Does NOT own: circuit breaker gate (engagement-fsm), parsing, auth escalation.
//
// Policy: maxRetries=2, baseDelay=30s, maxDelay=5min, backoff=2x.
// Error handling:
//   - IG code 4, 17 (app-level throttle): retry 2x, 60s base delay
//   - IG code 32 (page-level): retry 2x, 15s base (shorter cooldown)
//   - ETIMEDOUT / 5xx: retry 2x, 30s base
//   - auth_failure / permanent: no retry → exhaust immediately

const retry = require('../../substrates/retry');
const { getPolicy } = require('../policy');
const engagementTransport = require('../../acquisition-kernel/substrates/engagement/transport');
const { resolveAccountCredentials } = require('../../graph-capability-kernel/substrates/credential-resolver');
const parsing = require('../../acquisition-kernel/parsing');

// Engagement-specific: IG code → override base delay
const IG_CODE_DELAY_OVERRIDES = {
  4:   60000,  // App-level throttle → 60s base (slower recovery)
  17:  60000,  // User request limit → 60s
  32:  15000,  // Page-level → 15s (faster recovery)
  613: 60000,  // Rate limit exceeded → 60s
};

/**
 * Schedule a retry for the engagement domain.
 * Called by retry-cadence index after circuit breaker gate passes.
 *
 * @returns {object} timeoutId — for cancellation tracking
 */
function schedule(domain, accountId, intentId, params, retryCount, maxRetries, governance) {
  const policy = getPolicy(domain);
  // Use override delay if IG code is known, otherwise compute standard backoff
  const igCode = params._lastIgCode || null;
  const baseDelay = igCode && IG_CODE_DELAY_OVERRIDES[igCode]
    ? IG_CODE_DELAY_OVERRIDES[igCode]
    : policy.baseDelayMs;
  const delayMs = Math.min(baseDelay * Math.pow(policy.backoffMultiplier, retryCount - 1), policy.maxDelayMs);

  const timeoutId = setTimeout(() => _execute(domain, accountId, intentId, params, retryCount, maxRetries, governance), delayMs);

  return timeoutId;
}

/**
 * Internal: execute the retry attempt.
 */
async function _execute(domain, accountId, intentId, params, retryCount, maxRetries, governance) {
  try {
    const creds = await resolveAccountCredentials(accountId);

    // Fetch — engagement substrate
    let result;
    if (params.media_id) {
      result = await engagementTransport.fetchComments(accountId, params.media_id, params.limit, creds);
    } else if (params.conversation_id) {
      result = await engagementTransport.fetchMessages(accountId, params.conversation_id, params.limit, creds);
    } else {
      result = await engagementTransport.fetchConversations(accountId, params.convLimit || params.limit, creds);
    }

    if (result.success) {
      // Success → parse + persist async, emit observation
      parsing.dispatch(domain, result, accountId, intentId, {
        igUserId: result.igUserId, pageId: result.pageId, pageToken: result.pageToken,
      });
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

    // Classify error
    const classification = retry.handleFetchError(result, accountId);

    // Non-retryable errors → exhaust immediately (no retry for engagement domain)
    if (!classification.retryable) {
      if (governance) {
        governance.dispatch({
          type: 'RETRY_EXHAUSTED',
          accountId, domain, intentId,
          error: result.error || 'permanent_failure',
          error_category: result.error_category,
          igCode: result.code,
        });
      }
      return;
    }

    // Retryable → check if retries remain
    if (retryCount >= maxRetries) {
      if (governance) {
        governance.dispatch({
          type: 'RETRY_EXHAUSTED',
          accountId, domain, intentId,
          error: 'max_retries_exceeded',
          retryCount,
        });
      }
      return;
    }

    // Schedule next retry with IG code for override delay
    const nextParams = { ...params, _lastIgCode: result.code || null };
    schedule(domain, accountId, intentId, nextParams, retryCount + 1, maxRetries, governance);

  } catch (err) {
    console.error(`[engagement-retry-worker] Retry execution failed for ${domain}/${accountId}:`, err.message);
    if (governance) {
      governance.dispatch({ type: 'RETRY_EXHAUSTED', accountId, domain, intentId, error: err.message });
    }
  }
}

module.exports = { schedule };
