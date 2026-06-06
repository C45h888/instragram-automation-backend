// substrates/retry-cadence/policy.js
// Retry-cadence policy: per-substrate retry constants.
//
// Owns: domain-level retry policy — max retries, base delay, backoff.
// Does NOT own: execution, state tracking, circuit breaker decisions.

const POLICIES = {
  engagement: {
    maxRetries:         2,
    baseDelayMs:        30000,   // 30s
    maxDelayMs:         300000,  // 5min
    backoffMultiplier:  2,
  },
  ugc: {
    maxRetries:         1,
    baseDelayMs:        60000,   // 60s
    maxDelayMs:         600000,  // 10min
    backoffMultiplier:  2,
  },
  content: {
    maxRetries:         1,
    baseDelayMs:        45000,   // 45s
    maxDelayMs:         300000,  // 5min
    backoffMultiplier:  2,
  },
  insights: {
    maxRetries:         1,
    baseDelayMs:        60000,   // 60s
    maxDelayMs:         600000,  // 10min
    backoffMultiplier:  2,
  },
};

// Domain → substrate mapping
const DOMAIN_TO_SUBSTRATE = {
  comments: 'engagement',
  messages: 'engagement',
  ugc:      'ugc',
  insights: 'insights',
  media:    'content',
};

/**
 * Get the retry policy for a domain.
 * @param {string} domain
 * @returns {{ maxRetries: number, baseDelayMs: number, maxDelayMs: number, backoffMultiplier: number }}
 */
function getPolicy(domain) {
  const substrate = DOMAIN_TO_SUBSTRATE[domain] || 'engagement';
  return POLICIES[substrate] || POLICIES.engagement;
}

/**
 * Compute delay for retry attempt N (1-indexed).
 * @param {object} policy
 * @param {number} retryCount — 1 = first retry, 2 = second, etc.
 * @returns {number} delay in milliseconds
 */
function computeDelay(policy, retryCount) {
  const raw = policy.baseDelayMs * Math.pow(policy.backoffMultiplier, retryCount - 1);
  return Math.min(raw, policy.maxDelayMs);
}

/**
 * Compute the final retry delay, applying the classifier override.
 * Used by engagement-fsm when scheduling retries. The policy-computed
 * delay and the classifier-computed delay are both candidates. The
 * LONGER of the two wins — the classifier override represents the
 * IG-recommended wait, which should be respected even if the policy
 * says a shorter wait is OK.
 *
 * @param {object} policy — per-substrate retry config
 * @param {number} retryCount — 1-indexed attempt number
 * @param {object|null} actionTag — classification-worker output
 *   { type, retryAfterMs, retryAfterSeconds, igCode, ... }
 * @returns {number} delay in milliseconds
 */
function computeRetryDelay(policy, retryCount, actionTag) {
  const policyDelayMs = computeDelay(policy, retryCount);
  if (actionTag && actionTag.retryAfterMs != null) {
    return Math.max(policyDelayMs, actionTag.retryAfterMs);
  }
  return policyDelayMs;
}

module.exports = { getPolicy, computeDelay, computeRetryDelay };
