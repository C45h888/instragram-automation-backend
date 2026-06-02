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

module.exports = { getPolicy, computeDelay };
