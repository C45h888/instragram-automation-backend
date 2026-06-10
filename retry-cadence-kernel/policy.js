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
  // ── Publish (publish:*) — low frequency, very high value ───────
  // Engagement is driven by posts, so a successful retry directly
  // fuels downstream engagement. Conservative cadence: 2 retries
  // with 60s base delay (60s, 120s) capped at 10min. Respects
  // IG-code-specific classifier overrides.
  publishing: {
    maxRetries:         2,
    baseDelayMs:        60000,   // 60s for first retry
    maxDelayMs:         600000,  // 10min cap
    backoffMultiplier:  2,
    classifierOverride: true,
  },
  // ── Dedup — Redis-backed, one retry ────────────────────────────
  // Dedup operations are lightweight Redis SET/GET/EXISTS.
  // Failures are typically transient Redis connectivity issues.
  // One retry with 30s base delay (30s, 60s) capped at 5min.
  dedup: {
    maxRetries:         1,
    baseDelayMs:        30000,   // 30s
    maxDelayMs:         300000,  // 5min
    backoffMultiplier:  2,
  },
  // ── Reconciliation — heavyweight, one retry ────────────────────
  // Reconciliation cycles are expensive (lineage snapshot + engine
  // comparison across all domains). One retry with 60s base delay
  // (60s, 120s) capped at 5min. Conservative — only retry if the
  // cycle genuinely failed, not for drift detection retries.
  reconciliation: {
    maxRetries:         1,
    baseDelayMs:        60000,   // 60s
    maxDelayMs:         300000,  // 5min
    backoffMultiplier:  2,
  },
  // ── Telemetry Coordination — system-wide ingress lag retry ─────────────────
  // Ingress lag retries are system-wide (not per-account). Multiple
  // attempts with moderate backoff to allow the lag to clear naturally.
  // Engagement-fsm owns the cadence; telemetry-coordination-fsm holds state.
  'telemetry-coordination': {
    maxRetries:         3,
    baseDelayMs:        15000,   // 15s
    maxDelayMs:         60000,   // 1min
    backoffMultiplier:  1.5,
  },
  // ── Telemetry Namespace Retries — partition write failure recovery ──────────
  // 5 namespace-specific policies. Same retry shape as telemetry-coordination
  // (moderate backoff, 3 attempts) since the failure vector is the same
  // (Redis partition rpush). Per-namespace isolation prevents one slow
  // namespace from starving the others.
  'telemetry:runtime':   { maxRetries: 3, baseDelayMs: 15000, maxDelayMs: 60000, backoffMultiplier: 1.5 },
  'telemetry:integrity': { maxRetries: 3, baseDelayMs: 15000, maxDelayMs: 60000, backoffMultiplier: 1.5 },
  'telemetry:authority': { maxRetries: 3, baseDelayMs: 15000, maxDelayMs: 60000, backoffMultiplier: 1.5 },
  'telemetry:health':    { maxRetries: 3, baseDelayMs: 15000, maxDelayMs: 60000, backoffMultiplier: 1.5 },
  'telemetry:systemic':  { maxRetries: 3, baseDelayMs: 15000, maxDelayMs: 60000, backoffMultiplier: 1.5 },
  'telemetry:capability': { maxRetries: 3, baseDelayMs: 15000, maxDelayMs: 60000, backoffMultiplier: 1.5 },
  // ── Persist-telemetry retries — Supabase is an external unstable domain ──
  // Failure vectors are not predictable (5xx, 429, network, JWT expiry,
  // schema migrations). 3 attempts with moderate backoff. The substrate
  // (persistence-failure-substrate) decides retryability per-failure.
  // This policy is the budget; the substrate is the per-failure gate.
  'persist-telemetry': {
    maxRetries:        3,
    baseDelayMs:       30000,   // 30s
    maxDelayMs:        300000,  // 5min
    backoffMultiplier: 2,
  },
  // Reads are idempotent and cheap — same policy. The retry worker
  // (phase 2) will use the substrate's retryAfterMs as an override
  // for 429 responses.
  'persist-telemetry-read': {
    maxRetries:        3,
    baseDelayMs:       30000,   // 30s
    maxDelayMs:        300000,  // 5min
    backoffMultiplier: 2,
  },
};

// Domain → substrate mapping
const DOMAIN_TO_SUBSTRATE = {
  comments:        'engagement',
  messages:        'engagement',
  ugc:             'ugc',
  insights:        'insights',
  media:           'content',
  'publish:post':  'publishing',
  'publish:story': 'publishing',
  'publish:comment': 'publishing',
  'publish:message': 'publishing',
  'dedup:redis':     'dedup',
  'dedup:repair':    'dedup',
  reconciliation:    'reconciliation',
  'telemetry-coordination': 'telemetry-coordination',
  // Telemetry namespace retries — each namespace is its own substrate
  'telemetry:runtime':   'telemetry:runtime',
  'telemetry:integrity': 'telemetry:integrity',
  'telemetry:authority': 'telemetry:authority',
  'telemetry:health':    'telemetry:health',
  'telemetry:systemic':  'telemetry:systemic',
  'telemetry:capability': 'telemetry:capability',
  // Persist-telemetry — writes and reads use the same substrate policy
  'persist-telemetry':      'persist-telemetry',
  'persist-telemetry-read': 'persist-telemetry-read',
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

/**
 * Cap a substrate-generated delay at the policy's structural maximum.
 * Per Q4: substrate generates tactical delay, policy caps it.
 *
 * @param {number} substrateDelayMs — delay from the substrate's backoff
 * @param {string} domain — which domain's policy cap to apply
 * @returns {number} — min(substrateDelayMs, policy.maxDelayMs)
 */
function capDelay(substrateDelayMs, domain) {
  const policy = getPolicy(domain);
  if (!policy || !policy.maxDelayMs) return substrateDelayMs;
  return Math.min(substrateDelayMs, policy.maxDelayMs);
}

module.exports = { getPolicy, computeDelay, computeRetryDelay, capDelay };
