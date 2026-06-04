// substrates/graph-capability/cadence.js
// Timer constants for the Graph Capability substrate.
// Mirrors the Instagram operating plane: rate windows, health cadence, refresh windows.
//
// All values are timer-driven. Workers own their own setInterval — FSM does not poll.

module.exports = {
  // PAT health check — matches existing token-health.js 24h skip window
  PAT_HEALTH_INTERVAL_MS: 24 * 60 * 60 * 1000,

  // Scope re-check — faster than 7d cache so degraded mode can self-heal
  SCOPE_RECHECK_INTERVAL_MS: 6 * 60 * 60 * 1000,

  // UAT expiry proximity — matches existing 14d refresh window
  UAT_EXPIRY_PROXIMITY_MS: 14 * 24 * 60 * 60 * 1000,
  UAT_RECHECK_INTERVAL_MS: 60 * 60 * 1000, // hourly UAT freshness check

  // /debug_token rate limit — matches existing 200ms delay
  DETECTION_RATE_LIMIT_MS: 200,

  // Full re-evaluation cadence — drives CAPABILITY_REEVALUATE
  FULL_REEVALUATE_INTERVAL_MS: 60 * 60 * 1000,

  // Substrate façade aggregation cadence — how often the façade collapses
  // worker observations into a single CAPABILITY_OBSERVATION event
  AGGREGATION_INTERVAL_MS: 5 * 60 * 1000,
};
