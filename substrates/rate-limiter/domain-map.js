// substrates/rate-limiter/domain-map.js
// Rate-limiter domain map: domain → substrate + IG error code → cooldown.
//
// Owns: mapping domain names to substrates and IG error codes to cooldown durations.
// Does NOT own: rate limit state, enforcement, policy, execution mechanics.
//
// Pure lookup — zero side effects, zero state.

// Domain → substrate mapping
const DOMAIN_TO_SUBSTRATE = {
  comments: 'engagement',
  messages: 'engagement',
  ugc:      'ugc',
  insights: 'insights',
  media:    'content',
};

// Substrate → sibling domains (all domains that share a rate limit fate)
const SUBSTRATE_DOMAINS = {
  engagement: ['comments', 'messages'],
  ugc:        ['ugc'],
  insights:   ['insights'],
  content:    ['media'],
};

// IG Graph API error code → cooldown in seconds
// Sources: Meta Graph API docs — rate limit error codes
const IG_CODE_COOLDOWNS = {
  4:   3600,  // Application-level throttling
  17:  3600,  // User request limit reached (app-level)
  32:  900,   // Page-level throttling (shorter cooldown)
  613: 3600,  // Calls to this api have exceeded the rate limit
};

// Default cooldown per domain when IG code is unrecognized
const DOMAIN_DEFAULT_COOLDOWNS = {
  comments: 3600,
  messages: 3600,
  ugc:      3600,
  insights: 3600,
  media:    3600,
};

/**
 * Return the substrate a domain belongs to.
 * @param {string} domain — e.g. 'comments', 'ugc'
 * @returns {string|null}
 */
function getSubstrate(domain) {
  return DOMAIN_TO_SUBSTRATE[domain] || null;
}

/**
 * Return all domains affected when a domain is rate-limited.
 * Includes the triggering domain and its substrate siblings.
 * @param {string} domain
 * @returns {string[]}
 */
function getAffectedDomains(domain) {
  const substrate = getSubstrate(domain);
  if (!substrate) return [domain];
  return [...(SUBSTRATE_DOMAINS[substrate] || [domain])];
}

/**
 * Return the cooldown in seconds for an IG error code on a domain.
 * Uses IG code lookup first, falls back to domain default.
 * @param {string} domain
 * @param {number|null} igCode
 * @returns {number} seconds
 */
function getCooldown(domain, igCode) {
  if (igCode != null && IG_CODE_COOLDOWNS[igCode] != null) {
    return IG_CODE_COOLDOWNS[igCode];
  }
  return DOMAIN_DEFAULT_COOLDOWNS[domain] || 3600;
}

module.exports = { getSubstrate, getAffectedDomains, getCooldown };
