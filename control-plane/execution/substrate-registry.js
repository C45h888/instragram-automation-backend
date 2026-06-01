// control-plane/execution/substrate-registry.js
// Substrate Registry: pure lookup — maps domain names to substrate modules.
//
// Owns: domain → substrate mapping for acquisition domains.
// Does NOT own: orchestration, governance, fetch/persist wiring, policy.
//
// Replaces domain-registry.js — each substrate owns its full pipeline.
// Publish domains mapped to nearest substrate (separate concern, same transport).

const engagement = require('../../substrates/engagement');
const content     = require('../../substrates/content');
const ugc         = require('../../substrates/ugc');
const insights    = require('../../substrates/insights');

const REGISTRY = {
  comments:              engagement,
  messages:              engagement,
  ugc:                   ugc,
  insights:              insights,
  media:                 content,
  'publish:media':       content,
  'publish:ugc':         ugc,
  'publish:messaging':   engagement,
};

/**
 * Pure lookup — returns a substrate module with acquire(), or null.
 *
 * @param {string} domain — e.g. 'comments', 'messages', 'ugc', 'publish:media'
 * @returns {{ acquire: Function }|null}
 */
function lookup(domain) {
  return REGISTRY[domain] || null;
}

/** Returns all known domain keys. */
function allDomains() {
  return Object.keys(REGISTRY);
}

module.exports = { lookup, allDomains };
