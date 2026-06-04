// control-plane/execution/substrate-registry.js
// Substrate Registry: domain → substrate module lookup.
//
// Owns: mapping domain names to their bounded substrate modules.
// Does NOT own: orchestration, governance, policy, retry, execution flow.
//
// Replaces domain-registry.js. Each substrate exports { fetch, persist }.
// Publish domains removed — migrated to pull-based publishing pipeline
// (post-queue-worker under persist-telemetry-fsm governance).

const engagement = require('../../substrates/engagement');
const content     = require('../../substrates/content');
const ugc         = require('../../substrates/ugc');
const insights    = require('../../substrates/insights');

const DOMAIN_REGISTRY = {
  comments:  { fetch: engagement.fetch.bind(engagement), persist: engagement.persist.bind(engagement) },
  messages:  { fetch: engagement.fetch.bind(engagement), persist: engagement.persist.bind(engagement) },
  ugc:       { fetch: ugc.fetch.bind(ugc),              persist: ugc.persist.bind(ugc) },
  insights:  { fetch: insights.fetch.bind(insights),     persist: insights.persist.bind(insights) },
  media:     { fetch: content.fetch.bind(content),       persist: content.persist.bind(content) },
};

function lookup(domain) {
  // Publish domains are no longer routed through substrate-registry.
  // Publishing now flows through the pull-based pipeline:
  //   cognition-scanner → CK → publishing-fsm → persist-telemetry-fsm → post-queue-worker
  if (domain && domain.startsWith('publish:')) {
    return null;
  }
  return DOMAIN_REGISTRY[domain] || null;
}

function domainForAction(actionType) {
  // Removed — publish domain routing no longer uses substrate-registry.
  // Publishing intents now flow through the pull-based pipeline.
  // Returns a neutral key to avoid breaking emission.js LPUSH for orphaned intents.
  return 'publish:deprecated';
}

function fetchTypeForAction(actionType) {
  return 'publish:deprecated';
}

function allDomains() {
  return Object.keys(DOMAIN_REGISTRY);
}

module.exports = { lookup, domainForAction, fetchTypeForAction, allDomains };
