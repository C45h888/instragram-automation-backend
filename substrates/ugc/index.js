// substrates/ugc/index.js
// UGC substrate: full pipeline for hashtag search and tagged media.
//
// Owns: fetch → parse → normalize → persist for UGC domain.
// Does NOT own: retry logic, error classification, orchestration, credential resolution.

const transport = require('./transport');
const { mapRawPostToUgcContent } = require('./normalizer');
const persistence = require('../persistence');

/**
 * Fetch raw data from Instagram API for UGC domain.
 */
async function fetch(accountId, params, credentials) {
  if (params.hashtag) {
    return transport.fetchHashtagMedia(accountId, params.hashtag, params.limit, credentials);
  }
  return transport.fetchTaggedMedia(accountId, params.limit, credentials);
}

/**
 * Persist raw UGC data to Supabase. Normalizes internally.
 */
async function persist(accountId, rawData) {
  if (!rawData.records || rawData.records.length === 0) return { count: 0 };
  const source = rawData.cleanHashtag ? 'hashtag' : 'tagged';
  const records = rawData.records
    .filter(p => p.id)
    .map(p => mapRawPostToUgcContent(p, accountId, source, rawData.cleanHashtag || null));
  return persistence.storeUgcContentBatch(records);
}

module.exports = { fetch, persist };
