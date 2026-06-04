// substrates/ugc/index.js
// UGC substrate: factory-creates workers → bounded IG API read.
//
// Owns: worker factory + transport bridge. Pure delegation plane.
// Does NOT own: retry, error classification, orchestration, credential resolution.
//
// Workers: HashtagWorker (hashtag search), TaggedWorker (tagged media).
// Normalize: handles post → UgcContent mapping internally for persist.

const HashtagWorker = require('./workers/hashtag');
const TaggedWorker = require('./workers/tagged');
const { mapRawPostToUgcContent } = require('./normalizer');
const persistence = require('../persistence');

/**
 * Fetch raw data from Instagram API for UGC domain.
 * Factory-creates a worker and delegates the bounded call.
 *
 * @param {string} accountId
 * @param {object} params — { hashtag?: string, limit?: number }
 * @param {object} credentials — pre-resolved { igUserId, pageToken }
 * @returns {Promise<object>} raw transport response
 */
async function fetch(accountId, params, credentials) {
  if (params?.hashtag) {
    const worker = new HashtagWorker();
    return worker.execute(accountId, params, credentials);
  }
  const worker = new TaggedWorker();
  return worker.execute(accountId, params, credentials);
}

/**
 * Persist raw UGC data to Supabase. Normalizes internally.
 * Called by parsing workers asynchronously.
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
