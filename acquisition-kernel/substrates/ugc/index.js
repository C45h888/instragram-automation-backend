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

/**
 * Persist UGC data to Supabase.
 * Constitutional path: normalize → CK(DB_WRITE_REQUESTED) → writer.
 */
async function persist(accountId, rawData, extra = {}) {
  const governance = extra._governance;

  if (!rawData.records || rawData.records.length === 0) return { count: 0 };
  const source = rawData.cleanHashtag ? 'hashtag' : 'tagged';
  const rows = rawData.records
    .filter(p => p.id)
    .map(p => mapRawPostToUgcContent(p, accountId, source, rawData.cleanHashtag || null));
  if (rows.length === 0) return { count: 0 };

  governance?.dispatch({
    type: 'DB_WRITE_REQUESTED',
    domain: 'ugc', accountId, intentId: null,
    table: 'ugc_content',
    operation: 'batch_upsert_ugc',
    rows,
  });
  return { count: rows.length };
}

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

module.exports = { fetch, persist };
