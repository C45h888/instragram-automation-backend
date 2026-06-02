// substrates/parsing/workers/ugc-worker.js
// UGC parsing worker: parse → normalize → persist for UGC domain.
//
// Owns: transforming raw UGC media into Supabase rows.
// Does NOT own: fetch, transport, orchestration, governance.

const { mapRawPostToUgcContent } = require('../../ugc/normalizer');
const persistence = require('../../persistence');

/**
 * Execute the UGC parsing pipeline.
 *
 * @param {object} rawData — raw transport response { records, cleanHashtag }
 * @param {string} accountId
 * @param {object} [extra] — unused
 * @returns {Promise<{count: number, error?: string}>}
 */
async function execute(rawData, accountId, extra = {}) {
  if (!rawData.records || rawData.records.length === 0) return { count: 0 };

  const source = rawData.cleanHashtag ? 'hashtag' : 'tagged';
  const records = rawData.records
    .filter(p => p.id)
    .map(p => mapRawPostToUgcContent(p, accountId, source, rawData.cleanHashtag || null));

  const result = await persistence.storeUgcContentBatch(records);
  return { count: result.count || 0 };
}

module.exports = { execute };
