// substrates/ugc/index.js
// UGC substrate: full acquisition pipeline for user-generated content.
//
// Owns: fetch → parse → normalize → persist for UGC domain.
// Does NOT own: retry decisions, governance, orchestration.

const transport = require('./transport');
const parser = require('./parser');
const normalizer = require('./normalizer');
const persistence = require('../persistence');

/**
 * Execute a full acquisition cycle for the UGC domain.
 *
 * @param {string} accountId
 * @param {string} domain — 'ugc'
 * @param {object} params — intent payload { hashtag, limit }
 * @param {object} credentials — pre-resolved
 * @returns {Promise<{status: string, count: number, error: string|null, _usagePct: number|null}>}
 */
async function acquire(accountId, domain, params, credentials) {
  let raw;

  if (params.hashtag) {
    raw = await transport.fetchHashtagMedia(accountId, params.hashtag, params.limit, credentials);
  } else {
    raw = await transport.fetchTaggedMedia(accountId, params.limit, credentials);
  }

  if (!raw.success) {
    return { status: 'failed', count: 0, error: raw.error, _usagePct: raw._usagePct || null };
  }

  const sourceData = raw.rawMedia || raw.records || [];
  const parsed = parser.parseUgcMedia(sourceData);
  if (parsed.length === 0) {
    return { status: 'completed', count: 0, error: null, _usagePct: raw._usagePct };
  }

  const source = raw.cleanHashtag ? 'hashtag' : 'tagged';
  const records = parsed.map(p =>
    normalizer.mapRawPostToUgcContent(p, accountId, source, raw.cleanHashtag || null)
  );

  await persistence.storeUgcContentBatch(records);
  return { status: 'completed', count: records.length, error: null, _usagePct: raw._usagePct };
}

module.exports = { acquire };
