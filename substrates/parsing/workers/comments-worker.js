// substrates/parsing/workers/comments-worker.js
// Comments parsing worker: parse → normalize → persist for comments domain.
//
// Owns: transforming raw comment batches into Supabase rows.
// Does NOT own: fetch, transport, orchestration, governance.

const persistence = require('../../persistence');

/**
 * Execute the comment parsing pipeline.
 *
 * @param {object} rawData — raw transport response { batches, records }
 * @param {string} accountId
 * @param {object} [extra] — unused for comments
 * @returns {Promise<{count: number, error?: string}>}
 */
async function execute(rawData, accountId, extra = {}) {
  if (rawData.batches && rawData.batches.length > 0) {
    const result = await persistence.storeCommentBatches(accountId, rawData.batches);
    return { count: result.count || 0 };
  }
  if (rawData.records && rawData.records.length > 0) {
    const result = await persistence.storeCommentBatches(accountId, [
      { mediaId: 'direct', comments: rawData.records },
    ]);
    return { count: result.count || 0 };
  }
  return { count: 0 };
}

module.exports = { execute };
