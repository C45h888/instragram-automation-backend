// substrates/parsing/workers/content-worker.js
// Content parsing worker: parse → normalize → persist for content domain.
//
// Owns: transforming raw business post data into Supabase rows.
// Does NOT own: fetch, transport, orchestration, governance.

const persistence = require('../../persistence');

/**
 * Execute the content parsing pipeline.
 *
 * @param {object} rawData — raw transport response { posts }
 * @param {string} accountId
 * @param {object} [extra] — unused
 * @returns {Promise<{count: number, error?: string}>}
 */
async function execute(rawData, accountId, extra = {}) {
  if (!rawData.posts || rawData.posts.length === 0) return { count: 0 };
  const result = await persistence.storeBusinessPosts(accountId, rawData.posts);
  return { count: result.count || 0 };
}

module.exports = { execute };
