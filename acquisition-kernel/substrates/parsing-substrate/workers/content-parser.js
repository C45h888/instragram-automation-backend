// substrates/parsing/workers/content-worker.js
// Content parsing worker: parse → normalize → CK(DB_WRITE_REQUESTED).
//
// Owns: sequencing the content pipeline for business post data.
// Does NOT own: normalization logic (content normalizer), Supabase,
//               governance policy. Hashtag enrichment deferred to Phase 6.
//
// Phase 4: canonical path — uses domain substrate tools, no inline normalization.

const { normalizeBusinessPost } = require('../../ugc-content-substrate/content-normalizer');

async function execute(rawData, accountId, intentId, extra = {}, governance) {
  if (!rawData.posts || rawData.posts.length === 0) return { count: 0 };

  const rows = rawData.posts
    .filter(p => p && p.id)
    .map(p => normalizeBusinessPost(p, accountId));

  if (!rows.length) return { count: 0 };

  if (governance) {
    governance.dispatch({
      type: 'DB_WRITE_REQUESTED',
      domain: 'media',
      accountId, intentId,
      table: 'instagram_media',
      operation: 'batch_upsert_posts',
      rows,
    });
  }

  return { count: rows.length };
}

module.exports = { execute };
