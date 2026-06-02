// substrates/parsing/workers/content-worker.js
// Content parsing worker: build rows → CK(DB_WRITE_REQUESTED).
//
// Owns: transforming raw business post data into normalized instagram_media rows,
//        emitting through CK for governed DB write.
// Does NOT own: Supabase, governance policy, fetch, orchestration.

async function execute(rawData, accountId, intentId, extra = {}, governance) {
  if (!rawData.posts || rawData.posts.length === 0) return { count: 0 };

  const rows = rawData.posts
    .filter(p => p && p.id)
    .map(p => ({
      instagram_media_id: p.id,
      business_account_id: accountId,
      media_type: p.media_type || null,
      caption: p.caption || null,
      media_url: p.media_url || null,
      thumbnail_url: p.thumbnail_url || null,
      permalink: p.permalink || null,
      like_count: p.like_count || 0,
      comments_count: p.comments_count || 0,
      published_at: p.timestamp || null,
    }));

  if (rows.length === 0) return { count: 0 };

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

  return { count: 0 };
}

module.exports = { execute };
