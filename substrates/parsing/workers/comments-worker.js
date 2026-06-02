// substrates/parsing/workers/comments-worker.js
// Comments parsing worker: build rows → CK(DB_WRITE_REQUESTED).
//
// Owns: transforming raw comment batches into normalized rows,
//        emitting through CK for governed DB write.
// Does NOT own: Supabase, governance policy, fetch, orchestration.

async function execute(rawData, accountId, intentId, extra = {}, governance) {
  const rows = [];
  const allComments = rawData.batches
    ? rawData.batches.flatMap(b => (b.comments || []).map(c => ({ ...c, _mediaId: b.mediaId })))
    : (rawData.records || []).map(c => ({ ...c, _mediaId: 'direct' }));

  for (const c of allComments) {
    if (!c.id) continue;
    rows.push({
      instagram_comment_id: c.id,
      text: c.text || '',
      author_username: c.username || '',
      author_instagram_id: null,
      media_id: c._mediaId,
      business_account_id: accountId,
      created_at: c.timestamp,
      like_count: c.like_count || 0,
      reply_count: 0,
    });
  }

  if (rows.length === 0) return { count: 0 };

  if (governance) {
    governance.dispatch({
      type: 'DB_WRITE_REQUESTED',
      domain: 'comments',
      accountId, intentId,
      table: 'instagram_comments',
      operation: 'batch_upsert_comments',
      rows,
    });
  }

  return { count: 0 };
}

module.exports = { execute };
