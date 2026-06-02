// substrates/parsing/workers/insights-worker.js
// Insights parsing worker: build rows → CK(DB_WRITE_REQUESTED).
//
// Owns: transforming raw insights data into normalized instagram_media rows,
//        emitting through CK for governed DB write.
// Does NOT own: Supabase, governance policy, fetch, orchestration.

async function execute(rawData, accountId, intentId, extra = {}, governance) {
  if (!rawData.insights || rawData.insights.length === 0) return { count: 0 };

  const rows = rawData.insights
    .filter(item => item && item.media_id)
    .map(item => {
      const isStory = item.media_type === 'STORY';
      return {
        instagram_media_id: item.media_id,
        business_account_id: accountId,
        media_type: item.media_type || null,
        caption: item.caption || null,
        media_url: item.media_url || null,
        thumbnail_url: item.thumbnail_url || null,
        permalink: item.permalink || null,
        like_count: item.like_count || 0,
        comments_count: item.comments_count || 0,
        reach: item.insights.find(i => i.name === 'reach')?.values?.[0]?.value || 0,
        impressions: item.insights.find(i => i.name === 'impressions')?.values?.[0]?.value || 0,
        saves: isStory ? null : (item.insights.find(i => i.name === 'saved')?.values?.[0]?.value ?? 0),
        published_at: item.timestamp || null,
      };
    });

  if (rows.length === 0) return { count: 0 };

  if (governance) {
    governance.dispatch({
      type: 'DB_WRITE_REQUESTED',
      domain: 'insights',
      accountId, intentId,
      table: 'instagram_media',
      operation: 'batch_upsert_insights',
      rows,
    });
  }

  return { count: 0 };
}

module.exports = { execute };
