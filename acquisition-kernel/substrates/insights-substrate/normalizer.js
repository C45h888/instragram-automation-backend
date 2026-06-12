// substrates/insights/normalizer.js
// Insights normalizer: parsed IR → canonical instagram_media row shape.
//
// Owns: transforming parsed insights data into instagram_media rows
//        with reach/impressions/saves fields.
// Does NOT own: API transport, DB writes, parsing, orchestration.
//
// Extracted from substrates/normalization.js (former monolith).

/**
 * Normalize a parsed media insight item into a DB-ready instagram_media row.
 * Story saves fix: writes NULL (not 0) for STORY rows.
 *
 * @param {{ media_id: string, media_type: string, timestamp: string, caption: string|null, media_url: string|null, thumbnail_url: string|null, permalink: string|null, like_count: number, comments_count: number, insights: Array }} item
 * @param {string} businessAccountId
 * @returns {object}
 */
function normalizeMediaInsight(item, businessAccountId) {
  const isStory = item.media_type === 'STORY';
  const insights = item.insights || [];

  const _metric = (name) => insights.find(i => i.name === name)?.values?.[0]?.value;

  return {
    instagram_media_id: item.media_id,
    business_account_id: businessAccountId,
    media_type: item.media_type || null,
    caption: item.caption || null,
    media_url: item.media_url || null,
    thumbnail_url: item.thumbnail_url || null,
    permalink: item.permalink || null,
    like_count: item.like_count || 0,
    comments_count: item.comments_count || 0,
    // Core metrics (all media types)
    reach:        _metric('reach')        || 0,
    impressions:  _metric('impressions')  || 0,
    engagement:   _metric('engagement')   || 0,
    plays:        _metric('plays')        || 0,
    shares:       _metric('shares')       || 0,
    saved:        isStory ? null : (_metric('saved') ?? 0),
    total_interactions: _metric('total_interactions') || 0,
    // Video/Reels metrics (zero for non-video)
    video_views:  _metric('video_views')  || 0,
    // Reels-only metrics
    clips_replays_count:           _metric('clips_replays_count')           || 0,
    ig_reels_avg_watch_time:       _metric('ig_reels_avg_watch_time')       || 0,
    ig_reels_video_view_total_time:_metric('ig_reels_video_view_total_time')|| 0,
    published_at: item.timestamp || null,
  };
}

module.exports = { normalizeMediaInsight };
