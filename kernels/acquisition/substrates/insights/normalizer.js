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
    reach: item.insights.find(i => i.name === 'reach')?.values?.[0]?.value || 0,
    impressions: item.insights.find(i => i.name === 'impressions')?.values?.[0]?.value || 0,
    saves: isStory ? null : (item.insights.find(i => i.name === 'saved')?.values?.[0]?.value ?? 0),
    published_at: item.timestamp || null,
  };
}

module.exports = { normalizeMediaInsight };
