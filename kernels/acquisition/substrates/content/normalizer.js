// substrates/content/normalizer.js
// Content normalizer: parsed IR → canonical instagram_media row shape.
//
// Owns: transforming parsed content data into instagram_media rows.
// Does NOT own: API transport, DB writes, parsing, orchestration.
//
// Extracted from substrates/normalization.js (former monolith).

/**
 * Normalize a parsed business post into a DB-ready instagram_media row.
 *
 * @param {{ id: string, media_type: string|null, caption: string|null, media_url: string|null, thumbnail_url: string|null, permalink: string|null, timestamp: string|null, like_count: number, comments_count: number }} post
 * @param {string} businessAccountId
 * @returns {object}
 */
function normalizeBusinessPost(post, businessAccountId) {
  return {
    instagram_media_id: post.id,
    business_account_id: businessAccountId,
    media_type: post.media_type || null,
    caption: post.caption || null,
    media_url: post.media_url || null,
    thumbnail_url: post.thumbnail_url || null,
    permalink: post.permalink || null,
    like_count: post.like_count || 0,
    comments_count: post.comments_count || 0,
    published_at: post.timestamp || null,
  };
}

module.exports = { normalizeBusinessPost };
