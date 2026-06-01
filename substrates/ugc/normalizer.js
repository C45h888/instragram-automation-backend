// substrates/ugc/normalizer.js
// UGC normalizer: parsed IR → canonical ugc_content row shape.
//
// Owns: transforming parsed UGC data into ugc_content rows.
// Does NOT own: API transport, DB writes, parsing, orchestration.
//
// Extracted from substrates/normalization.js (former monolith).

const VALID_MEDIA_TYPES = new Set(['IMAGE', 'VIDEO', 'CAROUSEL_ALBUM', 'TEXT', 'REELS']);

/**
 * Normalize a raw media_type string to an enum-compatible value.
 */
function normaliseMediaType(raw) {
  const upper = (raw || 'IMAGE').toUpperCase();
  return VALID_MEDIA_TYPES.has(upper) ? upper : 'IMAGE';
}

/**
 * Map a parsed UGC post to a DB-ready ugc_content row.
 *
 * @param {{ id: string, media_type: string|null, media_url: string|null, caption: string|null, timestamp: string|null, username: string|null, like_count: number, comments_count: number, permalink: string|null, owner_id: string|null }} post
 * @param {string} businessAccountId
 * @param {'hashtag'|'tagged'} source
 * @param {string|null} sourceHashtag
 * @returns {object}
 */
function mapRawPostToUgcContent(post, businessAccountId, source, sourceHashtag = null) {
  return {
    business_account_id: businessAccountId,
    visitor_post_id:     post.id,
    author_id:           post.owner_id || null,
    author_username:     post.username || null,
    message:             (post.caption || '').slice(0, 2000),
    media_type:          normaliseMediaType(post.media_type),
    media_url:           post.media_url || null,
    permalink_url:       post.permalink || null,
    like_count:          post.like_count || 0,
    comment_count:       post.comments_count || 0,
    created_time:        post.timestamp || null,
    source,
    source_hashtag:      sourceHashtag,
    quality_score:       null,
    quality_tier:        null,
  };
}

module.exports = { mapRawPostToUgcContent, normaliseMediaType };
