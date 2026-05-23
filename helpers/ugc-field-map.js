// backend.api/helpers/ugc-field-map.js
// Single source of truth for raw Instagram API → ugc_content field mapping.
//
// THIN PROXY RULE: backend writes RAW data only.
// No quality scoring, no business logic here.
// Agent owns quality_score / quality_tier / quality_factors via upsert enrichment.
//
// Conflict key for all upserts: (business_account_id, visitor_post_id)

const VALID_MEDIA_TYPES = new Set(['IMAGE', 'VIDEO', 'CAROUSEL_ALBUM', 'TEXT', 'REELS']);

function normaliseMediaType(raw) {
  const upper = (raw || 'IMAGE').toUpperCase();
  return VALID_MEDIA_TYPES.has(upper) ? upper : 'IMAGE';
}

/**
 * Maps a raw Instagram Graph API post object to ugc_content columns.
 *
 * @param {Object} post             - Raw post from Graph API (hashtag search or /tags)
 * @param {string} businessAccountId - UUID from instagram_business_accounts
 * @param {'hashtag'|'tagged'} source
 * @param {string|null} sourceHashtag - Hashtag string (without #), null for tagged posts
 * @returns {Object} Row ready for ugc_content upsert
 */
function mapRawPostToUgcContent(post, businessAccountId, source, sourceHashtag = null) {
  return {
    business_account_id: businessAccountId,
    visitor_post_id:     post.id,
    author_id:           post.owner?.id || post.owner_id || null,
    author_username:     post.username || null,
    message:             (post.caption || '').slice(0, 2000),
    media_type:          normaliseMediaType(post.media_type),
    media_url:           post.media_url || post.thumbnail_url || null,
    permalink_url:       post.permalink || null,
    like_count:          post.like_count || 0,
    comment_count:       post.comments_count || 0,
    created_time:        post.timestamp || null,
    source,
    source_hashtag:      sourceHashtag,
    quality_score:       null,   // agent fills on enrichment pass
    quality_tier:        null,
  };
}

module.exports = { mapRawPostToUgcContent };
