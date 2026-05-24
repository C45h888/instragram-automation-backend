// substrates/normalization.js
// Bounded substrate: schema normalization.
//
// Owns: transforming raw Instagram API responses into canonical DB row shapes.
//        Every function is pure — no API calls, no DB writes, no side effects.
// Does NOT own: API transport, persistence, retry, orchestration.

const { syncHashtagsFromCaptions } = require('../helpers/agent-helpers');

// ═══════════════════════════════════════════════════════════════════════════════
// COMMENTS
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Normalizes a raw IG comment into a DB-ready instagram_comments row.
 *
 * @param {object} comment - Raw comment from IG API { id, text, username, timestamp, like_count }
 * @param {string} mediaUUID - Supabase UUID for the parent media row
 * @param {string} businessAccountId
 * @returns {object} DB row ready for upsert
 */
function normalizeComment(comment, mediaUUID, businessAccountId) {
  return {
    instagram_comment_id: comment.id,
    text: comment.text || '',
    author_username: comment.username || '',
    author_instagram_id: null,
    media_id: mediaUUID,
    business_account_id: businessAccountId,
    created_at: comment.timestamp,
    like_count: comment.like_count || 0,
    reply_count: 0,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// MESSAGES
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Maps any Meta message object → DB row shape for instagram_dm_messages.
 * Pure function — no async, no imports beyond stdlib.
 *
 * @param {object} m - Raw IG message object from Graph API
 * @param {string|null} conversationUUID - Supabase UUID for the conversation row
 * @param {string} businessAccountId
 * @param {string} igUserId - IG User ID (IGSID for the business)
 * @param {string|null} pageId - Facebook Page ID (defensive fallback)
 * @param {string|null} customerIgId - Customer's IGSID (for recipient fallback)
 * @returns {object} DB row ready for upsert
 */
function transformMessage(m, conversationUUID, businessAccountId, igUserId, pageId, customerIgId) {
  const fromBusiness = m.from?.id === igUserId || (pageId && m.from?.id === pageId);

  const att = m.attachments?.data?.[0] || null;
  const imgData = att?.image_data || null;
  const isSticker = imgData?.render_as_sticker === true;

  const mediaUrl = imgData?.url
    || imgData?.animated_gif_url
    || att?.file_url
    || m.story?.link
    || null;

  let messageType = 'text';
  if (isSticker)                    messageType = 'media';
  else if (att)                     messageType = 'media';
  else if (m.story)                 messageType = 'story_reply';
  else if (m.shares?.data?.length)  messageType = 'post_share';
  else if (m.is_unsupported)        messageType = 'text';

  const mediaType = imgData ? 'image' : att?.file_url ? 'file' : null;

  return {
    instagram_message_id: m.id,
    message_text: m.message || null,
    message_type: messageType,
    media_url: mediaUrl,
    media_type: mediaType,
    conversation_id: conversationUUID,
    business_account_id: businessAccountId,
    is_from_business: fromBusiness,
    recipient_instagram_id: m.to?.data?.[0]?.id
      || (fromBusiness ? customerIgId : igUserId)
      || '',
    sender_username: m.from?.username || null,
    sent_at: m.created_time,
    send_status: fromBusiness ? 'sent' : 'delivered',
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// BUSINESS POSTS
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Normalizes a raw business post into a DB-ready instagram_media row.
 *
 * @param {object} post - Raw post from IG API { id, media_type, caption, media_url, thumbnail_url, permalink, timestamp, like_count, comments_count }
 * @param {string} businessAccountId
 * @returns {object} DB row ready for upsert
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

// ═══════════════════════════════════════════════════════════════════════════════
// MEDIA INSIGHTS
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Normalizes a media insight item into a DB-ready instagram_media row.
 * Story saves fix: writes NULL (not 0) for STORY rows.
 *
 * @param {object} item - Output of fetchMediaInsightsBatch { media_id, media_type, ... insights }
 * @param {string} businessAccountId
 * @returns {object} DB row ready for upsert
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

// ═══════════════════════════════════════════════════════════════════════════════
// UGC
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Maps a raw IG post to a DB-ready ugc_content row.
 *
 * @param {Object} post             - Raw post from Graph API (hashtag search or /tags)
 * @param {string} businessAccountId - UUID from instagram_business_accounts
 * @param {'hashtag'|'tagged'} source
 * @param {string|null} sourceHashtag - Hashtag string (without #), null for tagged posts
 * @returns {Object} Row ready for ugc_content upsert
 */
const VALID_MEDIA_TYPES = new Set(['IMAGE', 'VIDEO', 'CAROUSEL_ALBUM', 'TEXT', 'REELS']);

function normaliseMediaType(raw) {
  const upper = (raw || 'IMAGE').toUpperCase();
  return VALID_MEDIA_TYPES.has(upper) ? upper : 'IMAGE';
}

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
    quality_score:       null,
    quality_tier:        null,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// HASHTAG SYNC (writes hashtags from captions — categorized as normalization)
// ═══════════════════════════════════════════════════════════════════════════════

module.exports = {
  normalizeComment,
  transformMessage,
  normalizeBusinessPost,
  normalizeMediaInsight,
  mapRawPostToUgcContent,
  syncHashtagsFromCaptions,
};
