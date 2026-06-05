// substrates/engagement/normalizer.js
// Engagement normalizer: parsed IR → canonical DB row shapes.
//
// Owns: transforming parsed engagement data into instagram_comments and
//        instagram_dm_messages row shapes.
// Does NOT own: API transport, DB writes, parsing, orchestration.
//
// Extracted from substrates/normalization.js (former monolith).

/**
 * Normalize a parsed comment into a DB-ready instagram_comments row.
 *
 * @param {{ id: string, text: string, username: string, timestamp: string, like_count: number }} comment
 * @param {string} mediaUUID — Supabase UUID for parent media row
 * @param {string} businessAccountId
 * @returns {object}
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

/**
 * Map a parsed Meta message to a DB-ready instagram_dm_messages row.
 * Pure function — no side effects.
 *
 * @param {object} m — parsed message from IG Graph API
 * @param {string|null} conversationUUID
 * @param {string} businessAccountId
 * @param {string} igUserId — business IG User ID
 * @param {string|null} pageId — Facebook Page ID
 * @param {string|null} customerIgId — customer's IGSID
 * @returns {object}
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

module.exports = { normalizeComment, transformMessage };
