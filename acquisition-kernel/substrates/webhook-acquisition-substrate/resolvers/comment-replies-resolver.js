// substrates/webhook-acquisition-substrate/resolvers/comment-replies-resolver.js
// Pure transform: canonical comment_reply event → instagram_comments row
// (reply rows have parent_comment_id set to disambiguate from top-level comments).
//
// Owns: shape mapping only. No I/O. No state. No governance calls.
// Does NOT own: account UUID resolution, DB writes.

const { EVENT_TYPES } = require('../normalizer');

const TABLE_COMMENTS = 'instagram_comments';
const OPERATION_UPSERT_REPLIES = 'batch_upsert_comment_replies';

function resolve(canonicalEvent, context) {
  if (!canonicalEvent || canonicalEvent.eventType !== EVENT_TYPES.COMMENT_REPLY) {
    return null;
  }
  const n = canonicalEvent.normalized || {};

  if (!n.replyId) return { error: 'missing_reply_id' };
  if (!n.parentCommentId) return { error: 'missing_parent_comment_id' };
  if (!context || !context.accountId) return { error: 'missing_account_context' };
  if (!context.businessAccountId) return { error: 'missing_business_account_context' };

  const row = {
    instagram_comment_id: n.replyId,
    business_account_id:   context.businessAccountId,
    author_instagram_id:   n.authorInstagramId || null,
    author_username:       n.authorUsername || null,
    text:                  n.text || null,
    media_id:              context.mediaId || null,
    instagram_media_id:    n.mediaId || null,
    parent_comment_id:     n.parentCommentId,
    is_reply:              true,
    is_self:               false,
    created_at:            new Date(canonicalEvent.occurredAt || Date.now()).toISOString(),
    metadata: {
      source: canonicalEvent.source,
      priority: canonicalEvent.priority,
      raw_event_id: canonicalEvent.eventId,
    },
  };

  return {
    table: TABLE_COMMENTS,
    operation: OPERATION_UPSERT_REPLIES,
    rows: [row],
  };
}

module.exports = { resolve, TABLE_COMMENTS, OPERATION_UPSERT_REPLIES };
