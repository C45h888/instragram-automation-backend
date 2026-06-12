// substrates/webhook-acquisition-substrate/resolvers/live-comments-resolver.js
// Pure transform: canonical live_comment event → ugc_content row.

const { EVENT_TYPES } = require('../normalizer');

const TABLE = 'ugc_content';
const OPERATION = 'batch_upsert_ugc';

function resolve(canonicalEvent, context) {
  if (!canonicalEvent || canonicalEvent.eventType !== EVENT_TYPES.LIVE_COMMENT) {
    return null;
  }
  const n = canonicalEvent.normalized || {};

  if (!n.commentId) return { error: 'missing_live_comment_id' };
  if (!context || !context.accountId) return { error: 'missing_account_context' };
  if (!context.businessAccountId) return { error: 'missing_business_account_context' };

  const eventId = `live_comment:${n.commentId}:${context.businessAccountId}`;

  const row = {
    visitor_post_id:       eventId,
    business_account_id:   context.businessAccountId,
    author_instagram_id:   n.authorInstagramId || null,
    author_username:       n.authorUsername || null,
    ugc_type:              'live_comment',
    message:               n.text || null,
    created_time:          new Date(canonicalEvent.occurredAt || Date.now()).toISOString(),
    metadata: {
      live_media_id:       n.mediaId || null,
      source:              canonicalEvent.source,
      priority:            canonicalEvent.priority,
      raw_event_id:        canonicalEvent.eventId,
    },
  };

  return { table: TABLE, operation: OPERATION, rows: [row] };
}

module.exports = { resolve, TABLE, OPERATION };
