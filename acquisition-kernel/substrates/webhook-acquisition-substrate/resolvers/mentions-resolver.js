// substrates/webhook-acquisition-substrate/resolvers/mentions-resolver.js
// Pure transform: canonical mention event → ugc_content row (mentions are UGC).
//
// Owns: shape mapping only. No I/O. No state. No governance calls.
//
// Contract:
//   Input  — canonical mention event
//            + resolved context { accountId, businessAccountId, mediaId (internal UUID) }
//   Output — { table, operation, rows } for DB_WRITE_REQUESTED
//
// Phase 2 placeholder: the ugc table is the existing target for UGC-style
// events. If a dedicated mentions table is added later, the resolver
// changes the target table; the FSM contract is unchanged.

const { EVENT_TYPES } = require('../normalizer');

const TABLE_UGC = 'ugc_content';
const OPERATION_UPSERT_UGC = 'batch_upsert_ugc';

function resolve(canonicalEvent, context) {
  if (!canonicalEvent || canonicalEvent.eventType !== EVENT_TYPES.MENTION) {
    return null;
  }
  const n = canonicalEvent.normalized || {};

  if (!n.mentionId) return { error: 'missing_mention_id' };
  if (!context || !context.accountId) return { error: 'missing_account_context' };
  if (!context.businessAccountId) return { error: 'missing_business_account_context' };

  const eventId = `mention:${n.mentionId}:${context.businessAccountId}`;

  const row = {
    visitor_post_id:        eventId,
    business_account_id:    context.businessAccountId,
    author_instagram_id:    n.authorInstagramId || null,
    author_username:        n.authorUsername || null,
    ugc_type:               'mention',
    created_time:           new Date(canonicalEvent.occurredAt || Date.now()).toISOString(),
    metadata: {
      source_media_id:      context.mediaId || null,
      instagram_media_id:   n.mediaId || null,
      referenced_comment_id: n.commentId || null,
      source:               canonicalEvent.source,
      priority:             canonicalEvent.priority,
      raw_event_id:         canonicalEvent.eventId,
    },
  };

  return {
    table: TABLE_UGC,
    operation: OPERATION_UPSERT_UGC,
    rows: [row],
  };
}

module.exports = { resolve, TABLE_UGC, OPERATION_UPSERT_UGC };
