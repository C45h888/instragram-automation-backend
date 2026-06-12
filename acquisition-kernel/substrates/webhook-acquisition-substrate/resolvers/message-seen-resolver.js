// substrates/webhook-acquisition-substrate/resolvers/message-seen-resolver.js
// Pure transform: canonical dm_seen event → update instagram_dm_messages.seen_at.
//
// Note: this is an UPDATE operation, not a row insert. The messages-writer
// dispatches by operation name.

const { EVENT_TYPES } = require('../normalizer');

const TABLE_MESSAGES = 'instagram_dm_messages';
const OPERATION_UPSERT_SEEN = 'batch_upsert_dm_seen';

function resolve(canonicalEvent, context) {
  if (!canonicalEvent || canonicalEvent.eventType !== EVENT_TYPES.DM_SEEN) {
    return null;
  }
  const n = canonicalEvent.normalized || {};

  if (!n.messageId) return { error: 'missing_message_id' };
  if (!context || !context.accountId) return { error: 'missing_account_context' };
  if (!context.businessAccountId) return { error: 'missing_business_account_context' };

  // messages-writer looks at: instagram_message_id (or messageId) + senderId + seenAt
  const row = {
    instagram_message_id: n.messageId,
    messageId:            n.messageId,
    senderId:             n.senderId || null,
    seenAt:               new Date(n.watermark || canonicalEvent.occurredAt || Date.now()).toISOString(),
    business_account_id:  context.businessAccountId,
    metadata: {
      source: canonicalEvent.source,
      priority: canonicalEvent.priority,
      raw_event_id: canonicalEvent.eventId,
    },
  };

  return {
    table: TABLE_MESSAGES,
    operation: OPERATION_UPSERT_SEEN,
    rows: [row],
  };
}

module.exports = { resolve, TABLE_MESSAGES, OPERATION_UPSERT_SEEN };
