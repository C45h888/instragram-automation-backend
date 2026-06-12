// substrates/webhook-acquisition-substrate/resolvers/standby-resolver.js
// Pure transform: canonical standby event → ugc_content row.
//
// Standby state is used by the auto-reply orchestrator (future) to pause
// automated responses when a human agent is handling the conversation.

const { EVENT_TYPES } = require('../normalizer');

const TABLE = 'ugc_content';
const OPERATION = 'batch_upsert_ugc';

function resolve(canonicalEvent, context) {
  if (!canonicalEvent || canonicalEvent.eventType !== EVENT_TYPES.STANDBY) {
    return null;
  }
  const n = canonicalEvent.normalized || {};

  if (!n.messageId) return { error: 'missing_message_id' };
  if (!context || !context.accountId) return { error: 'missing_account_context' };
  if (!context.businessAccountId) return { error: 'missing_business_account_context' };

  const eventId = `standby:${n.messageId}:${n.pageId || 'page'}`;

  const row = {
    visitor_post_id:       eventId,
    business_account_id:   context.businessAccountId,
    author_instagram_id:   null,
    author_username:       null,
    ugc_type:              'standby',
    created_time:          new Date(canonicalEvent.occurredAt || Date.now()).toISOString(),
    metadata: {
      page_id:             n.pageId || null,
      message_id:          n.messageId,
      is_active:           true,
      source:              canonicalEvent.source,
      priority:            canonicalEvent.priority,
      raw_event_id:        canonicalEvent.eventId,
    },
  };

  return { table: TABLE, operation: OPERATION, rows: [row] };
}

module.exports = { resolve, TABLE, OPERATION };
