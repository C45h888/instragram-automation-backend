// substrates/webhook-acquisition-substrate/resolvers/message-reactions-resolver.js
// Pure transform: canonical dm_reaction event → ugc_content row.

const { EVENT_TYPES } = require('../normalizer');

const TABLE = 'ugc_content';
const OPERATION = 'batch_upsert_ugc';

function resolve(canonicalEvent, context) {
  if (!canonicalEvent || canonicalEvent.eventType !== EVENT_TYPES.DM_REACTION) {
    return null;
  }
  const n = canonicalEvent.normalized || {};

  if (!n.messageId) return { error: 'missing_message_id' };
  if (!n.reaction) return { error: 'missing_reaction_emoji' };
  if (!n.senderId) return { error: 'missing_sender_id' };
  if (!context || !context.accountId) return { error: 'missing_account_context' };
  if (!context.businessAccountId) return { error: 'missing_business_account_context' };

  // Composite key: messageId + sender + reaction + action
  const eventId = `dm_reaction:${n.messageId}:${n.senderId}:${n.reaction}:${n.action || 'react'}`;

  const row = {
    visitor_post_id:       eventId,
    business_account_id:   context.businessAccountId,
    author_instagram_id:   n.senderId,
    author_username:       null,
    ugc_type:              'dm_reaction',
    created_time:          new Date(canonicalEvent.occurredAt || Date.now()).toISOString(),
    metadata: {
      message_id:          n.messageId,
      reaction:            n.reaction,
      action:              n.action || 'react',
      source:              canonicalEvent.source,
      priority:            canonicalEvent.priority,
      raw_event_id:        canonicalEvent.eventId,
    },
  };

  return { table: TABLE, operation: OPERATION, rows: [row] };
}

module.exports = { resolve, TABLE, OPERATION };
