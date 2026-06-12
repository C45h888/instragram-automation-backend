// substrates/webhook-acquisition-substrate/resolvers/tags-resolver.js
// Pure transform: canonical tag event → ugc_content row.

const { EVENT_TYPES } = require('../normalizer');

const TABLE = 'ugc_content';
const OPERATION = 'batch_upsert_ugc';

function resolve(canonicalEvent, context) {
  if (!canonicalEvent || canonicalEvent.eventType !== EVENT_TYPES.TAG) {
    return null;
  }
  const n = canonicalEvent.normalized || {};

  if (!n.mediaId) return { error: 'missing_media_id' };
  if (!context || !context.accountId) return { error: 'missing_account_context' };
  if (!context.businessAccountId) return { error: 'missing_business_account_context' };

  // Composite key: mediaId + author + businessAccount
  const eventId = `tag:${n.mediaId}:${n.authorInstagramId || 'unknown'}:${context.businessAccountId}`;

  const row = {
    visitor_post_id:       eventId,
    business_account_id:   context.businessAccountId,
    author_instagram_id:   n.authorInstagramId || null,
    author_username:       n.authorUsername || null,
    ugc_type:              'tag',
    media_url:             n.mediaUrl || null,
    created_time:          new Date(canonicalEvent.occurredAt || Date.now()).toISOString(),
    metadata: {
      instagram_media_id:  n.mediaId,
      source:              canonicalEvent.source,
      priority:            canonicalEvent.priority,
      raw_event_id:        canonicalEvent.eventId,
    },
  };

  return { table: TABLE, operation: OPERATION, rows: [row] };
}

module.exports = { resolve, TABLE, OPERATION };
