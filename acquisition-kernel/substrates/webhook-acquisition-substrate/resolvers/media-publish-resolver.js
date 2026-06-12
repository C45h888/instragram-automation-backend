// substrates/webhook-acquisition-substrate/resolvers/media-publish-resolver.js
// Pure transform: canonical media_publish event → instagram_media row.
//
// Media publish events use the same table as the legacy content-parser.
// The content-writer handles the upsert via the existing batch_upsert_posts
// operation (no new operation needed for this path).

const { EVENT_TYPES } = require('../normalizer');

const TABLE_MEDIA = 'instagram_media';
const OPERATION_UPSERT_POSTS = 'batch_upsert_posts';

function resolve(canonicalEvent, context) {
  if (!canonicalEvent || canonicalEvent.eventType !== EVENT_TYPES.MEDIA_PUBLISH) {
    return null;
  }
  const n = canonicalEvent.normalized || {};

  if (!n.mediaId) return { error: 'missing_media_id' };
  if (!context || !context.accountId) return { error: 'missing_account_context' };
  if (!context.businessAccountId) return { error: 'missing_business_account_context' };

  const row = {
    instagram_media_id: n.mediaId,
    business_account_id: context.businessAccountId,
    media_type:         n.mediaType || null,
    published_at:       new Date(canonicalEvent.occurredAt || Date.now()).toISOString(),
    metadata: {
      source: canonicalEvent.source,
      priority: canonicalEvent.priority,
      raw_event_id: canonicalEvent.eventId,
    },
  };

  return {
    table: TABLE_MEDIA,
    operation: OPERATION_UPSERT_POSTS,
    rows: [row],
  };
}

module.exports = { resolve, TABLE_MEDIA, OPERATION_UPSERT_POSTS };
