// substrates/webhook-acquisition-substrate/resolvers/story-mentions-resolver.js
// Pure transform: canonical story_mention event → ugc_content row.
//
// Owns: shape mapping only. No I/O. No state. No governance calls.
//
// Contract:
//   Input  — canonical story_mention event
//            + resolved context { accountId, businessAccountId }
//   Output — { table, operation, rows } for DB_WRITE_REQUESTED
//
// Story mentions have no media linkage (the story may be ephemeral), so
// the resolver writes a UGC row keyed by the mention id.

const { EVENT_TYPES } = require('../normalizer');

const TABLE_UGC = 'ugc_content';
const OPERATION_UPSERT_UGC = 'batch_upsert_ugc';

function resolve(canonicalEvent, context) {
  if (!canonicalEvent || canonicalEvent.eventType !== EVENT_TYPES.STORY_MENTION) {
    return null;
  }
  const n = canonicalEvent.normalized || {};

  if (!n.mentionId) return { error: 'missing_story_mention_id' };
  if (!context || !context.accountId) return { error: 'missing_account_context' };
  if (!context.businessAccountId) return { error: 'missing_business_account_context' };

  const row = {
    ugc_id:              n.mentionId,
    business_account_id: context.businessAccountId,
    author_instagram_id: n.authorInstagramId || null,
    author_username:     n.authorUsername || null,
    ugc_type:            'story_mention',
    source_story_id:     n.storyId || null,
    captured_at:         new Date(canonicalEvent.occurredAt || Date.now()).toISOString(),
    metadata: {
      source: canonicalEvent.source,
      priority: canonicalEvent.priority,
      raw_event_id: canonicalEvent.eventId,
    },
  };

  return {
    table: TABLE_UGC,
    operation: OPERATION_UPSERT_UGC,
    rows: [row],
  };
}

module.exports = { resolve, TABLE_UGC, OPERATION_UPSERT_UGC };
