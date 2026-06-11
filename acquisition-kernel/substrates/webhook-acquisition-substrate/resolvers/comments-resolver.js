// substrates/webhook-acquisition-substrate/resolvers/comments-resolver.js
// Pure transform: canonical comment event → instagram_comments row.
//
// Owns: shape mapping only. No I/O. No state. No governance calls.
// Does NOT own: account UUID resolution, media hydration, DB writes.
//
// Contract:
//   Input  — canonical event (from FSM _stagedEvents[accountId][i].normalized)
//            + resolved context { accountId, mediaId (internal UUID), businessAccountId }
//   Output — { table, operation, rows } for DB_WRITE_REQUESTED
//
// The FSM supplies the resolved context — the resolver does not fetch it.
// If context is missing, the resolver returns null and the FSM handles it.

const { EVENT_TYPES } = require('../normalizer');

const TABLE_COMMENTS = 'instagram_comments';
const OPERATION_UPSERT_COMMENTS = 'batch_upsert_comments';

function resolve(canonicalEvent, context) {
  // ── Guard: only handle comment events ─────────────────────────────────
  if (!canonicalEvent || canonicalEvent.eventType !== EVENT_TYPES.COMMENT) {
    return null;
  }
  const n = canonicalEvent.normalized || {};

  // ── Guard: required fields ───────────────────────────────────────────
  if (!n.commentId) return { error: 'missing_comment_id' };
  if (!context || !context.accountId) return { error: 'missing_account_context' };
  if (!context.businessAccountId) return { error: 'missing_business_account_context' };

  // ── Build row (matches comments-writer expected shape) ──────────────
  const row = {
    instagram_comment_id: n.commentId,
    business_account_id:   context.businessAccountId,
    author_instagram_id:   n.authorInstagramId || null,
    author_username:       n.authorUsername || null,
    text:                  n.text || null,
    media_id:              context.mediaId || null,        // internal UUID
    instagram_media_id:    n.mediaId || null,             // IG's id (for hydration)
    is_self:               !!n.isSelf,
    created_at:            new Date(canonicalEvent.occurredAt || Date.now()).toISOString(),
    metadata: {
      source: canonicalEvent.source,
      priority: canonicalEvent.priority,
      raw_event_id: canonicalEvent.eventId,
    },
  };

  return {
    table: TABLE_COMMENTS,
    operation: OPERATION_UPSERT_COMMENTS,
    rows: [row],
  };
}

module.exports = { resolve, TABLE_COMMENTS, OPERATION_UPSERT_COMMENTS };
