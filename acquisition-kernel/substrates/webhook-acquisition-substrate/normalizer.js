// substrates/webhook-acquisition-substrate/normalizer.js
// Shared shape transform: Meta webhook payload slice → canonical event object.
//
// Owns: pure transforms. No I/O. No state. No governance calls.
// Does NOT own: validation (workers), failure analysis (bedrock), persistence
//               (FSM holds the canonical event in Phase 1; Phase 2 will
//                resolve it to DB rows).
//
// The canonical event object is the FSM-storable shape. Workers emit these
// into CK so the acquisition-fsm can hold them. Phase 2 will add the
// PERSIST_STAGED_EVENT action that turns a canonical event into
// DB_WRITE_REQUESTED → postgres-telemetry-kernel.

const crypto = require('crypto');

// ── Source priority (mounted on ig-reliability PRIORITY_TABLE) ────────────
// All webhook events are CRITICAL per the bedrock's PRIORITY_TABLE.
// The substrate does not compute priority — it asserts the bedrock value.
const WEBHOOK_PRIORITY = 'CRITICAL';
const WEBHOOK_SOURCE = 'webhook:instagram';

// ── Event type enum (single source of truth) ──────────────────────────────
const EVENT_TYPES = Object.freeze({
  COMMENT:        'comment',
  DM_ECHO:        'dm_echo',
  DM_POSTBACK:    'dm_postback',
  MENTION:        'mention',
  STORY_MENTION:  'story_mention',
});

// ── Small helpers ──────────────────────────────────────────────────────────

function _ms(timestamp) {
  // Meta sends: numeric (ms), ISO string, or unix seconds
  if (timestamp == null) return Date.now();
  if (typeof timestamp === 'number') {
    // 13-digit = ms; 10-digit = seconds
    return timestamp > 1e12 ? timestamp : timestamp * 1000;
  }
  const parsed = Date.parse(timestamp);
  return isNaN(parsed) ? Date.now() : parsed;
}

function _eventId(...parts) {
  // Stable event id derived from Meta's source id. Workers pass the raw id
  // through; this is a fallback for entries that lack one.
  return crypto.createHash('sha1').update(parts.filter(Boolean).join(':')).digest('hex').slice(0, 24);
}

function _emptyNormalized() {
  return {
    senderId: null,
    recipientId: null,
    text: null,
    mediaId: null,
    isSelf: false,
    isEcho: false,
    postback: null,
  };
}

// ── Comment webhook (entry[].changes[].field === "comments") ──────────────
// Self-comment shape (Meta docs):
//   value.from = { id, username, self_ig_scoped_id }
//   value.id   = comment id
//   value.text = comment text
//   value.media = { id, media_product_type }
//   value.created_at (or entry.time) = timestamp
function normalizeComment(change, entryTime) {
  const value = change?.value || {};
  const from = value.from || {};
  const media = value.media || {};

  return {
    eventType: EVENT_TYPES.COMMENT,
    igAccountId: null, // resolved by worker from entry.id
    eventId: value.id || _eventId('comment', entryTime, value.text),
    occurredAt: _ms(value.created_at || entryTime),
    source: WEBHOOK_SOURCE,
    priority: WEBHOOK_PRIORITY,
    raw: { change, entryTime },
    normalized: {
      commentId: value.id || null,
      authorInstagramId: from.self_ig_scoped_id || from.id || null,
      authorUsername: from.username || null,
      text: value.text || null,
      mediaId: media.id || null,
      mediaProductType: media.media_product_type || null,
      isSelf: !!from.self_ig_scoped_id,
    },
  };
}

// ── Mention webhook (entry[].changes[].field === "mentions") ──────────────
//   value.id = mention id
//   value.comment_id = optional referenced comment
//   value.media_id = media where mention occurred
//   value.from = { id, username }
function normalizeMention(change, entryTime) {
  const value = change?.value || {};
  const from = value.from || {};

  return {
    eventType: EVENT_TYPES.MENTION,
    igAccountId: null,
    eventId: value.id || _eventId('mention', entryTime, value.media_id),
    occurredAt: _ms(value.created_at || entryTime),
    source: WEBHOOK_SOURCE,
    priority: WEBHOOK_PRIORITY,
    raw: { change, entryTime },
    normalized: {
      mentionId: value.id || null,
      authorInstagramId: from.id || null,
      authorUsername: from.username || null,
      commentId: value.comment_id || null,
      mediaId: value.media_id || null,
    },
  };
}

// ── Story mention webhook (entry[].changes[].field === "story_mentions") ─
//   value.id = mention id
//   value.story_id = story where mention occurred
//   value.from = { id, username }
function normalizeStoryMention(change, entryTime) {
  const value = change?.value || {};
  const from = value.from || {};

  return {
    eventType: EVENT_TYPES.STORY_MENTION,
    igAccountId: null,
    eventId: value.id || _eventId('story_mention', entryTime, value.story_id),
    occurredAt: _ms(value.created_at || entryTime),
    source: WEBHOOK_SOURCE,
    priority: WEBHOOK_PRIORITY,
    raw: { change, entryTime },
    normalized: {
      mentionId: value.id || null,
      authorInstagramId: from.id || null,
      authorUsername: from.username || null,
      storyId: value.story_id || null,
    },
  };
}

// ── DM messaging (entry[].messaging[]) ────────────────────────────────────
// Each entry.messaging[] item is one of:
//   { sender, recipient, timestamp, message: { mid, text, is_echo, is_self } } → DM_ECHO
//   { sender, recipient, timestamp, is_self, postback: { title, payload, mid } } → DM_POSTBACK
//   { sender, recipient, timestamp, message: { ... }, is_echo, is_self, ... } → generic text DM
function normalizeMessaging(item, entryTime) {
  const message = item?.message || null;
  const postback = item?.postback || null;

  // DM postback wins (CTA click semantics)
  if (postback) {
    return {
      eventType: EVENT_TYPES.DM_POSTBACK,
      igAccountId: null,
      eventId: postback.mid || _eventId('dm_postback', item.timestamp, postback.payload),
      occurredAt: _ms(item.timestamp || entryTime),
      source: WEBHOOK_SOURCE,
      priority: WEBHOOK_PRIORITY,
      raw: { item, entryTime },
      normalized: {
        messageId: postback.mid || null,
        senderId: item.sender?.id || null,
        recipientId: item.recipient?.id || null,
        text: null,
        isSelf: !!item.is_self,
        isEcho: false,
        postback: {
          title: postback.title || null,
          payload: postback.payload || null,
        },
      },
    };
  }

  // DM echo / text
  const isEcho = !!message?.is_echo;
  const isSelf = !!message?.is_self;

  return {
    eventType: EVENT_TYPES.DM_ECHO,
    igAccountId: null,
    eventId: message?.mid || _eventId('dm_echo', item.timestamp, message?.text),
    occurredAt: _ms(item.timestamp || entryTime),
    source: WEBHOOK_SOURCE,
    priority: WEBHOOK_PRIORITY,
    raw: { item, entryTime },
    normalized: {
      messageId: message?.mid || null,
      senderId: item.sender?.id || null,
      recipientId: item.recipient?.id || null,
      text: message?.text || null,
      isSelf,
      isEcho,
      postback: null,
    },
  };
}

module.exports = {
  EVENT_TYPES,
  WEBHOOK_PRIORITY,
  WEBHOOK_SOURCE,
  normalizeComment,
  normalizeMention,
  normalizeStoryMention,
  normalizeMessaging,
};
