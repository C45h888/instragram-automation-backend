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
  COMMENT:           'comment',
  COMMENT_REPLY:     'comment_reply',
  LIVE_COMMENT:      'live_comment',
  DM_ECHO:           'dm_echo',
  DM_POSTBACK:       'dm_postback',
  DM_REACTION:       'dm_reaction',
  DM_SEEN:           'dm_seen',
  MENTION:           'mention',
  STORY_MENTION:     'story_mention',
  STANDBY:           'standby',
  MEDIA_PUBLISH:     'media_publish',
  TAG:               'tag',
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

// ── Comment reply (entry[].changes[].field === "comments" with reply) ──────
// Meta delivers comment replies under field="comments" with parent_id set
// (value.parent_id is the id of the comment being replied to).
//   value.id          = reply id
//   value.parent_id   = parent comment id
//   value.text        = reply text
//   value.from        = { id, username }
//   value.media       = { id, media_product_type }
function normalizeCommentReply(change, entryTime) {
  const value = change?.value || {};
  const from = value.from || {};
  const media = value.media || {};

  return {
    eventType: EVENT_TYPES.COMMENT_REPLY,
    igAccountId: null,
    eventId: value.id || _eventId('comment_reply', entryTime, value.text, value.parent_id),
    occurredAt: _ms(value.created_at || entryTime),
    source: WEBHOOK_SOURCE,
    priority: WEBHOOK_PRIORITY,
    raw: { change, entryTime },
    normalized: {
      replyId: value.id || null,
      parentCommentId: value.parent_id || null,
      authorInstagramId: from.self_ig_scoped_id || from.id || null,
      authorUsername: from.username || null,
      text: value.text || null,
      mediaId: media.id || null,
    },
  };
}

// ── Live comment (entry[].changes[].field === "live_comments") ──────────────
// Comments on a live broadcast video.
//   value.id         = comment id
//   value.from       = { id, username }
//   value.media      = { id } — the live video
//   value.text       = comment text
function normalizeLiveComment(change, entryTime) {
  const value = change?.value || {};
  const from = value.from || {};
  const media = value.media || {};

  return {
    eventType: EVENT_TYPES.LIVE_COMMENT,
    igAccountId: null,
    eventId: value.id || _eventId('live_comment', entryTime, value.text),
    occurredAt: _ms(value.created_at || entryTime),
    source: WEBHOOK_SOURCE,
    priority: WEBHOOK_PRIORITY,
    raw: { change, entryTime },
    normalized: {
      commentId: value.id || null,
      authorInstagramId: from.id || null,
      authorUsername: from.username || null,
      text: value.text || null,
      mediaId: media.id || null,
    },
  };
}

// ── Message reaction (entry[].changes[].field === "message_reactions") ─────
// Reactions on DMs (emoji reactions to a specific message).
//   value.message_id = the message being reacted to
//   value.reaction   = emoji character
//   value.action     = "react" | "unreact"
//   value.sender     = { id }
function normalizeMessageReaction(change, entryTime) {
  const value = change?.value || {};
  const sender = value.sender || {};

  return {
    eventType: EVENT_TYPES.DM_REACTION,
    igAccountId: null,
    eventId: _eventId('dm_reaction', value.message_id, value.reaction, value.action, entryTime),
    occurredAt: _ms(entryTime),
    source: WEBHOOK_SOURCE,
    priority: WEBHOOK_PRIORITY,
    raw: { change, entryTime },
    normalized: {
      messageId: value.message_id || null,
      reaction: value.reaction || null,
      action: value.action || null,
      senderId: sender.id || null,
    },
  };
}

// ── Message seen / read receipt (entry[].changes[].field === "message_seen")
//   value.message_id = the message that was read
//   value.read       = { watermark (unix ms) }
//   value.sender     = { id } — the reader
function normalizeMessageSeen(change, entryTime) {
  const value = change?.value || {};
  const read = value.read || {};
  const sender = value.sender || {};

  return {
    eventType: EVENT_TYPES.DM_SEEN,
    igAccountId: null,
    eventId: _eventId('dm_seen', value.message_id, sender.id, read.watermark),
    occurredAt: _ms(read.watermark || entryTime),
    source: WEBHOOK_SOURCE,
    priority: WEBHOOK_PRIORITY,
    raw: { change, entryTime },
    normalized: {
      messageId: value.message_id || null,
      senderId: sender.id || null,
      watermark: read.watermark || null,
    },
  };
}

// ── Standby channel (entry[].changes[].field === "standby") ────────────────
// Standby messages come through a separate channel when a human agent
// (Page Inbox / human take-over) is handling the conversation. Webhook
// delivers a thin envelope so the business can pause automated replies.
//   value.message_id = the message handled by the human
//   value.page_id    = the Page id where the standby is active
function normalizeStandby(change, entryTime) {
  const value = change?.value || {};

  return {
    eventType: EVENT_TYPES.STANDBY,
    igAccountId: null,
    eventId: _eventId('standby', value.message_id, value.page_id, entryTime),
    occurredAt: _ms(entryTime),
    source: WEBHOOK_SOURCE,
    priority: WEBHOOK_PRIORITY,
    raw: { change, entryTime },
    normalized: {
      messageId: value.message_id || null,
      pageId: value.page_id || null,
    },
  };
}

// ── Media publish (entry[].changes[].field === "media") ────────────────────
// Delivered when the IG business account publishes a new media item.
//   value.media_id   = the new media id
//   value.media_type = "IMAGE" | "VIDEO" | "CAROUSEL_ALBUM" | ...
function normalizeMediaPublish(change, entryTime) {
  const value = change?.value || {};

  return {
    eventType: EVENT_TYPES.MEDIA_PUBLISH,
    igAccountId: null,
    eventId: value.media_id || _eventId('media_publish', entryTime),
    occurredAt: _ms(entryTime),
    source: WEBHOOK_SOURCE,
    priority: WEBHOOK_PRIORITY,
    raw: { change, entryTime },
    normalized: {
      mediaId: value.media_id || null,
      mediaType: value.media_type || null,
    },
  };
}

// ── Tag (entry[].changes[].field === "tags") ───────────────────────────────
// A user was tagged in a photo or video (other people's media).
//   value.media_id  = media containing the tag
//   value.media_url = permalink
//   value.from      = { id, username } of the media author
function normalizeTag(change, entryTime) {
  const value = change?.value || {};
  const from = value.from || {};

  return {
    eventType: EVENT_TYPES.TAG,
    igAccountId: null,
    eventId: _eventId('tag', value.media_id, from.id, entryTime),
    occurredAt: _ms(entryTime),
    source: WEBHOOK_SOURCE,
    priority: WEBHOOK_PRIORITY,
    raw: { change, entryTime },
    normalized: {
      mediaId: value.media_id || null,
      mediaUrl: value.media_url || null,
      authorInstagramId: from.id || null,
      authorUsername: from.username || null,
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
  normalizeCommentReply,
  normalizeLiveComment,
  normalizeMessageReaction,
  normalizeMessageSeen,
  normalizeStandby,
  normalizeMediaPublish,
  normalizeTag,
};
