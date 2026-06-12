// substrates/webhook-acquisition-substrate/resolvers/index.js
// Resolver index: route a canonical event to the right pure transform.
//
// Owns: routing only. No I/O. No state. No governance calls.
// Does NOT own: hydration, account resolution, DB writes.
//
// Used by the acquisition-fsm during PERSIST_STAGED_EVENT. The FSM
// supplies the resolved context; this module picks the resolver and
// calls it.

const { EVENT_TYPES } = require('../normalizer');

const commentsResolver        = require('./comments-resolver');
const messagesResolver        = require('./messages-resolver');
const mentionsResolver        = require('./mentions-resolver');
const storyMentionsResolver   = require('./story-mentions-resolver');
const commentRepliesResolver  = require('./comment-replies-resolver');
const liveCommentsResolver    = require('./live-comments-resolver');
const messageReactionsResolver= require('./message-reactions-resolver');
const messageSeenResolver     = require('./message-seen-resolver');
const standbyResolver         = require('./standby-resolver');
const mediaPublishResolver    = require('./media-publish-resolver');
const tagsResolver            = require('./tags-resolver');

const _RESOLVER_MAP = {
  [EVENT_TYPES.COMMENT]:        commentsResolver,
  [EVENT_TYPES.DM_ECHO]:        messagesResolver,
  [EVENT_TYPES.DM_POSTBACK]:    messagesResolver,
  [EVENT_TYPES.MENTION]:        mentionsResolver,
  [EVENT_TYPES.STORY_MENTION]:  storyMentionsResolver,
  [EVENT_TYPES.COMMENT_REPLY]:  commentRepliesResolver,
  [EVENT_TYPES.LIVE_COMMENT]:   liveCommentsResolver,
  [EVENT_TYPES.DM_REACTION]:    messageReactionsResolver,
  [EVENT_TYPES.DM_SEEN]:        messageSeenResolver,
  [EVENT_TYPES.STANDBY]:        standbyResolver,
  [EVENT_TYPES.MEDIA_PUBLISH]:  mediaPublishResolver,
  [EVENT_TYPES.TAG]:            tagsResolver,
};

function resolveForEvent(canonicalEvent, context) {
  if (!canonicalEvent || !canonicalEvent.eventType) {
    return { error: 'missing_event_type' };
  }
  const resolver = _RESOLVER_MAP[canonicalEvent.eventType];
  if (!resolver) {
    return { error: `unsupported_event_type:${canonicalEvent.eventType}` };
  }
  return resolver.resolve(canonicalEvent, context);
}

module.exports = { resolveForEvent, _RESOLVER_MAP };
