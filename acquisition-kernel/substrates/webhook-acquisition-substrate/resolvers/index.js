// substrates/webhook-acquisition-substrate/resolvers/index.js
// Resolver index: route a canonical event to the right pure transform.
//
// Owns: routing only. No I/O. No state. No governance calls.
// Does NOT own: hydration, account resolution, DB writes.
//
// Used by the acquisition-fsm during PERSIST_STAGED_EVENT. The FSM
// supplies the resolved context; this module picks the resolver and
// calls it.

const commentsResolver       = require('./comments-resolver');
const messagesResolver       = require('./messages-resolver');
const mentionsResolver       = require('./mentions-resolver');
const storyMentionsResolver  = require('./story-mentions-resolver');

function resolveForEvent(canonicalEvent, context) {
  if (!canonicalEvent || !canonicalEvent.eventType) {
    return { error: 'missing_event_type' };
  }
  switch (canonicalEvent.eventType) {
    case 'comment':        return commentsResolver.resolve(canonicalEvent, context);
    case 'dm_echo':        return messagesResolver.resolve(canonicalEvent, context);
    case 'dm_postback':    return messagesResolver.resolve(canonicalEvent, context);
    case 'mention':        return mentionsResolver.resolve(canonicalEvent, context);
    case 'story_mention':  return storyMentionsResolver.resolve(canonicalEvent, context);
    default:               return { error: `unsupported_event_type:${canonicalEvent.eventType}` };
  }
}

module.exports = { resolveForEvent };
