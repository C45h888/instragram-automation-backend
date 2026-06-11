// substrates/webhook-acquisition-substrate/workers/comments-worker.js
// Worker: Instagram self-comment webhook semantics.
//   entry[].changes[].field === "comments"
//
// Mounted on: substrates/ig-reliability-substrate.js (analyzeFailure).
// Lives in:  acquisition-kernel/substrates/webhook-acquisition-substrate/workers/.
//
// Owns: validates entry[].changes[].value shape for comments, normalizes
//        via the substrate's normalizer, dispatches the canonical event
//        to CK so the acquisition-fsm can hold it.
// Does NOT own: failure classification (bedrock), DB writes, retry policy.

const { analyzeFailure } =
  require('../../../../substrates/ig-reliability-substrate');
const { normalizeComment, EVENT_TYPES } = require('../normalizer');

function _validateCommentChange(change) {
  if (!change || typeof change !== 'object') {
    return { ok: false, reason: 'change_not_object' };
  }
  if (change.field !== 'comments') {
    return { ok: false, reason: 'wrong_field_for_comments_worker' };
  }
  const value = change.value;
  if (!value || typeof value !== 'object') {
    return { ok: false, reason: 'missing_value' };
  }
  if (!value.id) {
    return { ok: false, reason: 'missing_comment_id' };
  }
  if (!value.from?.id) {
    return { ok: false, reason: 'missing_from_id' };
  }
  return { ok: true };
}

async function execute(rawChange, accountId, intentId, governance) {
  // ── Layer 1: shape validation ────────────────────────────────────────
  const v = _validateCommentChange(rawChange);
  if (!v.ok) {
    return _emitFailure(rawChange, accountId, intentId, v.reason, governance);
  }

  // ── Layer 2: normalize ───────────────────────────────────────────────
  let canonical;
  try {
    canonical = normalizeComment(rawChange, null);
    canonical.igAccountId = accountId || null;
  } catch (err) {
    return _emitFailure(rawChange, accountId, intentId, `normalizer_threw:${err.message}`, governance);
  }

  // ── Layer 3: dispatch ────────────────────────────────────────────────
  try {
    if (governance && typeof governance.dispatch === 'function') {
      governance.dispatch({
        type: 'WEBHOOK_EVENT_RECEIVED',
        accountId,
        intentId,
        domain: 'webhook:comments',
        eventType: canonical.eventType,
        eventId: canonical.eventId,
        occurredAt: canonical.occurredAt,
        source: canonical.source,
        priority: canonical.priority,
        normalized: canonical.normalized,
        raw: canonical.raw,
      });
    }
  } catch (err) {
    return _emitFailure(rawChange, accountId, intentId, `dispatch_threw:${err.message}`, governance);
  }

  return {
    status: 'staged',
    eventId: canonical.eventId,
    eventType: canonical.eventType,
  };
}

function _emitFailure(rawChange, accountId, intentId, reason, governance) {
  const rawError = {
    message: reason,
    source: 'webhook-acquisition:comments',
    eventType: EVENT_TYPES.COMMENT,
  };

  let recommendations = [];
  try {
    const analysis = analyzeFailure(rawError, 'webhook:process', 'webhook-acquisition', {
      accountId,
      intentId,
      eventType: 'comment',
    });
    recommendations = analysis?.recommendations || [];
  } catch (_) {
    recommendations = [];
  }

  if (governance && typeof governance.dispatch === 'function') {
    try {
      governance.dispatch({
        type: 'WEBHOOK_EVENT_DISCARDED',
        accountId,
        intentId,
        domain: 'webhook:comments',
        eventType: EVENT_TYPES.COMMENT,
        reason,
        recommendations,
      });
    } catch (_) {}
  }

  return {
    status: 'discarded',
    eventId: null,
    eventType: EVENT_TYPES.COMMENT,
    reason,
  };
}

module.exports = { execute };
