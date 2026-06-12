// substrates/webhook-acquisition-substrate/workers/live-comments-worker.js
// Worker: Instagram live-video comment webhook semantics.
//   entry[].changes[].field === "live_comments"

const { analyzeFailure } =
  require('../../../../substrates/ig-reliability-substrate');
const { normalizeLiveComment, EVENT_TYPES } = require('../normalizer');
const { WorkerStateMachine } = require('./_state-machine');

const WORKER_DOMAIN = 'webhook:live-comments';
const WORKER_EVENT_TYPE = EVENT_TYPES.LIVE_COMMENT;

function _validate(change) {
  if (!change || typeof change !== 'object') return { ok: false, reason: 'change_not_object' };
  if (change.field !== 'live_comments') return { ok: false, reason: 'wrong_field_for_live_comments_worker' };
  const value = change.value;
  if (!value || typeof value !== 'object') return { ok: false, reason: 'missing_value' };
  if (!value.id) return { ok: false, reason: 'missing_comment_id' };
  if (!value.from?.id) return { ok: false, reason: 'missing_from_id' };
  return { ok: true };
}

async function execute(rawChange, accountId, intentId, governance) {
  const wstate = new WorkerStateMachine({
    accountId, intentId, eventType: WORKER_EVENT_TYPE,
    domain: WORKER_DOMAIN, governance,
  });

  wstate.transition('VALIDATING');
  const v = _validate(rawChange);
  if (!v.ok) {
    wstate.transition('FAILED_VALIDATION', v.reason);
    return _emitFailure(rawChange, accountId, intentId, v.reason, governance);
  }

  wstate.transition('NORMALIZING');
  let canonical;
  try {
    canonical = normalizeLiveComment(rawChange, null);
    canonical.igAccountId = accountId || null;
  } catch (err) {
    wstate.transition('FAILED_NORMALIZE', err.message);
    return _emitFailure(rawChange, accountId, intentId, `normalizer_threw:${err.message}`, governance);
  }

  wstate.transition('DISPATCHING');
  try {
    if (governance && typeof governance.dispatch === 'function') {
      governance.dispatch({
        type: 'WEBHOOK_EVENT_RECEIVED', accountId, intentId,
        domain: WORKER_DOMAIN,
        eventType: canonical.eventType, eventId: canonical.eventId,
        occurredAt: canonical.occurredAt, source: canonical.source, priority: canonical.priority,
        normalized: canonical.normalized, raw: canonical.raw,
      });
    }
  } catch (err) {
    wstate.transition('FAILED_DISPATCH', err.message);
    return _emitFailure(rawChange, accountId, intentId, `dispatch_threw:${err.message}`, governance);
  }

  wstate.transition('STAGED');
  return { status: 'staged', eventId: canonical.eventId, eventType: canonical.eventType };
}

function _emitFailure(rawChange, accountId, intentId, reason, governance) {
  const rawError = { message: reason, source: 'webhook-acquisition:live-comments', eventType: WORKER_EVENT_TYPE };
  let recommendations = [];
  try {
    const analysis = analyzeFailure(rawError, 'webhook:process', 'webhook-acquisition', { accountId, intentId, eventType: 'live_comment' });
    recommendations = analysis?.recommendations || [];
  } catch (_) { recommendations = []; }
  if (governance && typeof governance.dispatch === 'function') {
    try {
      governance.dispatch({ type: 'WEBHOOK_EVENT_DISCARDED', accountId, intentId, domain: WORKER_DOMAIN, eventType: WORKER_EVENT_TYPE, reason, recommendations });
    } catch (_) {}
  }
  return { status: 'discarded', eventId: null, eventType: WORKER_EVENT_TYPE, reason };
}

module.exports = { execute };
