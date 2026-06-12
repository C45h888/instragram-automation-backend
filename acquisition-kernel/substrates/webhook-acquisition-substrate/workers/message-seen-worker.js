// substrates/webhook-acquisition-substrate/workers/message-seen-worker.js
// Worker: Instagram DM message-seen (read receipt) webhook semantics.
//   entry[].changes[].field === "message_seen"

const { analyzeFailure } =
  require('../../../../substrates/ig-reliability-substrate');
const { normalizeMessageSeen, EVENT_TYPES } = require('../normalizer');
const { WorkerStateMachine } = require('./_state-machine');

const WORKER_DOMAIN = 'webhook:message-seen';
const WORKER_EVENT_TYPE = EVENT_TYPES.DM_SEEN;

function _validate(change) {
  if (!change || typeof change !== 'object') return { ok: false, reason: 'change_not_object' };
  if (change.field !== 'message_seen') return { ok: false, reason: 'wrong_field_for_message_seen_worker' };
  const value = change.value;
  if (!value || typeof value !== 'object') return { ok: false, reason: 'missing_value' };
  if (!value.message_id) return { ok: false, reason: 'missing_message_id' };
  if (!value.read?.watermark) return { ok: false, reason: 'missing_watermark' };
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
    canonical = normalizeMessageSeen(rawChange, null);
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
  const rawError = { message: reason, source: 'webhook-acquisition:message-seen', eventType: WORKER_EVENT_TYPE };
  let recommendations = [];
  try {
    const analysis = analyzeFailure(rawError, 'webhook:process', 'webhook-acquisition', { accountId, intentId, eventType: 'dm_seen' });
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
