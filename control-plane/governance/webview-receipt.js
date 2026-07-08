// control-plane/governance/webview-receipt.js
// Receipt emitter for the WebView reactive membrane (Pass 7 / S5).
//
// Writes a DecisionReceipt record to the
// `lineage:webview:read-results` Redis Stream using XADD.
//
// XADD field schema (mirrors the producer-side commands.rs:147 XADD
// shape at lineage:webview:transitions — the same 4 base fields +
// the local-enrichment ones added by the consumer):
//   transition_id            — copied from the input transition
//   correlation_id           — copied if present
//   decision                 — one of 'ACCEPTED'|'REJECTED'|'TIMEOUT'
//                             (V34 enforced)
//   decided_at_epoch_ms      — wall-clock ms when computeDecision returned
//   reason                   — present iff decision !== 'ACCEPTED'
//   rule_fingerprint         — present iff the dispatchResult carried it
//   lineage_id               — present iff the dispatch attached one
//   domain                   — copied from the input transition
//   from_state               — copied
//   to_state                 — copied
//   event                    — copied
//
// Failure semantics (spec invariant I36): the emitter NEVER rolls
// back the dispatch. If XADD fails, it returns { ok:false,
// error } and continues — the pump loop observes the failure via
// the returned flag and emits the READ_RESULTS_WRITE_FAILED
// telemetry event at its layer.

'use strict';

const { getRedisClient } = require('../../config/redis');

const READ_RESULTS_STREAM = 'lineage:webview:read-results';
const MAX_FIELD_LENGTH = 1024; // cap any single field to keep XADD healthy

/**
 * Emit a DecisionReceipt to lineage:webview:read-results.
 *
 * @param {object} receipt
 *   { transition_id, correlation_id?, decision, decided_at_epoch_ms,
 *     reason?, rule_fingerprint?, lineage_id?, domain?, from_state?,
 *     to_state?, event? }
 *
 * @returns {Promise<{ ok: boolean, stream_id?: string, error?: string }>}
 *   ok=false if Redis was unreachable or XADD failed; the
 *   caller (webview-stream.js) emits READ_RESULTS_WRITE_FAILED
 *   when this happens, but does NOT roll back.
 */
async function emitReceipt(receipt) {
  if (!receipt || typeof receipt !== 'object') {
    return { ok: false, error: 'emitReceipt: missing receipt object' };
  }
  if (typeof receipt.transition_id !== 'string'
      || receipt.transition_id.length === 0) {
    return { ok: false, error: 'emitReceipt: missing transition_id' };
  }
  if (typeof receipt.decision !== 'string'
      || !['ACCEPTED', 'REJECTED', 'TIMEOUT'].includes(receipt.decision)) {
    return { ok: false, error: `emitReceipt: invalid decision '${receipt.decision}' (must be ACCEPTED|REJECTED|TIMEOUT)` };
  }

  // Cap long string fields — XADD allows large values, but a runaway
  // reason field would bloat the stream.
  const c = (s) => (typeof s === 'string' && s.length > MAX_FIELD_LENGTH)
    ? s.slice(0, MAX_FIELD_LENGTH) : s;
  const str = (v) => v === undefined || v === null ? null : c(String(v));
  const num = (v) => (typeof v === 'number' && Number.isFinite(v)) ? v : null;

  // Build the XADD field list. ioredis accepts a flat arg array.
  const fields = [
    'transition_id',       c(receipt.transition_id),
    'decision',            receipt.decision,
    'decided_at_epoch_ms', num(receipt.decided_at_epoch_ms) ?? String(Date.now()),
  ];
  if (receipt.correlation_id) {
    fields.push('correlation_id', c(receipt.correlation_id));
  }
  if (receipt.reason) {
    fields.push('reason', c(receipt.reason));
  }
  if (receipt.rule_fingerprint) {
    fields.push('rule_fingerprint', c(receipt.rule_fingerprint));
  }
  if (receipt.lineage_id) {
    fields.push('lineage_id', c(receipt.lineage_id));
  }
  if (receipt.domain) {
    fields.push('domain', c(receipt.domain));
  }
  if (receipt.from_state) {
    fields.push('from_state', c(receipt.from_state));
  }
  if (receipt.to_state) {
    fields.push('to_state', c(receipt.to_state));
  }
  if (receipt.event) {
    fields.push('event', c(receipt.event));
  }

  const redis = getRedisClient();
  if (!redis || redis.status !== 'ready') {
    return { ok: false, error: 'redis unavailable' };
  }

  try {
    // XADD <stream> * <field> <value> [<field> <value> ...]
    const streamId = await redis.xadd(READ_RESULTS_STREAM, '*', ...fields);
    if (!streamId || typeof streamId !== 'string') {
      return { ok: false, error: `XADD returned no stream id (got ${streamId})` };
    }
    return { ok: true, stream_id: streamId };
  } catch (err) {
    return { ok: false, error: `XADD failed: ${err && err.message ? err.message : String(err)}` };
  }
}

module.exports = {
  emitReceipt,
  READ_RESULTS_STREAM,
};
