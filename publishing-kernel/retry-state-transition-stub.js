// publishing-kernel/retry-state-transition-stub.js
// Phase 1 (base) — Retry state transition stub for the publishing kernel.
//
// CONSTITUTIONAL CONTRACT (Phase 1 base — 2026-06-10):
//   - Pass-through adapter. NO state. NO retry counting.
//   - Routes publishing-kernel's local events to the canonical
//     retry-cadence-kernel signals (RETRY_REQUESTED /
//     RETRY_EXHAUSTED / RETRY_IN_PROGRESS).
//   - The canonical retry state lives in retry-cadence-kernel.
//     The publishing kernel is a CLIENT, not a co-owner.
//   - Owns the workerName → publish:* domain mapping so the
//     publish substrate's RETRY_REQUESTED emits use the correct
//     publish domain (publish:post, publish:story, publish:comment,
//     publish:message) — the publish retry worker bindings in
//     retry-cadence-kernel key on these domains.
//
// Why this exists:
//   Pre-purge, the publish substrates emitted RETRY_REQUESTED
//   with domain: 'content' / domain: 'engagement' — the read-side
//   domain names. This routed publish failures through the
//   read-side retry workers. The fix: the stub maps the
//   substrate's workerName to the publish:* domain and emits
//   RETRY_REQUESTED with the correct publish-domain key.
//
// Does NOT own: retry state, retry policy, retry scheduling,
//               classification, error normalisation.
// Does own: the workerName → publish:* domain mapping and the
//           canonical retry-cadence signal emission.
//
// API:
//   requestRetry({ accountId, intentId, workerName, params, error,
//                  errorShape, errorCategory, retryAfterMs })
//     → maps workerName → publish:* domain → emits RETRY_REQUESTED
//   notifyExhausted({ accountId, intentId, error, ... })
//     → emits RETRY_EXHAUSTED
//   notifyInProgress({ accountId, intentId, retryCount, delayMs, ... })
//     → emits RETRY_IN_PROGRESS

let _governance = null;

// ── workerName → publish:* domain mapping ─────────────────────────
// Source: publishing-kernel/substrates/content/index.js (workers:
//   'posts' → publish_post / repost_ugc, 'stories' → publish_story)
//          publishing-kernel/substrates/engagement/index.js (workers:
//   'comments' → reply_comment, 'messages' → reply_dm / send_dm)
const WORKER_TO_PUBLISH_DOMAIN = {
  posts:     'publish:post',
  stories:   'publish:story',
  comments:  'publish:comment',
  messages:  'publish:message',
};

function setGovernance(gov) {
  _governance = gov;
}

function _emit(event) {
  if (!_governance) {
    console.warn('[publishing:retry-stub] No governance reference; cannot emit', event.type);
    return;
  }
  (_governance.dispatchGlobal || _governance.dispatch)(event);
}

function _resolvePublishDomain(workerName) {
  if (!workerName) return 'publish:unknown';
  return WORKER_TO_PUBLISH_DOMAIN[workerName] || `publish:${workerName}`;
}

/**
 * Forward a retry request to the retry-cadence-kernel with
 * the correct publish:* domain key.
 *
 * @param {object} params — { accountId, intentId, workerName, params,
 *                            error, errorShape, errorCategory,
 *                            retryAfterMs }
 */
function requestRetry(params) {
  const { accountId, intentId, workerName, error, errorShape, errorCategory, retryAfterMs } = params || {};
  _emit({
    type: 'RETRY_REQUESTED',
    accountId,
    domain: _resolvePublishDomain(workerName),
    intentId: intentId || null,
    params: params?.params || {},
    error: error || null,
    errorShape: errorShape || null,
    errorCategory: errorCategory || 'transient',
    retryAfterMs: retryAfterMs ?? null,
  });
}

/**
 * Forward a retry-exhausted signal. Emitted by the publishing
 * FSM on terminal failure (closes the EXECUTING → IDLE
 * transition).
 *
 * @param {object} params — { accountId, intentId, error, ... }
 */
function notifyExhausted(params) {
  _emit({
    type: 'RETRY_EXHAUSTED',
    ...(params || {}),
    domain: params?.domain || 'publishing',
  });
}

/**
 * Forward a retry-in-progress signal. The publishing FSM
 * holds its EXECUTING state while the retry chain is in flight
 * (observability fidelity).
 *
 * @param {object} params — { accountId, intentId, retryCount, delayMs, ... }
 */
function notifyInProgress(params) {
  _emit({
    type: 'RETRY_IN_PROGRESS',
    ...(params || {}),
    domain: params?.domain || 'publishing',
  });
}

module.exports = {
  setGovernance,
  requestRetry,
  notifyExhausted,
  notifyInProgress,
  // Exposed for tests / future use
  _resolvePublishDomain,
  WORKER_TO_PUBLISH_DOMAIN,
};
