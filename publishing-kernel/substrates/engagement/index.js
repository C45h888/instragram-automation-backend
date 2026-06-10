// publishing-kernel/substrates/engagement/index.js
// Engagement bounded substrate: owns worker factory, rate limiter, execution loop.
//
// Owns: factory-creating engagement workers, pre-flight checks, per-item iteration,
//        outcome→CK signal classification, credential resolution. Zero internal
//        retry.
// Does NOT own: IG API calls (delegates to worker), retry policy (engagement-fsm
//               domain), state.
//
// Contract (Step 7 — credential resolution normalisation):
//   execute(accountId, items, governance) → void
//   Credentials are resolved INTERNALLY by the substrate from
//   graph-capability-kernel. The orchastrator does NOT pass
//   credentials.
//
// Workers:
//   'comments' → EngagementWorker('reply_comment')
//   'messages' → EngagementWorker('reply_dm') or EngagementWorker('send_dm')

const EngagementWorker = require('./worker');
const rateLimiter = require('./rate-limiter');
const { resolveAccountCredentials } =
  require('../../../graph-capability-kernel/substrates/credential-resolver');
// Phase 1 (base): retry cadence routing goes through the
// publishing-kernel stub. The stub maps workerName → publish:*
// domain and emits the canonical RETRY_REQUESTED. The previous
// inline dispatch (domain: 'engagement') routed publish failures
// through the read-side retry workers; the stub fixes that.
const retryStub = require('../../retry-state-transition-stub');

/**
 * Execute a batch of engagement publishing items.
 * Iterates items sequentially — each item gets pre-flight checks, worker
 * factory creation, credential resolution, one bounded IG API call, and
 * outcome signal dispatch.
 *
 * @param {string} accountId
 * @param {Array<{worker: string, actionType: string, record: object}>} items
 * @param {object} governance — CK module (for dispatch + subscribeAction)
 */
async function execute(accountId, items, governance) {
  // Resolve credentials ONCE per batch
  const credentials = await resolveAccountCredentials(accountId);
  for (const item of items) {
    await _executeItem(accountId, item, governance, credentials);
  }
}

// ── Per-item execution ─────────────────────────────────────────────────────

async function _executeItem(accountId, item, governance, credentials) {
  const { actionType, worker: workerName, record } = item;

  // ── Pre-flight: substrate rate-limit check ─────────────────────────────
  const rl = rateLimiter.isRateLimited(accountId);
  if (rl.limited) {
    governance.dispatch({
      type: 'RATE_LIMIT_DETECTED',
      accountId,
      domain: 'engagement',
      cooldownMs: rl.until ? rl.until - Date.now() : 3600000,
    });
    return; // stop batch
  }

  // ── Pre-flight: circuit breaker check ──────────────────────────────────
  governance.dispatch({
    type: 'CIRCUIT_BREAKER_CHECK',
    accountId,
    domain: 'engagement',
  });

  // ── Record rate-limit call ──────────────────────────────────────────────
  rateLimiter.recordCall(accountId);

  // ── Build payload from record ───────────────────────────────────────────
  const payload = _buildPayload(record);

  // ── Factory-create worker, execute ONE bounded call ─────────────────────
  const worker = new EngagementWorker(actionType);
  const result = await worker.execute(accountId, credentials, payload);

  // ── Outcome → signal upward. ZERO internal retry. ───────────────────────
  if (result.success) {
    governance.dispatch({
      type: 'PUBLISHING_OBSERVATION',
      status: 'ok',
      accountId,
      metadata: {
        instagram_id: result.instagram_id || null,
        actionType,
        worker: workerName,
      },
    });
    return;
  }

  const { error_category, retryable, error, retry_after_seconds } = result;

  // Rate limit → escalate immediately
  if (error_category === 'rate_limit') {
    governance.dispatch({
      type: 'RATE_LIMIT_DETECTED',
      accountId,
      domain: 'engagement',
      cooldownMs: (retry_after_seconds || 3600) * 1000,
    });
    return;
  }

  // Auth failure → escalate immediately
  if (error_category === 'auth_failure' || error_category === 'permission') {
    governance.dispatch({
      type: 'AUTH_FAILURE_STRIKE',
      accountId,
      error,
    });
    return;
  }

  // Transient → escalate to engagement-fsm for retry sovereignty
  // via the publishing-kernel retry stub. The stub maps
  // workerName ('comments' | 'messages') → publish:* domain so
  // the retry-cadence kernel routes through the publish worker
  // bindings.
  if (retryable) {
    retryStub.requestRetry({
      accountId,
      intentId: record.id || null,
      workerName,
      params: { actionType, worker: workerName, record },
      error,
      errorCategory: error_category || 'transient',
      retryAfterMs: (retry_after_seconds || 0) * 1000,
    });
    return;
  }

  // Permanent failure — no retry path
  governance.dispatch({
    type: 'PUBLISH_FAILURE',
    accountId,
    reason: error || 'Permanent publish failure',
    metadata: { actionType, worker: workerName },
  });
}

// ── Payload builder ────────────────────────────────────────────────────────

function _buildPayload(record) {
  return {
    comment_id: record.comment_id,
    reply_text: record.reply_text,
    conversation_id: record.conversation_id,
    message_text: record.message_text,
    recipient_id: record.recipient_id,
  };
}

// ── Direct execute methods (for non-batched, non-governed calls) ──────────

const transport = require('./transport');
const { suspectIgCategory } = require('../../../substrates/transport/error-classifier');

async function executeCommentReply(accountId, credentials, payload) {
  const { pageToken } = credentials;
  try {
    const result = await transport.replyComment(payload.comment_id, pageToken, payload.reply_text);
    return { success: true, instagram_id: result.id };
  } catch (error) {
    const msg = error.response?.data?.error?.message || error.message;
    return {
      success: false, error: msg, code: error.response?.data?.error?.code || null,
      suspectedCategory: suspectIgCategory(error), rawError: error,
    };
  }
}

async function executeDmReply(accountId, credentials, payload) {
  const { pageToken } = credentials;
  try {
    const result = await transport.replyDm(payload.conversation_id, pageToken, payload.message_text);
    return { success: true, instagram_id: result.id };
  } catch (error) {
    const msg = error.response?.data?.error?.message || error.message;
    return {
      success: false, error: msg, code: error.response?.data?.error?.code || null,
      suspectedCategory: suspectIgCategory(error), rawError: error,
    };
  }
}

async function executeDmSend(accountId, credentials, payload) {
  const { igUserId, pageToken, pageId } = credentials;
  try {
    const result = await transport.sendDm(pageId, igUserId, pageToken, payload.recipient_id, payload.message_text);
    return { success: true, instagram_id: result.messageId };
  } catch (error) {
    const msg = error.response?.data?.error?.message || error.message;
    return {
      success: false, error: msg, code: error.response?.data?.error?.code || null,
      suspectedCategory: suspectIgCategory(error), rawError: error,
    };
  }
}

module.exports = { execute, executeCommentReply, executeDmReply, executeDmSend };
