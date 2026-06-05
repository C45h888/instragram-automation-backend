// publishing-kernel/substrates/engagement/index.js
// Engagement bounded substrate: owns worker factory, rate limiter, execution loop.
//
// Owns: factory-creating engagement workers, pre-flight checks, per-item iteration,
//        outcome→CK signal classification. Zero internal retry.
// Does NOT own: IG API calls (delegates to worker), credentials (receives from
//               orchestrator), retry policy (engagement-fsm domain), state.
//
// Contract:
//   execute(accountId, items, governance, credentials) → void
//   All outcomes dispatched as CK signals upward. No return value.
//
// Workers:
//   'comments' → EngagementWorker('reply_comment')
//   'messages' → EngagementWorker('reply_dm') or EngagementWorker('send_dm')

const EngagementWorker = require('./worker');
const rateLimiter = require('./rate-limiter');

/**
 * Execute a batch of engagement publishing items.
 * Iterates items sequentially — each item gets pre-flight checks, worker
 * factory creation, one bounded IG API call, and outcome signal dispatch.
 *
 * @param {string} accountId
 * @param {Array<{worker: string, actionType: string, record: object}>} items
 * @param {object} governance — CK module (for dispatch + subscribeAction)
 * @param {{igUserId: string, pageToken: string, pageId?: string}} credentials
 */
async function execute(accountId, items, governance, credentials) {
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
  if (retryable) {
    governance.dispatch({
      type: 'RETRY_REQUESTED',
      accountId,
      domain: 'engagement',
      intentId: record.id || null,
      params: { actionType, worker: workerName, record },
      error,
      error_category: error_category || 'transient',
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
const { categorizeIgError } = require('../../../helpers/agent-helpers');

async function executeCommentReply(accountId, credentials, payload) {
  const { pageToken } = credentials;
  try {
    const result = await transport.replyComment(payload.comment_id, pageToken, payload.reply_text);
    return { success: true, instagram_id: result.id };
  } catch (error) {
    const msg = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);
    return { success: false, error: msg, retryable, error_category, retry_after_seconds };
  }
}

async function executeDmReply(accountId, credentials, payload) {
  const { pageToken } = credentials;
  try {
    const result = await transport.replyDm(payload.conversation_id, pageToken, payload.message_text);
    return { success: true, instagram_id: result.id };
  } catch (error) {
    const msg = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);
    return { success: false, error: msg, retryable, error_category, retry_after_seconds };
  }
}

async function executeDmSend(accountId, credentials, payload) {
  const { igUserId, pageToken, pageId } = credentials;
  try {
    const result = await transport.sendDm(pageId, igUserId, pageToken, payload.recipient_id, payload.message_text);
    return { success: true, instagram_id: result.messageId };
  } catch (error) {
    const msg = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);
    return { success: false, error: msg, retryable, error_category, retry_after_seconds };
  }
}

module.exports = { execute, executeCommentReply, executeDmReply, executeDmSend };
