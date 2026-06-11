// publishing-kernel/substrates/engagement/index.js
// Engagement bounded substrate: owns worker factory, bedrock routing, execution loop.
//
// Owns: factory-creating engagement workers, pre-flight checks, per-item iteration,
//       outcome routing via ig-reliability-substrate.analyzeFailure(). Zero
//       internal retry.
// Does NOT own: IG API calls (delegates to worker), retry policy (engagement-fsm
//               domain), error classification (bedrock owns §2, §12, §15),
//               rate limiting (bedrock §5 owns).
//
// All error classification routes through substrates/ig-reliability-substrate.
// No suspectIgCategory. No error_category strings. No rate-limiter.js.
//
// Routing decisions based on bedrock §15 recommendations:
//   THROTTLE_ACCOUNT / DEFER_NONCRITICAL_WORK → RATE_LIMIT_DETECTED
//   REFRESH_TOKEN / REAUTHORIZE_USER → AUTH_FAILURE_STRIKE
//   REQUEUE_OPERATION + retryable → retryStub.requestRetry()
//   otherwise → PUBLISH_FAILURE
//
// Workers:
//   'comments' → EngagementWorker('reply_comment')
//   'messages' → EngagementWorker('reply_dm') or EngagementWorker('send_dm')

const EngagementWorker = require('./worker');
const { resolveAccountCredentials } =
  require('../../../graph-capability-kernel/substrates/credential-resolver');
const retryStub = require('../../retry-state-transition-stub');
const igReliability = require('../../../substrates/ig-reliability-substrate');

/**
 * Execute a batch of engagement publishing items.
 * Iterates items sequentially — each item gets pre-flight checks, worker
 * factory creation, credential resolution, one bounded IG API call, and
 * outcome routing via bedrock recommendations.
 */
async function execute(accountId, items, governance) {
  const credentials = await resolveAccountCredentials(accountId);
  for (const item of items) {
    await _executeItem(accountId, item, governance, credentials);
  }
}

// ── Per-item execution ─────────────────────────────────────────────────────

async function _executeItem(accountId, item, governance, credentials) {
  const { actionType, worker: workerName, record } = item;

  // ── Pre-flight: circuit breaker check (preserved — ordering, not classification) ─
  governance.dispatch({
    type: 'CIRCUIT_BREAKER_CHECK',
    accountId,
    domain: 'engagement',
  });

  // ── Build payload from record ───────────────────────────────────────────
  const payload = _buildPayload(record);

  // ── Factory-create worker, execute ONE bounded call ─────────────────────
  const worker = new EngagementWorker(actionType);
  const result = await worker.execute(accountId, credentials, payload);

  // ── Outcome → bedrock routing. ZERO internal retry. ─────────────────────
  if (result.success) {
    governance.dispatch({
      type: 'PUBLISHING_OBSERVATION',
      status: 'ok',
      accountId,
      metadata: {
        instagram_id: result.instagram_id || null,
        actionType,
        worker: workerName,
        domain: _resolveDomain(actionType),
      },
    });
    return;
  }

  // ── FAILURE PATH — canonical analysis via bedrock ───────────────────────
  const operation = _resolveOperation(actionType);
  const analysis = igReliability.analyzeFailure(result.rawError, operation, 'ig-graph', {
    accountId,
    attemptN: 1,
    businessAccountId: accountId,
    headers: { retryAfter: result.retryAfterHeader },
  });

  // Route on bedrock §15 recommendations
  if (analysis.recommendations.includes('THROTTLE_ACCOUNT') || analysis.recommendations.includes('DEFER_NONCRITICAL_WORK')) {
    governance.dispatch({
      type: 'RATE_LIMIT_DETECTED',
      accountId,
      domain: 'engagement',
      cooldownMs: analysis.rateLimit?.retryAfterMs ?? 3600000,
      analysis: { category: analysis.category, severity: analysis.severity },
    });
    return;
  }

  if (analysis.recommendations.includes('REFRESH_TOKEN') || analysis.recommendations.includes('REAUTHORIZE_USER')) {
    governance.dispatch({
      type: 'AUTH_FAILURE_STRIKE',
      accountId,
      error: result.rawError?.message || 'Authentication failure',
      analysis: { category: analysis.category, severity: analysis.severity, recommendations: analysis.recommendations },
    });
    return;
  }

  if (analysis.retryable && analysis.recommendations.includes('REQUEUE_OPERATION')) {
    retryStub.requestRetry({
      accountId,
      intentId: record.id || null,
      workerName,
      params: { actionType, worker: workerName, record },
      error: result.rawError?.message || 'Transient engagement failure',
      errorCategory: analysis.category,
      retryAfterMs: analysis.backoff?.computedMs || 0,
      analysis: { severity: analysis.severity, confidence: analysis.confidence },
    });
    return;
  }

  // Terminal failure — no retry path
  governance.dispatch({
    type: 'PUBLISH_FAILURE',
    accountId,
    domain: operation,
    intentId: record.id || null,
    reason: result.rawError?.message || 'Permanent engagement failure',
    metadata: { actionType, worker: workerName, severity: analysis.severity, recommendations: analysis.recommendations },
  });
}

// ── Helpers ────────────────────────────────────────────────────────────────

function _buildPayload(record) {
  return {
    comment_id: record.comment_id,
    reply_text: record.reply_text,
    conversation_id: record.conversation_id,
    message_text: record.message_text,
    recipient_id: record.recipient_id,
  };
}

function _resolveOperation(actionType) {
  if (actionType === 'reply_comment') return 'publish:comment';
  if (actionType === 'reply_dm' || actionType === 'send_dm') return 'publish:message';
  return 'publish:comment';
}

function _resolveDomain(actionType) {
  if (actionType === 'reply_dm' || actionType === 'send_dm') return 'engagement';
  return 'engagement';
}

// ── Direct execute methods (for non-batched, non-governed calls) ──────────

const transport = require('./transport');
const { resolveAccountCredentials: resolveCreds } =
  require('../../../graph-capability-kernel/substrates/credential-resolver');

async function executeCommentReply(accountId, credentials, payload) {
  const { pageToken } = credentials;
  try {
    const result = await transport.replyComment(payload.comment_id, pageToken, payload.reply_text);
    return { success: true, instagram_id: result.id };
  } catch (error) {
    return { success: false, rawError: error, code: error?.response?.data?.error?.code ?? null };
  }
}

async function executeDmReply(accountId, credentials, payload) {
  const { pageToken } = credentials;
  try {
    const result = await transport.replyDm(payload.conversation_id, pageToken, payload.message_text);
    return { success: true, instagram_id: result.id };
  } catch (error) {
    return { success: false, rawError: error, code: error?.response?.data?.error?.code ?? null };
  }
}

async function executeDmSend(accountId, credentials, payload) {
  const { igUserId, pageToken, pageId } = credentials;
  try {
    const result = await transport.sendDm(pageId, igUserId, pageToken, payload.recipient_id, payload.message_text);
    return { success: true, instagram_id: result.messageId };
  } catch (error) {
    return { success: false, rawError: error, code: error?.response?.data?.error?.code ?? null };
  }
}

module.exports = { execute, executeCommentReply, executeDmReply, executeDmSend };
