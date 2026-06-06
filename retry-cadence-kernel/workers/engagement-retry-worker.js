// retry-cadence-kernel/workers/engagement-retry-worker.js
// Engagement retry worker: comments + messages domain retry execution.
//
// CONSTITUTIONAL CONTRACT (Step 4 of authority centralisation):
//   - Bounded single I/O call. Runs the transport fetch, emits
//     WORKER_OUTCOME_REPORTED with raw outcome. That is all.
//   - Does NOT classify errors (no substrates/retry.js handleFetchError).
//   - Does NOT apply delay overrides. The classifier does that.
//   - Does NOT decide retry vs skip vs break.
//   - Does NOT schedule retries. engagement-fsm owns scheduling.
//   - Does NOT call other workers.
//   - Does NOT mutate engagement state.
//   - The retryCount parameter is informational — the FSM owns
//     the canonical count via engagement-fsm._executionRetries.
//
// Receives: { domain, accountId, intentId, params, retryCount,
//             maxRetries, governance } from engagement-fsm via
//             retry-cadence-kernel/index.js schedule().

const engagementTransport = require('../../acquisition-kernel/substrates/engagement-substrate/transport');
const { resolveAccountCredentials } = require('../../graph-capability-kernel/substrates/credential-resolver');
const parsing = require('../../acquisition-kernel/substrates/parsing-substrate');

/**
 * Execute a single bounded retry attempt for engagement domain.
 * Runs the transport fetch + parsing dispatch. Emits
 * WORKER_OUTCOME_REPORTED with raw outcome. Returns.
 */
async function execute(domain, accountId, intentId, params, retryCount, maxRetries, governance) {
  const startTime = Date.now();
  let result;

  try {
    const creds = await resolveAccountCredentials(accountId);

    if (params.media_id) {
      result = await engagementTransport.fetchComments(accountId, params.media_id, params.limit, creds);
    } else if (params.conversation_id) {
      result = await engagementTransport.fetchMessages(accountId, params.conversation_id, params.limit, creds);
    } else {
      result = await engagementTransport.fetchConversations(accountId, params.convLimit || params.limit, creds);
    }
  } catch (err) {
    result = {
      success: false, count: 0,
      error: err.message, code: null,
      retryable: null, error_category: null, retry_after_seconds: null,
    };
  }

  const latencyMs = Date.now() - startTime;

  // Success → dispatch to parsing substrate
  if (result.success) {
    parsing.dispatch(domain, result, accountId, intentId, {
      igUserId: result.igUserId, pageId: result.pageId, pageToken: result.pageToken,
    });
    governance.dispatch({
      type: 'PARSING_DISPATCHED',
      accountId, intentId, domain,
      rawCount: result.count || 0,
    });
    result.count = 0;
  }

  // ── Emit raw outcome upward ─────────────────────────────────────────
  // engagement-fsm is the intelligence membrane. It receives
  // WORKER_OUTCOME_REPORTED, calls the classification-worker,
  // decides the action, and emits the downstream signal.
  // This worker does not classify. It does not decide.
  governance.dispatch({
    type: 'WORKER_OUTCOME_REPORTED',
    accountId, intentId, domain,
    status: result.success ? 'completed' : 'failed',
    result: result.success ? { count: result.count || 0 } : null,
    error: result.success ? null : (result.error || null),
    errorShape: result.success ? null : {
      category: result.error_category || null,
      code: result.code || null,
      retryable: result.retryable ?? null,
      retryAfterSeconds: result.retry_after_seconds || null,
    },
    latencyMs,
    retryCount,
    transportMeta: {
      success: result.success,
      igUserId: result.igUserId || null,
      pageId: result.pageId || null,
    },
  });
}

module.exports = { execute };
