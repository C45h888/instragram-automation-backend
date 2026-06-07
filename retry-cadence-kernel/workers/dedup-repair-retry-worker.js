// retry-cadence-kernel/workers/dedup-repair-retry-worker.js
// Dedup-Repair Retry Worker: bounded executor for conversation repair.
//
// Imports dedup-kernel/substrates/repair directly — no intermediate
// retry substrate. Factory-creates a ConversationRepairWorker,
// runs one Graph API fetch + DB upsert attempt. Emits
// WORKER_OUTCOME_REPORTED. Done.
//
// CONSTITUTIONAL CONTRACT:
//   - One bounded I/O call. Factory-creates ConversationRepairWorker,
//     runs execute(), emits WORKER_OUTCOME_REPORTED.
//   - Does NOT classify, schedule, decide, or mutate FSM state.
//
// Receives: (domain, accountId, intentId, params, retryCount,
//            maxRetries, governance) from engagement-fsm._executeRetry.
// params: { threadId, igUserId, pageToken, pageId }

const ConversationRepairWorker = require('../../dedup-kernel/substrates/repair/workers/conversation-repair-worker');

async function execute(domain, accountId, intentId, params, retryCount, maxRetries, governance) {
  // Dual-signature shim: CK gate passes { domain, accountId, ... } as
  // a single object; fallback path passes positional args. Normalise
  // so the rest of the function receives destructured locals.
  if (typeof domain === 'object' && domain !== null) {
    ({ domain, accountId, intentId, params, retryCount, maxRetries, governance } = domain);
  }

  const startTime = Date.now();

  try {
    const worker = new ConversationRepairWorker();
    const result = await worker.execute({
      threadId: params.threadId,
      accountId,
      igUserId: params.igUserId,
      pageToken: params.pageToken,
      pageId: params.pageId,
    }, governance);

    governance.dispatch({
      type: 'WORKER_OUTCOME_REPORTED',
      accountId, intentId, domain,
      status: result.recovered > 0 ? 'completed' : 'failed',
      result: { recovered: result.recovered, uuid: result.uuid },
      error: result.recovered > 0 ? null : 'conversation_not_found',
      errorShape: result.recovered > 0 ? null : {
        category: 'permanent', code: null, retryable: false, retryAfterSeconds: null,
      },
      latencyMs: Date.now() - startTime,
      retryCount,
    });
  } catch (err) {
    governance.dispatch({
      type: 'WORKER_OUTCOME_REPORTED',
      accountId, intentId, domain,
      status: 'failed',
      result: null,
      error: err.message,
      errorShape: {
        category: _classify(err),
        code: err.code || null,
        retryable: _classify(err) === 'transient',
        retryAfterSeconds: null,
      },
      latencyMs: Date.now() - startTime,
      retryCount,
    });
  }
}

function _classify(err) {
  const s = err.response?.status || err.status || null;
  if (s === 401 || s === 403) return 'auth_failure';
  if (s === 404) return 'permanent';
  if (s === 429) return 'rate_limit';
  if (s && s >= 500) return 'transient';
  if (s && s >= 400) return 'permanent';
  const m = (err.message || '').toLowerCase();
  if (/etimedout|econnrefused|timeout|connect|econnreset|enotfound/i.test(m)) return 'transient';
  return 'transient';
}

module.exports = { execute };
