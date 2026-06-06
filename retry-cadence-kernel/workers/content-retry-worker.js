// retry-cadence-kernel/workers/content-retry-worker.js
// Content retry worker: business media posts retry execution.
//
// CONSTITUTIONAL CONTRACT (Step 4 of authority centralisation):
//   - Bounded single I/O call. Runs the transport fetch, emits
//     WORKER_OUTCOME_REPORTED with raw outcome. That is all.
//   - Does NOT classify errors. engagement-fsm classifies.
//   - Does NOT decide retry vs skip vs break.
//   - Does NOT schedule retries. engagement-fsm owns scheduling.
//   - Does NOT call other workers.
//   - Does NOT mutate engagement state.

const contentTransport = require('../../acquisition-kernel/substrates/ugc-content-substrate/content-transport');
const { resolveAccountCredentials } = require('../../graph-capability-kernel/substrates/credential-resolver');
const parsing = require('../../acquisition-kernel/substrates/parsing-substrate');

async function execute(domain, accountId, intentId, params, retryCount, maxRetries, governance) {
  const startTime = Date.now();
  let result;

  try {
    const creds = await resolveAccountCredentials(accountId);
    result = await contentTransport.fetchPosts(accountId, params.limit || 50, creds);
  } catch (err) {
    result = {
      success: false, count: 0, posts: [],
      error: err.message, code: null,
      retryable: null, error_category: null, retry_after_seconds: null,
    };
  }

  const latencyMs = Date.now() - startTime;

  if (result.success) {
    parsing.dispatch(domain, result, accountId, intentId, {});
    governance.dispatch({
      type: 'PARSING_DISPATCHED',
      accountId, intentId, domain,
      rawCount: result.count || 0,
    });
    result.count = 0;
  }

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
    transportMeta: { success: result.success },
  });
}

module.exports = { execute };
