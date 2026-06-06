// retry-cadence-kernel/workers/insights-retry-worker.js
// Insights retry worker: account insights + media insights retry execution.
//
// CONSTITUTIONAL CONTRACT (Step 4 of authority centralisation):
//   - Bounded single I/O call. Runs the transport fetch, emits
//     WORKER_OUTCOME_REPORTED with raw outcome. That is all.
//   - Does NOT classify errors. engagement-fsm classifies.
//   - Does NOT decide retry vs skip vs break.
//   - Does NOT schedule retries. engagement-fsm owns scheduling.
//   - Does NOT call other workers.
//   - Does NOT mutate engagement state.

const insightsTransport = require('../../acquisition-kernel/substrates/insights-substrate/transport');
const { resolveAccountCredentials } = require('../../graph-capability-kernel/substrates/credential-resolver');
const parsing = require('../../acquisition-kernel/substrates/parsing-substrate');

async function execute(domain, accountId, intentId, params, retryCount, maxRetries, governance) {
  const startTime = Date.now();
  let result;

  try {
    const creds = await resolveAccountCredentials(accountId);
    const sevenDaysAgo = params.since || Math.floor((Date.now() - 7 * 24 * 3600000) / 1000);
    const now = params.until || Math.floor(Date.now() / 1000);

    // Step 1: media feed
    const feedResult = await insightsTransport.fetchMediaFeed(accountId, sevenDaysAgo, now, creds);
    if (!feedResult || !feedResult.success) {
      result = feedResult || { success: false, error: 'feed_fetch_failed' };
    } else {
      const mediaList = feedResult.mediaList || [];
      if (mediaList.length === 0) {
        result = { success: true, insights: [], mediaList: [], _usagePct: feedResult._usagePct };
      } else {
        const insights = await insightsTransport.fetchMediaInsightsBatch(mediaList, creds.pageToken);
        result = { success: true, insights, mediaList, _usagePct: feedResult._usagePct };
      }
    }
  } catch (err) {
    result = {
      success: false, count: 0,
      error: err.message, code: null,
      retryable: null, error_category: null, retry_after_seconds: null,
    };
  }

  const latencyMs = Date.now() - startTime;

  if (result.success) {
    parsing.dispatch(domain, result, accountId, intentId, {
      igUserId: result.igUserId, pageToken: result.pageToken,
    });
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
