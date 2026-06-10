// retry-cadence-kernel/workers/ig-publish-post-retry-worker.js
// IG Publish Post Retry Worker — re-invokes the publish:post
// operation via the content substrate.
//
// CONSTITUTIONAL CONTRACT (Phase 4):
//   Owns: ONE call to contentSubstrate.execute() for a publish:post
//          retry attempt. The substrate is the bounded executor
//          that knows the IG API; this worker is the operationally
//          bounded retry executor.
//   Does NOT own: classification (ig-reliability-substrate),
//                 scheduling (engagement-fsm), error normalization.
//
// The canonical retry path: engagement-fsm's REQUEUE_OPERATION_AUTHORIZED
// transition delegates to ig-recovery-substrate, which dispatches
// to this worker (per the recommendation-domain mapping).

const contentSubstrate = require('../../publishing-kernel/substrates/content');

const SUPPORTED_DOMAINS = ['publish:post'];

/**
 * Execute a publish:post retry attempt.
 *
 * @param {object} event — { accountId, intentId, domain, analysis, params, ... }
 * @param {object} governance — CK reference
 * @returns {Promise<{ success, workerName, durationMs, error, data }>}
 */
async function execute(event, governance) {
  const startTime = Date.now();
  const { accountId, intentId, domain, params } = event || {};

  if (!SUPPORTED_DOMAINS.includes(domain)) {
    return {
      success: false,
      workerName: 'ig-publish-post-retry-worker',
      durationMs: 0,
      error: `unsupported domain: ${domain}`,
    };
  }

  // Re-invoke the publish substrate with the original payload
  let result;
  try {
    const items = params?.items || params?.payload?.items || [];
    result = await contentSubstrate.execute(accountId, items, governance);
  } catch (err) {
    return {
      success: false,
      workerName: 'ig-publish-post-retry-worker',
      durationMs: Date.now() - startTime,
      error: err.message || String(err),
    };
  }

  if (result && result.success) {
    if (governance?.dispatchGlobal) {
      governance.dispatchGlobal({
        type: 'IG_PUBLISH_POST_RETRY_COMPLETE',
        accountId, intentId, domain,
        durationMs: Date.now() - startTime,
      });
    }
    return {
      success: true,
      workerName: 'ig-publish-post-retry-worker',
      durationMs: Date.now() - startTime,
      error: null,
      data: result.data || result,
    };
  }

  // Substrate returned a structured failure — emit raw outcome
  // upward so the canonical IG_FAILURE_OBSERVED path can re-analyze
  if (result && !result.success) {
    if (governance?.dispatchGlobal) {
      governance.dispatchGlobal({
        type: 'IG_FAILURE_OBSERVED',
        rawError: result,
        accountId, intentId, domain,
        suspectedCategory: result.suspectedCategory || 'publishing_unknown',
        workerName: 'ig-publish-post-retry-worker',
      });
    }
  }

  return {
    success: false,
    workerName: 'ig-publish-post-retry-worker',
    durationMs: Date.now() - startTime,
    error: result?.error || 'substrate_returned_failure',
  };
}

module.exports = {
  name: 'ig-publish-post-retry-worker',
  execute,
  supportedDomains: SUPPORTED_DOMAINS,
};
