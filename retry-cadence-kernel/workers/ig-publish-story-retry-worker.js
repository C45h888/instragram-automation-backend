// retry-cadence-kernel/workers/ig-publish-story-retry-worker.js
// IG Publish Story Retry Worker — re-invokes the publish:story
// operation via the content substrate.
//
// Mirrors ig-publish-post-retry-worker shape; same substrate,
// different domain tag.

const contentSubstrate = require('../../publishing-kernel/substrates/content');

const SUPPORTED_DOMAINS = ['publish:story'];

async function execute(event, governance) {
  const startTime = Date.now();
  const { accountId, intentId, domain, params } = event || {};

  if (!SUPPORTED_DOMAINS.includes(domain)) {
    return {
      success: false,
      workerName: 'ig-publish-story-retry-worker',
      durationMs: 0,
      error: `unsupported domain: ${domain}`,
    };
  }

  let result;
  try {
    const items = params?.items || params?.payload?.items || [];
    result = await contentSubstrate.execute(accountId, items, governance);
  } catch (err) {
    return {
      success: false,
      workerName: 'ig-publish-story-retry-worker',
      durationMs: Date.now() - startTime,
      error: err.message || String(err),
    };
  }

  if (result && result.success) {
    if (governance?.dispatchGlobal) {
      governance.dispatchGlobal({
        type: 'IG_PUBLISH_STORY_RETRY_COMPLETE',
        accountId, intentId, domain,
        durationMs: Date.now() - startTime,
      });
    }
    return {
      success: true,
      workerName: 'ig-publish-story-retry-worker',
      durationMs: Date.now() - startTime,
      error: null,
      data: result.data || result,
    };
  }

  if (result && !result.success) {
    if (governance?.dispatchGlobal) {
      governance.dispatchGlobal({
        type: 'IG_FAILURE_OBSERVED',
        rawError: result,
        accountId, intentId, domain,
        suspectedCategory: result.suspectedCategory || 'publishing_unknown',
        workerName: 'ig-publish-story-retry-worker',
      });
    }
  }

  return {
    success: false,
    workerName: 'ig-publish-story-retry-worker',
    durationMs: Date.now() - startTime,
    error: result?.error || 'substrate_returned_failure',
  };
}

module.exports = {
  name: 'ig-publish-story-retry-worker',
  execute,
  supportedDomains: SUPPORTED_DOMAINS,
};
