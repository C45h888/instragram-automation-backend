// retry-cadence-kernel/workers/publish-content-retry-worker.js
// Publish content retry worker: publish:post + publish:story retry execution.
//
// CONSTITUTIONAL CONTRACT (Step 6 of authority centralisation):
//   - Bounded I/O executor (operationally complete, semantically blind)
//   - Imports the publish substrate directly (the substrate IS the
//     executor; this worker is a thin wrapper that emits the cadence
//     signal after execution)
//   - Emits ONLY WORKER_OUTCOME_REPORTED — the FSM reads it
//   - No classification, no state mutation, no scheduling
//   - The classification-worker classifies. engagement-fsm schedules.
//     engagement-fsm calls this worker via _executeRetry(context).

const contentSubstrate = require('../../publishing-kernel/substrates/content');
const publishErrorParser = require('./publish-error-parser');

const SUPPORTED_DOMAINS = ['publish:post', 'publish:story'];

/**
 * Execute a publish content retry attempt.
 * Called by engagement-fsm._executeRetry.
 *
 * Step 7 normalisation: the substrate resolves its own credentials
 * internally. The worker does not import or pass credentials.
 *
 * @param {string} domain — 'publish:post' | 'publish:story'
 * @param {string} accountId
 * @param {string} intentId
 * @param {object} params — must include `items` (the post/story payload)
 * @param {number} retryCount — 1-indexed (1 = first retry)
 * @param {number} maxRetries
 * @param {object} governance — constitutional kernel reference
 * @returns {Promise<void>}
 */
async function execute(domain, accountId, intentId, params, retryCount, maxRetries, governance) {
  if (!SUPPORTED_DOMAINS.includes(domain)) {
    governance.dispatch({
      type: 'WORKER_OUTCOME_REPORTED',
      accountId, intentId, domain,
      status: 'failed',
      errorShape: {
        category: 'permanent', code: null, retryable: false,
        retryAfterSeconds: null, domain, source: 'unsupported_domain',
      },
      error: `publish-content-retry-worker: unsupported domain '${domain}'`,
      latencyMs: 0,
    });
    return;
  }

  const startTime = Date.now();
  const items = params?.items || params?.payload?.items || [];

  if (!items || items.length === 0) {
    governance.dispatch({
      type: 'WORKER_OUTCOME_REPORTED',
      accountId, intentId, domain,
      status: 'failed',
      errorShape: {
        category: 'permanent', code: null, retryable: false,
        retryAfterSeconds: null, domain, source: 'missing_items',
      },
      error: 'publish-content-retry-worker: missing items',
      latencyMs: 0,
    });
    return;
  }

  let result;
  try {
    // Step 7: substrate resolves credentials internally. Pass 3 args.
    result = await contentSubstrate.execute(accountId, items, governance);
  } catch (err) {
    // Substrate threw — wrap and emit
    const errorShape = publishErrorParser.parseError(err, domain);
    governance.dispatch({
      type: 'WORKER_OUTCOME_REPORTED',
      accountId, intentId, domain,
      status: 'failed',
      errorShape,
      error: err.message || String(err),
      latencyMs: Date.now() - startTime,
      retryCount,
    });
    return;
  }

  // Substrate returned (may be success or structured failure)
  if (result && result.success) {
    governance.dispatch({
      type: 'WORKER_OUTCOME_REPORTED',
      accountId, intentId, domain,
      status: 'completed',
      result: result.data || result,
      latencyMs: Date.now() - startTime,
      retryCount,
    });
    return;
  }

  // Substrate returned a structured failure
  const errorShape = publishErrorParser.parse(result, domain);
  governance.dispatch({
    type: 'WORKER_OUTCOME_REPORTED',
    accountId, intentId, domain,
    status: 'failed',
    errorShape,
    error: result?.error || 'substrate_returned_failure',
    latencyMs: Date.now() - startTime,
    retryCount,
  });
}

module.exports = {
  name: 'publish-content-retry-worker',
  execute,
  supportedDomains: SUPPORTED_DOMAINS,
};
