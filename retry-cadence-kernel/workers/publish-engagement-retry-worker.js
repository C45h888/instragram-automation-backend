// retry-cadence-kernel/workers/publish-engagement-retry-worker.js
// Publish engagement retry worker: publish:comment + publish:message retry execution.
//
// CONSTITUTIONAL CONTRACT (Step 6 of authority centralisation):
//   - Bounded I/O executor (operationally complete, semantically blind)
//   - Imports the publish engagement substrate directly
//   - Emits ONLY WORKER_OUTCOME_REPORTED — the FSM reads it
//   - No classification, no state mutation, no scheduling
//   - The classification-worker classifies. engagement-fsm schedules.

const engagementSubstrate = require('../../publishing-kernel/substrates/engagement');
const publishErrorParser = require('./publish-error-parser');

const SUPPORTED_DOMAINS = ['publish:comment', 'publish:message'];

/**
 * Execute a publish engagement retry attempt.
 * Called by engagement-fsm._executeRetry.
 *
 * Step 7 normalisation: the substrate resolves its own credentials
 * internally. The worker does not import or pass credentials.
 */
async function execute(domain, accountId, intentId, params, retryCount, maxRetries, governance) {
  if (!SUPPORTED_DOMAINS.includes(domain)) {
    (governance.dispatchGlobal || governance.dispatch)({
      type: 'WORKER_OUTCOME_REPORTED',
      accountId, intentId, domain,
      status: 'failed',
      errorShape: {
        category: 'permanent', code: null, retryable: false,
        retryAfterSeconds: null, domain, source: 'unsupported_domain',
      },
      error: `publish-engagement-retry-worker: unsupported domain '${domain}'`,
      latencyMs: 0,
    });
    return;
  }

  const startTime = Date.now();
  const items = params?.items || params?.payload?.items || [];

  if (!items || items.length === 0) {
    (governance.dispatchGlobal || governance.dispatch)({
      type: 'WORKER_OUTCOME_REPORTED',
      accountId, intentId, domain,
      status: 'failed',
      errorShape: {
        category: 'permanent', code: null, retryable: false,
        retryAfterSeconds: null, domain, source: 'missing_items',
      },
      error: 'publish-engagement-retry-worker: missing items',
      latencyMs: 0,
    });
    return;
  }

  let result;
  try {
    // Step 7: substrate resolves credentials internally. Pass 3 args.
    result = await engagementSubstrate.execute(accountId, items, governance);
  } catch (err) {
    const errorShape = publishErrorParser.parseError(err, domain);
    (governance.dispatchGlobal || governance.dispatch)({
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

  if (result && result.success) {
    (governance.dispatchGlobal || governance.dispatch)({
      type: 'WORKER_OUTCOME_REPORTED',
      accountId, intentId, domain,
      status: 'completed',
      result: result.data || result,
      latencyMs: Date.now() - startTime,
      retryCount,
    });
    return;
  }

  const errorShape = publishErrorParser.parse(result, domain);
  (governance.dispatchGlobal || governance.dispatch)({
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
  name: 'publish-engagement-retry-worker',
  execute,
  supportedDomains: SUPPORTED_DOMAINS,
};
