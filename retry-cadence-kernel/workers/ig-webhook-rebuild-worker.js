// retry-cadence-kernel/workers/ig-webhook-rebuild-worker.js
// IG Webhook Rebuild Worker — resubscribes to IG webhooks when
// webhook health is DEGRADED or FAILED. The IG API allows
// subscribing to specific topics; this worker re-establishes
// the subscription via the graph API.

const axios = require('axios');
const { resolveAccountCredentials } =
  require('../../graph-capability-kernel/substrates/credential-resolver');

const GRAPH_API_BASE = 'https://graph.instagram.com';

async function execute(event, governance) {
  const startTime = Date.now();
  const { accountId, intentId, params } = event || {};
  const callbackUrl = params?.callbackUrl || process.env.IG_WEBHOOK_CALLBACK_URL;
  const verifyToken = params?.verifyToken || process.env.IG_WEBHOOK_VERIFY_TOKEN;
  const topics = params?.topics || ['comments', 'mentions', 'messages'];

  if (!callbackUrl || !verifyToken) {
    return {
      success: false,
      workerName: 'ig-webhook-rebuild-worker',
      durationMs: 0,
      error: 'missing_webhook_config',
    };
  }

  let credentials;
  try {
    credentials = await resolveAccountCredentials(accountId);
  } catch (err) {
    return {
      success: false,
      workerName: 'ig-webhook-rebuild-worker',
      durationMs: Date.now() - startTime,
      error: `credential_resolver_failure: ${err.message}`,
    };
  }

  const results = [];
  for (const topic of topics) {
    try {
      const response = await axios.post(`${GRAPH_API_BASE}/${credentials.igUserId}/subscribed_apps`, {
        subscribed_fields: [topic],
        callback_url: callbackUrl,
        verify_token: verifyToken,
        access_token: credentials.accessToken,
      }, { timeout: 15000 });
      results.push({ topic, success: true, data: response.data });
    } catch (err) {
      results.push({ topic, success: false, error: err.message });
    }
  }

  const allOk = results.every((r) => r.success);

  if (governance?.dispatchGlobal) {
    governance.dispatchGlobal({
      type: 'IG_WEBHOOK_REBUILD_COMPLETE',
      accountId, intentId,
      results,
      durationMs: Date.now() - startTime,
    });
  }

  return {
    success: allOk,
    workerName: 'ig-webhook-rebuild-worker',
    durationMs: Date.now() - startTime,
    error: allOk ? null : 'partial_or_full_failure',
    data: { results },
  };
}

module.exports = {
  name: 'ig-webhook-rebuild-worker',
  execute,
};
