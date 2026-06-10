// retry-cadence-kernel/workers/ig-proactive-refresh-worker.js
// IG Proactive Refresh Worker — pre-emptive token refresh
// before expiry. The substrate (§3) emits PROACTIVE_REFRESH
// when the token is within the refresh window (default 7 days)
// but no failure has occurred yet. This worker refreshes the
// token to prevent operational failures.

const { resolveAccountCredentials } =
  require('../../graph-capability-kernel/substrates/credential-resolver');
const axios = require('axios');

const GRAPH_API_BASE = 'https://graph.instagram.com';

async function execute(event, governance) {
  const startTime = Date.now();
  const { accountId, intentId } = event || {};

  if (!accountId) {
    return {
      success: false,
      workerName: 'ig-proactive-refresh-worker',
      durationMs: 0,
      error: 'missing_accountId',
    };
  }

  let credentials;
  try {
    credentials = await resolveAccountCredentials(accountId);
  } catch (err) {
    return {
      success: false,
      workerName: 'ig-proactive-refresh-worker',
      durationMs: Date.now() - startTime,
      error: `credential_resolver_failure: ${err.message}`,
    };
  }

  if (!credentials?.accessToken) {
    return {
      success: false,
      workerName: 'ig-proactive-refresh-worker',
      durationMs: Date.now() - startTime,
      error: 'no_token',
    };
  }

  let response;
  try {
    response = await axios.get(`${GRAPH_API_BASE}/refresh_access_token`, {
      params: {
        grant_type: 'ig_refresh_token',
        access_token: credentials.accessToken,
      },
      timeout: 15000,
    });
  } catch (err) {
    return {
      success: false,
      workerName: 'ig-proactive-refresh-worker',
      durationMs: Date.now() - startTime,
      error: `ig_refresh_endpoint_failure: ${err.message}`,
    };
  }

  const { access_token, expires_in } = response.data || {};
  if (!access_token) {
    return {
      success: false,
      workerName: 'ig-proactive-refresh-worker',
      durationMs: Date.now() - startTime,
      error: 'ig_refresh_returned_no_token',
    };
  }

  try {
    const { persistRefreshedToken } = require(
      '../../graph-capability-kernel/substrates/credential-resolver'
    );
    if (typeof persistRefreshedToken === 'function') {
      await persistRefreshedToken(accountId, access_token, expires_in);
    }
  } catch (err) {
    if (governance?.dispatchGlobal) {
      governance.dispatchGlobal({
        type: 'TOKEN_REFRESH_PERSIST_FAILED',
        accountId,
        context: 'proactive',
        error: err.message,
      });
    }
  }

  if (governance?.dispatchGlobal) {
    governance.dispatchGlobal({
      type: 'TOKEN_PROACTIVE_REFRESH_COMPLETE',
      accountId, intentId,
      expiresIn: expires_in,
      durationMs: Date.now() - startTime,
    });
  }

  return {
    success: true,
    workerName: 'ig-proactive-refresh-worker',
    durationMs: Date.now() - startTime,
    error: null,
    data: { expiresIn: expires_in, proactive: true },
  };
}

module.exports = {
  name: 'ig-proactive-refresh-worker',
  execute,
};
