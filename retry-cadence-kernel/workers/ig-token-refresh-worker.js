// retry-cadence-kernel/workers/ig-token-refresh-worker.js
// IG Token Refresh Worker — operationally bounded executor that
// refreshes an Instagram Graph long-lived access token.
//
// CONSTITUTIONAL CONTRACT (Phase 4):
//   Owns: ONE HTTP call to IG's token refresh endpoint. The worker
//          resolves credentials, calls the refresh endpoint, and
//          persists the new token via the credential-resolver.
//   Does NOT own: token classification (ig-reliability-substrate §3),
//                 refresh policy (policy.js), retry decisions
//                 (engagement-fsm), state.
//
// USAGE:
//   Invoked by engagement-fsm's REFRESH_TOKEN_AUTHORIZED transition
//   via ig-recovery-substrate.execute().
//
// IG REFRESH ENDPOINT:
//   GET https://graph.instagram.com/refresh_access_token
//     ?grant_type=ig_refresh_token
//     &access_token={long-lived-token}
//
//   Returns: { access_token, token_type, expires_in }
//
//   Long-lived tokens are valid for ~60 days. IG allows ONE refresh
//   per long-lived token. After that, the user must re-authorize.
//   The substrate (§3) tracks refresh history and emits
//   REAUTHORIZE_USER when refresh budget is exhausted.

const { resolveAccountCredentials } =
  require('../../graph-capability-kernel/substrates/credential-resolver');
const axios = require('axios');

const GRAPH_API_BASE = 'https://graph.instagram.com';

/**
 * Execute a token refresh.
 *
 * @param {object} event — { accountId, analysis: { token, ... }, ... }
 * @param {object} governance — CK reference (used to dispatch
 *                              REFRESH_TOKEN_COMPLETE for observability)
 * @returns {Promise<{ success, workerName, durationMs, error, data }>}
 */
async function execute(event, governance) {
  const startTime = Date.now();
  const { accountId, analysis } = event || {};

  if (!accountId) {
    return {
      success: false,
      workerName: 'ig-token-refresh-worker',
      durationMs: 0,
      error: 'missing_accountId',
    };
  }

  // Resolve the current long-lived token via the credential-resolver
  // (the substrate integrates with this — per spec §4, the substrate
  // never touches credential storage directly)
  let credentials;
  try {
    credentials = await resolveAccountCredentials(accountId);
  } catch (err) {
    return {
      success: false,
      workerName: 'ig-token-refresh-worker',
      durationMs: Date.now() - startTime,
      error: `credential_resolver_failure: ${err.message}`,
    };
  }

  if (!credentials || !credentials.accessToken) {
    return {
      success: false,
      workerName: 'ig-token-refresh-worker',
      durationMs: Date.now() - startTime,
      error: 'no_long_lived_token',
    };
  }

  // Call IG refresh endpoint
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
    const msg = err.response?.data?.error?.message || err.message;
    return {
      success: false,
      workerName: 'ig-token-refresh-worker',
      durationMs: Date.now() - startTime,
      error: `ig_refresh_endpoint_failure: ${msg}`,
    };
  }

  const { access_token, expires_in } = response.data || {};
  if (!access_token) {
    return {
      success: false,
      workerName: 'ig-token-refresh-worker',
      durationMs: Date.now() - startTime,
      error: 'ig_refresh_returned_no_token',
    };
  }

  // Persist the new token via the credential-resolver. This is a
  // delegated write — the resolver owns credential storage, the
  // worker is the operationally bounded executor.
  try {
    const { persistRefreshedToken } = require(
      '../../graph-capability-kernel/substrates/credential-resolver'
    );
    if (typeof persistRefreshedToken === 'function') {
      await persistRefreshedToken(accountId, access_token, expires_in);
    }
  } catch (err) {
    // Refresh succeeded at IG, but persistence failed. Emit a
    // signal so the operator can intervene.
    if (governance?.dispatchGlobal) {
      governance.dispatchGlobal({
        type: 'TOKEN_REFRESH_PERSIST_FAILED',
        accountId,
        error: err.message,
      });
    }
  }

  // Emit observability
  if (governance?.dispatchGlobal) {
    governance.dispatchGlobal({
      type: 'TOKEN_REFRESH_COMPLETE',
      accountId,
      expiresIn: expires_in,
      durationMs: Date.now() - startTime,
    });
  }

  return {
    success: true,
    workerName: 'ig-token-refresh-worker',
    durationMs: Date.now() - startTime,
    error: null,
    data: { expiresIn: expires_in, refreshCompleted: true },
  };
}

module.exports = {
  name: 'ig-token-refresh-worker',
  execute,
};
