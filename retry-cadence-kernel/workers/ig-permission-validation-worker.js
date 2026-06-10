// retry-cadence-kernel/workers/ig-permission-validation-worker.js
// IG Permission Validation Worker — fetches the token's granted
// permissions and compares them against the operation's required
// scopes. Used when the substrate emits VALIDATE_PERMISSIONS
// (PERMISSION_FAILURE category).

const axios = require('axios');
const { resolveAccountCredentials } =
  require('../../graph-capability-kernel/substrates/credential-resolver');

const GRAPH_API_BASE = 'https://graph.instagram.com';

// Required scope per publish domain
const REQUIRED_SCOPES = {
  'publish:post':     ['instagram_basic', 'instagram_content_publish'],
  'publish:story':    ['instagram_basic', 'instagram_content_publish'],
  'publish:comment':  ['instagram_basic', 'instagram_manage_comments'],
  'publish:message':  ['instagram_basic', 'instagram_manage_messages'],
  'read:comments':    ['instagram_basic'],
  'read:messages':    ['instagram_basic'],
};

async function execute(event, governance) {
  const startTime = Date.now();
  const { accountId, intentId, domain, analysis } = event || {};

  const required = REQUIRED_SCOPES[domain] || REQUIRED_SCOPES['publish:post'];

  let credentials;
  try {
    credentials = await resolveAccountCredentials(accountId);
  } catch (err) {
    return {
      success: false,
      workerName: 'ig-permission-validation-worker',
      durationMs: Date.now() - startTime,
      error: `credential_resolver_failure: ${err.message}`,
    };
  }

  // Fetch granted permissions via /me/permissions
  let response;
  try {
    response = await axios.get(`${GRAPH_API_BASE}/me/permissions`, {
      params: { access_token: credentials.accessToken },
      timeout: 15000,
    });
  } catch (err) {
    return {
      success: false,
      workerName: 'ig-permission-validation-worker',
      durationMs: Date.now() - startTime,
      error: `ig_permissions_endpoint_failure: ${err.message}`,
    };
  }

  const granted = (response.data?.data || [])
    .filter((p) => p.status === 'granted')
    .map((p) => p.permission);

  const missing = required.filter((s) => !granted.includes(s));
  const valid = missing.length === 0;

  if (!valid && governance?.dispatchGlobal) {
    governance.dispatchGlobal({
      type: 'AUTH_FAILURE_STRIKE',
      accountId,
      error: `missing_scopes: ${missing.join(', ')}`,
    });
  }

  if (governance?.dispatchGlobal) {
    governance.dispatchGlobal({
      type: 'IG_PERMISSION_VALIDATION_COMPLETE',
      accountId, intentId, domain,
      granted, missing, valid,
      durationMs: Date.now() - startTime,
    });
  }

  return {
    success: valid,
    workerName: 'ig-permission-validation-worker',
    durationMs: Date.now() - startTime,
    error: valid ? null : `missing_scopes: ${missing.join(', ')}`,
    data: { granted, missing, required, valid },
  };
}

module.exports = {
  name: 'ig-permission-validation-worker',
  execute,
};
