// postgres-telemetry-kernel/substrates/credential-store/workers/credential-upsert-worker.js
// Credential Upsert Worker: upsert instagram_credentials row.
//
// Owns: row assembly. All Supabase I/O delegated to bedrock.
// Does NOT own: key provisioning, encryption, business account logic.

const bedrock = require('../../../bedrock');

async function execute(params) {
  const { userId, businessAccountId, encryptedToken, tokenType, scope, expiresAt, dataAccessExpiresAt, pageId } = params;

  const credentialRow = {
    user_id: userId,
    business_account_id: businessAccountId,
    access_token_encrypted: encryptedToken,
    token_type: tokenType,
    scope: scope || [],
    issued_at: new Date().toISOString(),
    expires_at: expiresAt || null,
    data_access_expires_at: dataAccessExpiresAt || null,
    is_active: true,
    last_refreshed_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  };
  if (pageId) credentialRow.page_id = pageId;

  const result = await bedrock.token.persistCredentialState(credentialRow, {
    domain: 'token',
  });

  return { success: result.success, error: result.error || null };
}

module.exports = { execute };
