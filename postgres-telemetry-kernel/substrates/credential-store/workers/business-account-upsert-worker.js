// postgres-telemetry-kernel/substrates/credential-store/workers/business-account-upsert-worker.js
// Business Account Upsert Worker: upsert instagram_business_accounts row.
//
// Owns: row assembly + PAT/UAT logic. All Supabase I/O delegated to bedrock.
// Does NOT own: key provisioning, encryption, credential storage.

const bedrock = require('../../../bedrock');

async function execute(params) {
  const { userId, igBusinessAccountId, pageName, encryptionKeyId, scope, operation, existingBusinessAccountId } = params;

  // UAT: business account already exists, return existing ID
  if (operation === 'store_uat') {
    return { success: true, businessAccountId: existingBusinessAccountId };
  }

  // PAT: upsert business account
  if (!userId || !igBusinessAccountId) {
    return { success: false, error: 'userId and igBusinessAccountId required for PAT store' };
  }

  const row = {
    user_id: userId,
    instagram_business_id: igBusinessAccountId,
    name: pageName,
    username: pageName,
    is_connected: true,
    connection_status: 'active',
    encryption_key_id: encryptionKeyId,
    granted_permissions: scope,
    last_sync_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  };

  const result = await bedrock.token.persistBusinessAccount(row, {
    domain: 'token',
  });

  // Note: the old worker used .select().single() to get the returned ID.
  // Bedrock upsert returns {success, data} — the returned row's id can be
  // accessed via result.data if needed. For now, caller handles businessAccountId.
  return {
    success: result.success,
    businessAccountId: result.data?.id || existingBusinessAccountId,
    error: result.error || null,
  };
}

module.exports = { execute };
