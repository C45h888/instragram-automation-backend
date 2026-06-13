// postgres-telemetry-kernel/substrates/credential-store/workers/key-provision-worker.js
// Key Provision Worker: get or create encryption key for credential storage.
//
// Owns: operation routing. All Supabase I/O + vault ops delegated to bedrock.
// Does NOT own: encryption, business account logic, credential storage.

const bedrock = require('../../../bedrock');

async function execute(params) {
  const { userId, igBusinessAccountId, operation } = params;

  // ── PAT: lookup/create key by user_id + instagram_business_id ────
  if (operation === 'store_pat' && igBusinessAccountId) {
    return bedrock.token.provisionEncryptionKey(userId, igBusinessAccountId);
  }

  // ── UAT: lookup existing key by business_account_id ──────────────
  if (operation === 'store_uat') {
    const result = await bedrock.token.getEncryptionKey(userId, params.businessAccountId);
    return { success: result.success, encryptionKeyId: result.data?.encryption_key_id || null };
  }

  return { success: true, encryptionKeyId: null };
}

module.exports = { execute };
