// postgres-telemetry-kernel/substrates/graph-capability/workers/read-key-worker.js
// Encryption key reader: SELECT on instagram_business_accounts.
//
// Owns: param validation. All Supabase I/O delegated to bedrock.
// Does NOT own: key provisioning, encryption RPC, signal dispatch.

const bedrock = require('../../../bedrock');

async function execute(params, governance) {
  const { userId, businessAccountId } = params;
  if (!userId || !businessAccountId) {
    return { success: false, data: null, error: 'userId and businessAccountId required' };
  }

  const result = await bedrock.token.getEncryptionKey(userId, businessAccountId);

  // Ensure data is never null (caller expects { encryption_key_id: null })
  if (result.success && !result.data) {
    return { success: true, data: { encryption_key_id: null }, error: null };
  }

  return result;
}

module.exports = { execute };
