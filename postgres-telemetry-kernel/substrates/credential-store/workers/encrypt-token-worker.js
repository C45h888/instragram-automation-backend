// postgres-telemetry-kernel/substrates/credential-store/workers/encrypt-token-worker.js
// Encrypt Token Worker: encrypt via Supabase vault RPC.
//
// Owns: param validation. RPC delegated to bedrock.
// Does NOT own: key provisioning, credential storage, signal dispatch.

const bedrock = require('../../../bedrock');

async function execute(params) {
  const { token, encryptionKeyId } = params;

  if (!token) {
    return { success: false, encryptedToken: null, error: 'token required' };
  }

  const result = await bedrock.rpc.encryptToken(token, encryptionKeyId);

  if (!result.success) {
    return { success: false, encryptedToken: null, error: result.error || 'Encryption failed' };
  }

  return { success: true, encryptedToken: result.data, error: null };
}

module.exports = { execute };
