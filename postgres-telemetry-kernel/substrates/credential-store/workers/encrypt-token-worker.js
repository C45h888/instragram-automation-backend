// postgres-telemetry-kernel/substrates/credential-store/workers/encrypt-token-worker.js
// Encrypt Token Worker: encrypt an access token via Supabase vault RPC.
//
// Owns: ONE bounded RPC call — encrypt_instagram_token.
// Does NOT own: key provisioning, credential storage, signal dispatch.

const { getSupabaseAdmin } = require('../../../../config/supabase');

/**
 * @param {{ token: string, encryptionKeyId: string|null }} params
 * @returns {Promise<{ success: boolean, encryptedToken?: string|null, error?: string }>}
 */
async function execute(params) {
  const { token, encryptionKeyId } = params;

  if (!token) {
    return { success: false, encryptedToken: null, error: 'token required' };
  }

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    return { success: false, encryptedToken: null, error: 'supabase_unavailable' };
  }

  try {
    const { data: encryptedToken, error: encryptError } = await supabase
      .rpc('encrypt_instagram_token', { token, p_key_id: encryptionKeyId });

    if (encryptError || !encryptedToken) {
      return { success: false, encryptedToken: null, error: encryptError?.message || 'Encryption returned null' };
    }

    return { success: true, encryptedToken };
  } catch (err) {
    return { success: false, encryptedToken: null, error: err.message };
  }
}

module.exports = { execute };
