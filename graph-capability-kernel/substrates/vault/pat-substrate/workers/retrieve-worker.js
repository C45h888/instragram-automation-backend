// graph-capability-kernel/substrates/vault/pat-substrate/workers/retrieve-worker.js
// PAT retrieve worker: decrypt only.
// DB reads happen in the façade via CK.governedRead → persist-telemetry FSM.
//
// Owns: ONE bounded decrypt RPC call.
// Does NOT own: DB reads (façade concern), expiry checks, signal dispatch.

const { getSupabaseAdmin } = require('../../../../../config/supabase');

class RetrieveWorker {
  /**
   * @param {{ encryptedToken: string, encryptionKeyId: string|null }} input
   * @returns {Promise<string>} decrypted token
   * @throws if decryption fails
   */
  async execute({ encryptedToken, encryptionKeyId }) {
    const supabase = getSupabaseAdmin();
    if (!supabase) throw new Error('Database not available');

    const { data: decryptedToken, error: decryptError } = await supabase
      .rpc('decrypt_instagram_token', { encrypted_token: encryptedToken, p_key_id: encryptionKeyId });

    if (decryptError || !decryptedToken) throw new Error(`Token decryption failed: ${decryptError?.message || 'null result'}`);

    return decryptedToken;
  }
}

module.exports = RetrieveWorker;