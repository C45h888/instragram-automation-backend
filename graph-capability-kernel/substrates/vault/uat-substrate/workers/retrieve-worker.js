// graph-capability-kernel/substrates/vault/uat-substrate/workers/retrieve-worker.js
// UAT retrieve worker: decrypt + expiry check only.
// DB reads happen in the façade via CK.governedRead → persist-telemetry FSM.
//
// Owns: ONE bounded decrypt RPC call + expiry validation.
// Does NOT own: DB reads (façade concern), signal dispatch.

const { getSupabaseAdmin } = require('../../../../../config/supabase');

class RetrieveWorker {
  /**
   * @param {{
   *   encryptedToken: string,
   *   encryptionKeyId: string|null,
   *   expiresAt: string|null,
   *   dataAccessExpiresAt: string|null,
   *   scope: string[],
   *   issuedAt: string,
   * }} input
   * @returns {Promise<{ token: string, expiresAt: string|null, dataAccessExpiresAt: string|null, scope: string[], issuedAt: string }>}
   * @throws if expired or decryption fails
   */
  async execute({ encryptedToken, encryptionKeyId, expiresAt, dataAccessExpiresAt, scope, issuedAt }) {
    // ── Expiry check ──────────────────────────────────────────────────────
    if (expiresAt && new Date(expiresAt) < new Date()) {
      throw new Error('UAT has expired. User must reconnect via OAuth.');
    }

    // ── Decrypt (vault concern) ────────────────────────────────────────────
    const supabase = getSupabaseAdmin();
    if (!supabase) throw new Error('Database not available');

    const { data: decryptedToken, error: decryptError } = await supabase
      .rpc('decrypt_instagram_token', { encrypted_token: encryptedToken, p_key_id: encryptionKeyId });

    if (decryptError || !decryptedToken) throw new Error(`UAT decryption failed: ${decryptError?.message || 'null result'}`);

    return {
      token: decryptedToken,
      expiresAt,
      dataAccessExpiresAt,
      scope,
      issuedAt,
    };
  }
}

module.exports = RetrieveWorker;