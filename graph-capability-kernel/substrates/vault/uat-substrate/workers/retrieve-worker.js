// graph-capability-kernel/substrates/vault/uat-substrate/workers/retrieve-worker.js
// UAT retrieve worker: select + decrypt + expiry check.
// Migrated from substrates/vault/uat-substrate/workers/retrieve-worker.js

const { getSupabaseAdmin } = require('../../../../../config/supabase');

class RetrieveWorker {
  /**
   * @param {{ userId: string, businessAccountId: string }} input
   * @returns {Promise<{ token: string, expiresAt: string|null, dataAccessExpiresAt: string|null, scope: string[], issuedAt: string }>}
   * @throws if not found or expired
   */
  async execute({ userId, businessAccountId }) {
    const supabase = getSupabaseAdmin();
    if (!supabase) throw new Error('Database not available');

    const { data, error } = await supabase
      .from('instagram_credentials')
      .select('*')
      .eq('user_id', userId).eq('business_account_id', businessAccountId)
      .eq('token_type', 'user').eq('is_active', true).single();

    if (error?.code === 'PGRST116') throw new Error('No UAT found. User must complete OAuth flow.');
    if (error || !data) throw new Error(`UAT retrieval failed: ${error?.message || 'not found'}`);

    if (data.expires_at && new Date(data.expires_at) < new Date()) {
      throw new Error('UAT has expired. User must reconnect via OAuth.');
    }

    let encryptionKeyId = null;
    try {
      const { data: bizAccount } = await supabase
        .from('instagram_business_accounts')
        .select('encryption_key_id').eq('id', businessAccountId).maybeSingle();
      encryptionKeyId = bizAccount?.encryption_key_id || null;
    } catch (keyErr) {
      console.warn('⚠️ UAT key lookup failed, using shared key:', keyErr.message);
    }

    const { data: decryptedToken, error: decryptError } = await supabase
      .rpc('decrypt_instagram_token', { encrypted_token: data.access_token_encrypted, p_key_id: encryptionKeyId });

    if (decryptError || !decryptedToken) throw new Error(`UAT decryption failed: ${decryptError?.message || 'null result'}`);

    return {
      token: decryptedToken,
      expiresAt: data.expires_at,
      dataAccessExpiresAt: data.data_access_expires_at,
      scope: data.scope,
      issuedAt: data.issued_at,
    };
  }
}

module.exports = RetrieveWorker;