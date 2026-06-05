// graph-capability-kernel/substrates/vault/pat-substrate/workers/retrieve-worker.js
// PAT retrieve worker: supabase select + decrypt.
// Migrated from substrates/vault/pat-substrate/workers/retrieve-worker.js
//
// Owns: ONE bounded supabase read + decrypt.
// Does NOT own: factory, pre-flight, retry, cache, encryption key provisioning.

const { getSupabaseAdmin } = require('../../../../../config/supabase');

class RetrieveWorker {
  /**
   * @param {{ userId: string, businessAccountId: string }} input
   * @returns {Promise<string>} decrypted token
   * @throws {Error} if not found or decryption fails
   */
  async execute({ userId, businessAccountId }) {
    try {
      const supabase = getSupabaseAdmin();
      if (!supabase) throw new Error('Database not available');

      const { data, error } = await supabase
        .from('instagram_credentials')
        .select('*')
        .eq('user_id', userId)
        .eq('business_account_id', businessAccountId)
        .eq('token_type', 'page')
        .eq('is_active', true)
        .single();

      if (error?.code === 'PGRST116') throw new Error('No page token found. User must complete OAuth flow first.');
      if (error) throw new Error(`Database error: ${error.message}`);
      if (!data) throw new Error('No page token found.');

      // Legacy expires_at warning only — page tokens are non-expiring
      if (data.expires_at && new Date(data.expires_at) < new Date()) {
        console.warn(`[retrievePageToken] Legacy expires_at in past for user ${userId}. Ignoring — page tokens are non-expiring.`);
      }

      let encryptionKeyId = null;
      try {
        const { data: bizAccount } = await supabase
          .from('instagram_business_accounts')
          .select('encryption_key_id')
          .eq('user_id', userId).eq('id', businessAccountId).maybeSingle();
        encryptionKeyId = bizAccount?.encryption_key_id || null;
      } catch (keyLookupErr) {
        console.warn('⚠️ Could not look up per-user encryption key:', keyLookupErr.message);
      }

      const { data: decryptedToken, error: decryptError } = await supabase
        .rpc('decrypt_instagram_token', { encrypted_token: data.access_token_encrypted, p_key_id: encryptionKeyId });

      if (decryptError || !decryptedToken) throw new Error(`Token decryption failed: ${decryptError?.message || 'null result'}`);

      return decryptedToken;
    } catch (error) {
      console.error('❌ Failed to retrieve page token:', error.message);
      throw error;
    }
  }
}

module.exports = RetrieveWorker;