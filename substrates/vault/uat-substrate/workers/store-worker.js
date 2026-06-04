// substrates/vault/uat-substrate/workers/store-worker.js
// UAT store worker: encrypt + upsert credential row.
// Migrated from services/tokens/uat.js: storeUserToken.

const { getSupabaseAdmin } = require('../../../../config/supabase');

class StoreWorker {
  /**
   * @param {{ userId: string, businessAccountId: string, userAccessToken: string, scope?: string[], expiresAt?: string|null, dataAccessExpiresAt?: string|null }} input
   * @returns {Promise<{ success: boolean, error?: string }>}
   */
  async execute({ userId, businessAccountId, userAccessToken, scope, expiresAt, dataAccessExpiresAt }) {
    try {
      const supabase = getSupabaseAdmin();
      if (!supabase) return { success: false, error: 'Database not available' };

      let encryptionKeyId = null;
      try {
        const { data: bizAccount } = await supabase
          .from('instagram_business_accounts')
          .select('encryption_key_id').eq('id', businessAccountId).maybeSingle();
        encryptionKeyId = bizAccount?.encryption_key_id || null;
      } catch (keyErr) {
        console.warn('⚠️ UAT encryption key lookup failed, using shared key:', keyErr.message);
      }

      const { data: encryptedToken, error: encryptError } = await supabase
        .rpc('encrypt_instagram_token', { token: userAccessToken, p_key_id: encryptionKeyId });

      if (encryptError) return { success: false, error: encryptError.message };

      const { error: credError } = await supabase
        .from('instagram_credentials')
        .upsert({
          user_id: userId, business_account_id: businessAccountId,
          access_token_encrypted: encryptedToken, token_type: 'user',
          scope: scope || [], issued_at: new Date().toISOString(),
          expires_at: expiresAt, data_access_expires_at: dataAccessExpiresAt || null,
          is_active: true, last_refreshed_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
        }, { onConflict: 'user_id,business_account_id,token_type' })
        .select();

      if (credError) return { success: false, error: credError.message };

      console.log('✅ UAT stored in vault (token_type=user)');
      return { success: true };
    } catch (err) {
      return { success: false, error: err.message };
    }
  }
}

module.exports = StoreWorker;
