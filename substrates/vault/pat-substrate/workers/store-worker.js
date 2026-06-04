// substrates/vault/pat-substrate/workers/store-worker.js
// PAT store worker: vault key provisioning + upsert business_account + encrypt + upsert credential + audit.
// Migrated from services/tokens/pat.js: storePageToken.
//
// Owns: ONE bounded supabase transaction.
// Does NOT own: factory, pre-flight, orchestration, retry, cache invalidation.

const crypto = require('crypto');
const { getSupabaseAdmin, logAudit } = require('../../../../config/supabase');
const { clearCredentialCache } = require('../../../../helpers/credential-cache');
const { PAT_SCOPE_DEFAULTS } = require('../../default-scopes');

class StoreWorker {
  /**
   * @param {{ userId: string, igBusinessAccountId: string, pageAccessToken: string, pageId: string, pageName: string, scope?: string[] }} input
   * @returns {Promise<{ success: boolean, businessAccountId?: string, expiresAt?: string|null, error?: string }>}
   */
  async execute({ userId, igBusinessAccountId, pageAccessToken, pageId, pageName, scope }) {
    try {
      const supabase = getSupabaseAdmin();
      if (!supabase) return { success: false, error: 'Database not available' };
      if (!pageName) return { success: false, error: 'pageName is required' };

      // Provision or reuse per-user Vault encryption key
      let encryptionKeyId = null;
      try {
        const { data: existingAccount } = await supabase
          .from('instagram_business_accounts')
          .select('encryption_key_id')
          .eq('user_id', userId)
          .eq('instagram_business_id', igBusinessAccountId)
          .maybeSingle();

        if (existingAccount?.encryption_key_id) {
          encryptionKeyId = existingAccount.encryption_key_id;
        } else {
          const userKey = crypto.randomBytes(32).toString('hex');
          const { data: vaultSecret, error: vaultError } = await supabase
            .schema('vault').from('secrets')
            .insert({ name: `instagram_token_key_${userId}`, secret: userKey, description: `Per-user Instagram token encryption key for user ${userId}` })
            .select('id').single();
          if (!vaultError) encryptionKeyId = vaultSecret.id;
        }
      } catch (keyErr) {
        console.warn('⚠️ Key provisioning error, using shared key:', keyErr.message);
      }

      const finalScope = scope || PAT_SCOPE_DEFAULTS;

      const { data: businessAccount, error: accountError } = await supabase
        .from('instagram_business_accounts')
        .upsert({
          user_id: userId, instagram_business_id: igBusinessAccountId,
          name: pageName, username: pageName, is_connected: true,
          connection_status: 'active', encryption_key_id: encryptionKeyId,
          granted_permissions: finalScope, last_sync_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
        }, { onConflict: 'user_id,instagram_business_id', ignoreDuplicates: false })
        .select().single();

      if (accountError) return { success: false, error: `Failed to create business account: ${accountError.message}` };

      const { data: encryptedToken, error: encryptError } = await supabase
        .rpc('encrypt_instagram_token', { token: pageAccessToken, p_key_id: encryptionKeyId });

      if (encryptError || !encryptedToken) {
        return { success: false, error: encryptError?.message || 'Encryption returned null' };
      }

      const { data: credentialData, error: credentialError } = await supabase
        .from('instagram_credentials')
        .upsert({
          user_id: userId, business_account_id: businessAccount.id,
          access_token_encrypted: encryptedToken, token_type: 'page',
          page_id: pageId, scope: finalScope, issued_at: new Date().toISOString(),
          expires_at: null, is_active: true,
          last_refreshed_at: new Date().toISOString(), updated_at: new Date().toISOString(),
        }, { onConflict: 'user_id,business_account_id,token_type' })
        .select();

      if (credentialError) return { success: false, error: `Failed to store credentials: ${credentialError.message}` };

      // Audit log — non-blocking
      try {
        await logAudit('token_stored', userId, {
          action: 'store_page_token', business_account_id: businessAccount.id,
          page_id: pageId, scope: finalScope, credential_id: credentialData?.[0]?.id, success: true,
        });
      } catch (auditError) {
        console.warn('⚠️ Audit logging failed (non-blocking):', auditError.message);
      }

      clearCredentialCache(businessAccount.id);

      return { success: true, businessAccountId: businessAccount.id, expiresAt: null };
    } catch (error) {
      return { success: false, error: error.message || 'Unknown error storing page token' };
    }
  }
}

module.exports = StoreWorker;
