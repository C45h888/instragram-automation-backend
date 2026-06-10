// postgres-telemetry-kernel/substrates/credential-store/workers/key-provision-worker.js
// Key Provision Worker: get or create encryption key for credential storage.
//
// Owns: ONE bounded operation — SELECT existing key OR INSERT new vault secret.
// Does NOT own: encryption, business account logic, credential storage.

const crypto = require('crypto');
const { getSupabaseAdmin } = require('../../../../config/supabase');

/**
 * @param {{ userId: string, igBusinessAccountId?: string, operation: 'store_pat'|'store_uat' }} params
 * @returns {Promise<{ success: boolean, encryptionKeyId?: string|null, error?: string }>}
 */
async function execute(params) {
  const { userId, igBusinessAccountId, operation } = params;

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    return { success: false, encryptionKeyId: null, error: 'supabase_unavailable' };
  }

  try {
    // ── PAT: lookup existing key by user_id + instagram_business_id ────────
    if (operation === 'store_pat' && igBusinessAccountId) {
      const { data: existingAccount } = await supabase
        .from('instagram_business_accounts')
        .select('encryption_key_id')
        .eq('user_id', userId)
        .eq('instagram_business_id', igBusinessAccountId)
        .maybeSingle();

      if (existingAccount?.encryption_key_id) {
        return { success: true, encryptionKeyId: existingAccount.encryption_key_id };
      }

      // No existing key — create one
      const userKey = crypto.randomBytes(32).toString('hex');
      const { data: vaultSecret, error: vaultError } = await supabase
        .schema('vault')
        .from('secrets')
        .insert({
          name: `instagram_token_key_${userId}`,
          secret: userKey,
          description: `Per-user Instagram token encryption key for user ${userId}`,
        })
        .select('id')
        .single();

      if (vaultError) {
        console.warn('⚠️ Key provisioning error, using shared key:', vaultError.message);
        return { success: true, encryptionKeyId: null };
      }

      return { success: true, encryptionKeyId: vaultSecret.id };
    }

    // ── UAT: lookup existing key by business_account_id ────────────────────
    if (operation === 'store_uat') {
      const { data: bizAccount } = await supabase
        .from('instagram_business_accounts')
        .select('encryption_key_id')
        .eq('id', params.businessAccountId)
        .maybeSingle();

      return { success: true, encryptionKeyId: bizAccount?.encryption_key_id || null };
    }

    return { success: true, encryptionKeyId: null };
  } catch (err) {
    return { success: false, encryptionKeyId: null, error: err.message };
  }
}

module.exports = { execute };
