// postgres-telemetry-kernel/substrates/credential-store/workers/business-account-upsert-worker.js
// Business Account Upsert Worker: upsert instagram_business_accounts row.
//
// Owns: ONE bounded UPSERT on instagram_business_accounts.
// Does NOT own: key provisioning, encryption, credential storage.

const { getSupabaseAdmin } = require('../../../../config/supabase');

/**
 * @param {{
 *   userId: string,
 *   igBusinessAccountId: string,
 *   pageName: string,
 *   encryptionKeyId: string|null,
 *   scope: string[],
 *   operation: 'store_pat'|'store_uat',
 *   existingBusinessAccountId?: string,
 * }} params
 * @returns {Promise<{ success: boolean, businessAccountId?: string, error?: string }>}
 */
async function execute(params) {
  const { userId, igBusinessAccountId, pageName, encryptionKeyId, scope, operation, existingBusinessAccountId } = params;

  // UAT: business account already exists, return existing ID
  if (operation === 'store_uat') {
    return { success: true, businessAccountId: existingBusinessAccountId };
  }

  // PAT: upsert business account
  if (!userId || !igBusinessAccountId) {
    return { success: false, error: 'userId and igBusinessAccountId required for PAT store' };
  }

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    return { success: false, error: 'supabase_unavailable' };
  }

  try {
    const { data: businessAccount, error } = await supabase
      .from('instagram_business_accounts')
      .upsert({
        user_id: userId,
        instagram_business_id: igBusinessAccountId,
        name: pageName,
        username: pageName,
        is_connected: true,
        connection_status: 'active',
        encryption_key_id: encryptionKeyId,
        granted_permissions: scope,
        last_sync_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }, {
        onConflict: 'user_id,instagram_business_id',
        ignoreDuplicates: false,
      })
      .select()
      .single();

    if (error) {
      return { success: false, error: error.message };
    }

    return { success: true, businessAccountId: businessAccount.id };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

module.exports = { execute };
