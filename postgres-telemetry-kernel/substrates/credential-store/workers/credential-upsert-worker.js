// postgres-telemetry-kernel/substrates/credential-store/workers/credential-upsert-worker.js
// Credential Upsert Worker: upsert instagram_credentials row.
//
// Owns: ONE bounded UPSERT on instagram_credentials.
// Does NOT own: key provisioning, encryption, business account logic.

const { getSupabaseAdmin } = require('../../../../config/supabase');

/**
 * @param {{
 *   userId: string,
 *   businessAccountId: string,
 *   encryptedToken: string,
 *   tokenType: 'page'|'user',
 *   scope: string[],
 *   expiresAt: string|null,
 *   dataAccessExpiresAt: string|null,
 *   pageId?: string,
 * }} params
 * @returns {Promise<{ success: boolean, error?: string }>}
 */
async function execute(params) {
  const { userId, businessAccountId, encryptedToken, tokenType, scope, expiresAt, dataAccessExpiresAt, pageId } = params;

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    return { success: false, error: 'supabase_unavailable' };
  }

  try {
    const credentialRow = {
      user_id: userId,
      business_account_id: businessAccountId,
      access_token_encrypted: encryptedToken,
      token_type: tokenType,
      scope: scope || [],
      issued_at: new Date().toISOString(),
      expires_at: expiresAt || null,
      data_access_expires_at: dataAccessExpiresAt || null,
      is_active: true,
      last_refreshed_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    };
    if (pageId) credentialRow.page_id = pageId;

    const { error } = await supabase
      .from('instagram_credentials')
      .upsert(credentialRow, {
        onConflict: 'user_id,business_account_id,token_type',
        ignoreDuplicates: false,
      });

    if (error) {
      return { success: false, error: error.message };
    }

    return { success: true };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

module.exports = { execute };
