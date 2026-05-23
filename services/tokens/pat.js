// backend.api/services/tokens/pat.js
// Page Access Token lifecycle: exchange → encrypt → store → retrieve.
// Imports clearCredentialCache from helpers/credential-cache (not agent-helpers)
// to permanently break the circular dependency.

const { axios, GRAPH_API_BASE, PAT_SCOPE_DEFAULTS } = require('./base');
const { getSupabaseAdmin, logAudit } = require('../../config/supabase');
const { clearCredentialCache } = require('../../helpers/credential-cache');

/**
 * Exchange user access token for page token + discover IG Business Account.
 * Returns success object — single page auto-selects, multiple pages return picker list.
 * @param {string} userAccessToken
 * @returns {Promise<{success, requiresSelection?, pageAccessToken?, pageId?, pageName?, igBusinessAccountId?, pages?, error?}>}
 */
async function exchangeForPageToken(userAccessToken) {
  try {
    console.log('🔄 Starting page token exchange...');
    const pagesResponse = await axios.get(`${GRAPH_API_BASE}/me/accounts`, {
      params: { fields: 'id,name,access_token,instagram_business_account', access_token: userAccessToken },
      timeout: 10000
    });

    const pages = pagesResponse.data.data;
    if (!pages || pages.length === 0) {
      return { success: false, error: 'No Facebook pages found. Please ensure you have a Facebook Page connected to your account.' };
    }

    const pagesWithIG = pages.filter(p => p.instagram_business_account?.id);
    if (pagesWithIG.length === 0) {
      return { success: false, error: 'No Instagram Business Account connected.', errorCode: 'NO_IG_BUSINESS_ACCOUNT' };
    }

    if (pagesWithIG.length === 1) {
      const page = pagesWithIG[0];
      return {
        success: true, requiresSelection: false,
        pageAccessToken: page.access_token, pageId: page.id,
        pageName: page.name, igBusinessAccountId: page.instagram_business_account.id,
        tokenType: 'page'
      };
    }

    return {
      success: true, requiresSelection: true,
      pages: pagesWithIG.map(page => ({
        pageId: page.id, pageName: page.name,
        pageAccessToken: page.access_token,
        igBusinessAccountId: page.instagram_business_account.id
      }))
    };
  } catch (error) {
    if (error.response) {
      const apiError = error.response.data?.error;
      if (apiError?.code === 190) return { success: false, error: 'Invalid or expired user access token.' };
      if (apiError?.code === 100) return { success: false, error: 'Invalid API request. Check permissions.' };
      if (apiError?.message) return { success: false, error: `Facebook API Error: ${apiError.message}` };
    }
    if (error.code === 'ECONNREFUSED' || error.code === 'ETIMEDOUT') {
      return { success: false, error: 'Unable to connect to Facebook Graph API.' };
    }
    return { success: false, error: error.message || 'Page token exchange failed' };
  }
}

/**
 * Encrypt and persist a page access token. Creates/updates the business account row first.
 * Busts the credential cache on success.
 * @param {{ userId, igBusinessAccountId, pageAccessToken, pageId, pageName, scope? }} params
 * @returns {Promise<{success, businessAccountId?, expiresAt?, error?}>}
 */
async function storePageToken({ userId, igBusinessAccountId, pageAccessToken, pageId, pageName, scope }) {
  try {
    console.log('💾 Storing page token in database...');
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
        const crypto = require('crypto');
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
        updated_at: new Date().toISOString()
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
        last_refreshed_at: new Date().toISOString(), updated_at: new Date().toISOString()
      }, { onConflict: 'user_id,business_account_id,token_type' })
      .select();

    if (credentialError) return { success: false, error: `Failed to store credentials: ${credentialError.message}` };

    // Audit log — non-blocking
    try {
      await logAudit('token_stored', userId, {
        action: 'store_page_token', business_account_id: businessAccount.id,
        page_id: pageId, scope: finalScope, credential_id: credentialData?.[0]?.id, success: true
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

/**
 * Retrieve and decrypt a page access token.
 * @param {string} userId
 * @param {string} businessAccountId - UUID
 * @returns {Promise<string>} Decrypted token
 * @throws {Error} If not found or decryption fails
 */
async function retrievePageToken(userId, businessAccountId) {
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

module.exports = { exchangeForPageToken, storePageToken, retrievePageToken };
