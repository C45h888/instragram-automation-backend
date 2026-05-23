// backend.api/services/tokens/uat.js
// User Access Token lifecycle: store → retrieve → refresh.
// UATs expire (~60 days) — unlike PATs which are non-expiring.

const { axios, GRAPH_API_BASE } = require('./base');
const { detectTokenType } = require('./detection');
const { getSupabaseAdmin } = require('../../config/supabase');
const { clearCredentialCache } = require('../../helpers/credential-cache');

/**
 * Store a validated UAT as token_type='user'.
 * @param {{ userId, businessAccountId, userAccessToken, scope, expiresAt, dataAccessExpiresAt }} params
 */
async function storeUserToken({ userId, businessAccountId, userAccessToken, scope, expiresAt, dataAccessExpiresAt }) {
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
        updated_at: new Date().toISOString()
      }, { onConflict: 'user_id,business_account_id,token_type' })
      .select();

    if (credError) return { success: false, error: credError.message };

    console.log('✅ UAT stored in vault (token_type=user)');
    return { success: true };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Retrieve and decrypt a UAT. Throws if expired — UATs DO expire, unlike PATs.
 * @returns {Promise<{token, expiresAt, dataAccessExpiresAt, scope, issuedAt}>}
 */
async function retrieveUserToken(userId, businessAccountId) {
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

  return { token: decryptedToken, expiresAt: data.expires_at, dataAccessExpiresAt: data.data_access_expires_at, scope: data.scope, issuedAt: data.issued_at };
}

/**
 * Extend a UAT via fb_exchange_token, validate, store, bust cache.
 * Cannot refresh an expired UAT — user must re-login via OAuth.
 * @returns {Promise<{success, expiresAt, scopes}>}
 */
async function refreshUserToken(userId, businessAccountId) {
  const current = await retrieveUserToken(userId, businessAccountId);

  const extendRes = await axios.get(`${GRAPH_API_BASE}/oauth/access_token`, {
    params: {
      grant_type: 'fb_exchange_token',
      client_id: process.env.INSTAGRAM_APP_ID,
      client_secret: process.env.INSTAGRAM_APP_SECRET,
      fb_exchange_token: current.token
    },
    timeout: 10000
  });

  const newToken = extendRes.data.access_token;
  const expiresIn = extendRes.data.expires_in;

  const tokenInfo = await detectTokenType(newToken);
  if (!tokenInfo || !tokenInfo.isValid) throw new Error('Refreshed UAT failed /debug_token validation');

  const newExpiresAt = expiresIn ? new Date(Date.now() + expiresIn * 1000).toISOString() : null;
  const dataAccessExpiresAt = tokenInfo.dataAccessExpiresAt ? new Date(tokenInfo.dataAccessExpiresAt * 1000).toISOString() : null;

  const storeResult = await storeUserToken({ userId, businessAccountId, userAccessToken: newToken, scope: tokenInfo.scopes, expiresAt: newExpiresAt, dataAccessExpiresAt });
  if (!storeResult.success) throw new Error(`Failed to store refreshed UAT: ${storeResult.error}`);

  clearCredentialCache(businessAccountId);

  return { success: true, expiresAt: newExpiresAt, scopes: tokenInfo.scopes };
}

module.exports = { storeUserToken, retrieveUserToken, refreshUserToken };
