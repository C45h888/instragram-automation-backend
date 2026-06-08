// postgres-telemetry-kernel/writers/credential-store-writer.js
// Credential store writer: handles PAT and UAT credential persistence.
// Migrated from graph-capability-kernel/substrates/vault/*/workers/store-worker.js
//
// Owns: vault key provisioning, encryption RPC, business account upsert,
//       credential upsert, audit logging, cache invalidation.
// Does NOT own: Graph API calls, token exchange, signal dispatch.
//
// Contract: execute(params, governance) — async, emits DB_WRITE_COMPLETE.
// Called via: CK → persist-telemetry FSM → dispatchWrite(upsert_credential, ...)

const crypto = require('crypto');
const { getSupabaseAdmin, logAudit } = require('../../config/supabase');
const { clearCredentialCache } = require('../../helpers/credential-cache');

const PAT_SCOPE_DEFAULTS = [
  'instagram_basic', 'instagram_manage_comments', 'instagram_manage_insights',
  'instagram_content_publish', 'instagram_manage_messages',
  'pages_show_list', 'pages_read_engagement', 'pages_manage_metadata',
  'pages_read_user_content', 'pages_manage_posts', 'pages_manage_engagement',
];

/**
 * @param {{
 *   domain: string,
 *   accountId: string,
 *   intentId?: string,
 *   operation: 'store_pat' | 'store_uat',
 *   userId: string,
 *   igBusinessAccountId?: string,
 *   businessAccountId?: string,
 *   pageAccessToken?: string,
 *   userAccessToken?: string,
 *   pageId?: string,
 *   pageName?: string,
 *   scope?: string[],
 *   expiresAt?: string|null,
 *   dataAccessExpiresAt?: string|null,
 *   tokenType: 'page' | 'user',
 *   signalCb?: Function  // called on success with { businessAccountId }
 * }} params
 * @param {object} governance — CK reference
 */
async function execute(params, governance) {
  // FSM passes { domain, accountId, intentId, table, rows }.
  // Credential-specific fields are in rows[0].
  const { domain, accountId, intentId, rows } = params;
  const row = (rows && rows[0]) || {};
  const { operation, userId, igBusinessAccountId, businessAccountId: baId,
          pageAccessToken, userAccessToken, pageId, pageName, scope, expiresAt,
          dataAccessExpiresAt, tokenType, signalCb } = row;
  const supabase = getSupabaseAdmin();
  const token = pageAccessToken || userAccessToken;

  if (!supabase) {
    governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId,
      table: 'instagram_credentials', count: 0, status: 'failed', error: 'supabase_unavailable' });
    return;
  }

  try {
    // ── 1. Key provisioning (PAT only — UAT reuses existing key) ──────────
    let encryptionKeyId = null;
    if (operation === 'store_pat') {
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
            .insert({ name: `instagram_token_key_${userId}`, secret: userKey,
                      description: `Per-user Instagram token encryption key for user ${userId}` })
            .select('id').single();
          if (!vaultError) encryptionKeyId = vaultSecret.id;
        }
      } catch (keyErr) {
        console.warn('⚠️ Key provisioning error, using shared key:', keyErr.message);
      }
    } else {
      // UAT: lookup existing key
      try {
        const { data: bizAccount } = await supabase
          .from('instagram_business_accounts')
          .select('encryption_key_id').eq('id', baId || accountId).maybeSingle();
        encryptionKeyId = bizAccount?.encryption_key_id || null;
      } catch (keyErr) {
        console.warn('⚠️ UAT encryption key lookup failed:', keyErr.message);
      }
    }

    // ── 2. Encryption RPC ────────────────────────────────────────────────
    const { data: encryptedToken, error: encryptError } = await supabase
      .rpc('encrypt_instagram_token', { token, p_key_id: encryptionKeyId });
    if (encryptError || !encryptedToken) {
      governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId,
        table: 'instagram_credentials', count: 0, status: 'failed',
        error: encryptError?.message || 'Encryption returned null' });
      return;
    }

    // ── 3. Business account upsert (PAT only) ─────────────────────────────
    let resolvedBaId = baId;
    if (operation === 'store_pat') {
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
      if (accountError) {
        governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId,
          table: 'instagram_business_accounts', count: 0, status: 'failed',
          error: accountError.message });
        return;
      }
      resolvedBaId = businessAccount.id;
    }

    // ── 4. Credential upsert ─────────────────────────────────────────────
    const credentialRow = {
      user_id: userId,
      business_account_id: resolvedBaId,
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

    const { error: credError } = await supabase
      .from('instagram_credentials')
      .upsert(credentialRow, {
        onConflict: 'user_id,business_account_id,token_type',
        ignoreDuplicates: false,
      });
    if (credError) {
      governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId,
        table: 'instagram_credentials', count: 0, status: 'failed',
        error: credError.message });
      return;
    }

    // ── 5. Audit + cache ──────────────────────────────────────────────────
    logAudit('token_stored', userId, {
      action: `store_${tokenType}_token`,
      business_account_id: resolvedBaId,
      page_id: pageId, scope, success: true,
    }).catch(() => {});
    clearCredentialCache(resolvedBaId);

    // ── 6. Signal callback ───────────────────────────────────────────────
    if (typeof signalCb === 'function') {
      signalCb(resolvedBaId);
    }

    governance?.dispatch({
      type: 'DB_WRITE_COMPLETE', domain, accountId, intentId,
      table: 'instagram_credentials', count: 1, status: 'success', error: null,
      businessAccountId: resolvedBaId,
    });
    console.log(`✅ ${tokenType} token stored via governance (credential-store-writer)`);

  } catch (err) {
    governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId,
      table: 'instagram_credentials', count: 0, status: 'failed', error: err.message });
  }
}

module.exports = { execute };
