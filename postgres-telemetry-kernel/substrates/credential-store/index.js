// postgres-telemetry-kernel/substrates/credential-store/index.js
// Credential Store Substrate: orchestrates bounded workers for credential persistence.
//
// Owns: composing workers, data routing between them, audit logging,
//       cache invalidation, DB_WRITE_COMPLETE emission.
// Does NOT own: individual Supabase operations (workers own those),
//               Graph API calls, token exchange, signal dispatch.
//
// Workers (each one bounded I/O call):
//   key-provision-worker      — SELECT + INSERT vault (get or create encryption key)
//   encrypt-token-worker      — RPC encrypt_instagram_token
//   business-account-upsert-worker — UPSERT instagram_business_accounts
//   credential-upsert-worker       — UPSERT instagram_credentials
//
// Contract: execute(params, governance) — async, emits DB_WRITE_COMPLETE.
// Called via: CK → persist-telemetry FSM → dispatchWrite(upsert_credential, ...)

const { logAudit } = require('../../../config/supabase');
const { clearCredentialCache } = require('../../../helpers/credential-cache');

const keyProvisionWorker = require('./workers/key-provision-worker');
const encryptTokenWorker = require('./workers/encrypt-token-worker');
const businessAccountUpsertWorker = require('./workers/business-account-upsert-worker');
const credentialUpsertWorker = require('./workers/credential-upsert-worker');

const PAT_SCOPE_DEFAULTS = [
  'instagram_basic', 'instagram_manage_comments', 'instagram_manage_insights',
  'instagram_content_publish', 'instagram_manage_messages',
  'pages_show_list', 'pages_read_engagement', 'pages_manage_metadata',
  'pages_read_user_content', 'pages_manage_posts', 'pages_manage_engagement',
];

/**
 * @param {object} params — FSM passes { domain, accountId, intentId, table, rows }
 *   rows[0]: { operation, userId, igBusinessAccountId, businessAccountId,
 *              pageAccessToken, userAccessToken, pageId, pageName, scope,
 *              expiresAt, dataAccessExpiresAt, tokenType, signalCb }
 * @param {object} governance — CK reference
 */
async function execute(params, governance) {
  const { domain, accountId, intentId, table } = params;
  const row = (params.rows && params.rows[0]) || {};
  const {
    operation, userId, igBusinessAccountId, businessAccountId: baId,
    pageAccessToken, userAccessToken, pageId, pageName, scope, expiresAt,
    dataAccessExpiresAt, tokenType, signalCb,
  } = row;

  const token = pageAccessToken || userAccessToken;

  if (!token) {
    governance?.dispatch({
      type: 'DB_WRITE_COMPLETE', domain, accountId, intentId,
      table: 'instagram_credentials', count: 0, status: 'failed', error: 'token required',
    });
    return;
  }

  // ── Step 1: Key provision ───────────────────────────────────────────────
  const keyResult = await keyProvisionWorker.execute({
    userId,
    igBusinessAccountId,
    businessAccountId: baId,
    operation,
  });
  if (!keyResult.success) {
    governance?.dispatch({
      type: 'DB_WRITE_COMPLETE', domain, accountId, intentId,
      table: 'instagram_credentials', count: 0, status: 'failed', error: keyResult.error,
    });
    return;
  }
  const encryptionKeyId = keyResult.encryptionKeyId;

  // ── Step 2: Encrypt token ───────────────────────────────────────────────
  const encryptResult = await encryptTokenWorker.execute({ token, encryptionKeyId });
  if (!encryptResult.success) {
    governance?.dispatch({
      type: 'DB_WRITE_COMPLETE', domain, accountId, intentId,
      table: 'instagram_credentials', count: 0, status: 'failed', error: encryptResult.error,
    });
    return;
  }

  // ── Step 3: Business account upsert (PAT only) ──────────────────────────
  let resolvedBaId = baId;
  const finalScope = scope || PAT_SCOPE_DEFAULTS;

  if (operation === 'store_pat') {
    const baResult = await businessAccountUpsertWorker.execute({
      userId,
      igBusinessAccountId,
      pageName,
      encryptionKeyId,
      scope: finalScope,
      operation: 'store_pat',
    });
    if (!baResult.success) {
      governance?.dispatch({
        type: 'DB_WRITE_COMPLETE', domain, accountId, intentId,
        table: 'instagram_business_accounts', count: 0, status: 'failed', error: baResult.error,
      });
      return;
    }
    resolvedBaId = baResult.businessAccountId;
  }

  // ── Step 4: Credential upsert ───────────────────────────────────────────
  const credResult = await credentialUpsertWorker.execute({
    userId,
    businessAccountId: resolvedBaId,
    encryptedToken: encryptResult.encryptedToken,
    tokenType,
    scope: finalScope,
    expiresAt,
    dataAccessExpiresAt,
    pageId,
  });
  if (!credResult.success) {
    governance?.dispatch({
      type: 'DB_WRITE_COMPLETE', domain, accountId, intentId,
      table: 'instagram_credentials', count: 0, status: 'failed', error: credResult.error,
    });
    return;
  }

  // ── Side effects: audit + cache + signal ────────────────────────────────
  logAudit('token_stored', userId, {
    action: `store_${tokenType}_token`,
    business_account_id: resolvedBaId,
    page_id: pageId,
    scope: finalScope,
    success: true,
  }).catch(() => {});

  clearCredentialCache(resolvedBaId);

  if (typeof signalCb === 'function') {
    signalCb(resolvedBaId);
  }

  governance?.dispatch({
    type: 'DB_WRITE_COMPLETE',
    domain, accountId, intentId,
    table: 'instagram_credentials',
    count: 1,
    status: 'success',
    error: null,
    businessAccountId: resolvedBaId,
  });

  console.log(`✅ ${tokenType} token stored via credential-store substrate`);
}

module.exports = { execute };
