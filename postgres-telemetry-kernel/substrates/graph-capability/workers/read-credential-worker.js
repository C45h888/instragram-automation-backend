// postgres-telemetry-kernel/substrates/graph-capability/workers/read-credential-worker.js
// Credential reader: governed SELECTs on instagram_credentials.
//
// Owns: query routing + domain logic. All Supabase I/O delegated to bedrock.
// Does NOT own: decryption (vault concern), signal dispatch, cache invalidation.
//
// Dispatched via: CK.governedRead('db.credential', params)

const bedrock = require('../../../bedrock');

async function execute(params, governance) {
  // ── Batch scan: active page credentials ──────────────────────────────────
  if (params.query === 'scanActivePageCredentials') {
    return bedrock.token.getActivePageCredentials({
      businessAccountId: params.businessAccountId,
      limit: params.limit || 200,
    });
  }

  // ── Batch scan: expiring UATs ────────────────────────────────────────────
  if (params.query === 'scanExpiringUATs') {
    return bedrock.token.getExpiringUATs(params.windowDays || 14, {
      businessAccountId: params.businessAccountId,
      limit: params.limit || 200,
    });
  }

  // ── Batch scan: data_access_expires_at within window ──────────────────────
  if (params.query === 'scanDataAccessExpiry') {
    return bedrock.token.getExpiringUATs(params.windowDays || 30, {
      businessAccountId: params.businessAccountId,
      limit: params.limit || 200,
    });
  }

  // ── getCredentialPageId — page_id only ─────────────────────────────────
  if (params.query === 'getCredentialPageId') {
    if (!params.businessAccountId) {
      return { success: false, data: null, error: 'businessAccountId required' };
    }
    return bedrock.token.getCredentialPageId(params.businessAccountId);
  }

  // ── Single-cred lookup (default) ─────────────────────────────────────────
  const { userId, businessAccountId, tokenType = 'page' } = params;
  if (!userId || !businessAccountId) {
    return { success: false, data: null, error: 'userId and businessAccountId required' };
  }

  return bedrock.token.getCredentialHealth(businessAccountId);
}

module.exports = { execute };
