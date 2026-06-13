// postgres-telemetry-kernel/writers/api-usage-writer.js
// API Usage Writer: governed write for the api_usage table.
//
// Owns: param extraction. All Supabase I/O delegated to bedrock.
// Does NOT own: governance policy (FSM), rate-limit decisions.
//
// Bedrock handles: client check, upsert, retry, governance dispatch.

const bedrock = require('../bedrock');

async function execute(params, governance) {
  const { domain, accountId, table } = params;
  const row = (params.rows && params.rows[0]) || {};
  const { userId, businessAccountId, endpoint, method, hourBucket, statusCode, success } = row;

  await bedrock.token.persistApiUsage({
    user_id: userId,
    business_account_id: businessAccountId || null,
    endpoint: endpoint || null,
    method: method || null,
    hour_bucket: hourBucket,
    request_count: 1,
    status_code: statusCode || null,
    success: typeof success === 'boolean' ? success : true,
    updated_at: new Date().toISOString(),
  }, {
    accountId, intentId: `${userId}-${hourBucket}`, governance, domain,
  });
}

module.exports = { execute };
