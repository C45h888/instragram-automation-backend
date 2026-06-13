// postgres-telemetry-kernel/writers/ugc-writer.js
// UGC writer: ugc_content batch upsert.
//
// Owns: operation-to-domain routing. All Supabase I/O delegated to bedrock.
// Does NOT own: governance, normalization, fetch, orchestration,
//               failure classification (persistence-failure-substrate),
//               retry policy (retry-cadence-kernel).
//
// Bedrock handles: client check, upsert, retry, idempotency,
//                  composite onConflict (business_account_id,visitor_post_id),
//                  governance dispatch.

const bedrock = require('../bedrock');

async function execute(params, governance) {
  const { domain, accountId, intentId, rows } = params;

  await bedrock.ugc.persistUgcContent(rows, {
    accountId, intentId, governance, domain,
  });
}

module.exports = { execute };
