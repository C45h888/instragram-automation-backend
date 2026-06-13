// postgres-telemetry-kernel/writers/conversations-writer.js
// Conversations writer: instagram_dm_conversations batch upsert.
//
// Owns: operation-to-domain routing. All Supabase I/O delegated to bedrock.
// Does NOT own: governance, normalization, fetch, orchestration,
//               customer_user_id resolution (Phase 3A: hydrator),
//               failure classification (persistence-failure-substrate),
//               retry policy (retry-cadence-kernel).
//
// Bedrock handles: client check, upsert, retry, idempotency, governance dispatch.

const bedrock = require('../bedrock');

async function execute(params, governance) {
  const { domain, accountId, intentId, rows } = params;

  await bedrock.ugc.persistConversation(rows, {
    accountId, intentId, governance, domain,
  });
}

module.exports = { execute };
