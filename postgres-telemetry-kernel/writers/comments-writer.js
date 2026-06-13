// postgres-telemetry-kernel/writers/comments-writer.js
// Comments writer: instagram_comments batch upsert (parent comments + replies).
//
// Owns: operation-to-domain routing. All Supabase I/O delegated to bedrock.
// Does NOT own: governance, normalization, fetch, orchestration,
//               failure classification (persistence-failure-substrate),
//               retry policy (retry-cadence-kernel).
//
// Operation dispatch:
//   batch_upsert_comments       → bedrock.ugc.persistCommentEvent
//   batch_upsert_comment_replies→ bedrock.ugc.persistCommentEvent
//
// Bedrock handles: client check, upsert, retry, idempotency, governance dispatch.

const bedrock = require('../bedrock');

async function execute(params, governance) {
  const { domain, accountId, intentId, rows, operation } = params;

  // Both operations use the same table and PK — route to bedrock.ugc
  await bedrock.ugc.persistCommentEvent(rows, {
    accountId, intentId, governance, domain,
  });
}

module.exports = { execute };
