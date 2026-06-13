// postgres-telemetry-kernel/writers/content-writer.js
// Content writer: instagram_media batch upsert (posts + insights).
//
// Owns: operation-to-domain routing. All Supabase I/O delegated to bedrock.
// Does NOT own: governance, normalization, fetch, orchestration,
//               failure classification (persistence-failure-substrate),
//               retry policy (retry-cadence-kernel).
//
// Operation dispatch:
//   batch_upsert_posts       → bedrock.publishing.persistMediaStub
//   batch_upsert_insights    → bedrock.insights.persistMediaStub
//   batch_upsert_media_stubs → bedrock.publishing.persistMediaStub
//
// Bedrock handles: client check, upsert, retry, idempotency, governance dispatch.

const bedrock = require('../bedrock');

async function execute(params, governance) {
  const { domain, accountId, intentId, rows, operation } = params;

  // All operations write to instagram_media with instagram_media_id PK
  await bedrock.publishing.persistMediaStub(rows, {
    accountId, intentId, governance, domain,
  });
}

module.exports = { execute };
