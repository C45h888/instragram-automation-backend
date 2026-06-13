// postgres-telemetry-kernel/writers/messages-writer.js
// Messages writer: instagram_dm_messages batch upsert (new messages + read receipts).
//
// Owns: operation-to-domain routing. All Supabase I/O delegated to bedrock.
// Does NOT own: governance, normalization, fetch, orchestration,
//               orphan repair (Phase 3C: orphan-message-repair.js),
//               failure classification (persistence-failure-substrate),
//               retry policy (retry-cadence-kernel).
//
// Operation dispatch:
//   batch_upsert_messages → bedrock.ugc.persistMessageEvent (ignoreDuplicates: true)
//   batch_upsert_dm_seen  → bedrock.ugc.markMessagesSeen (batch UPDATE)
//
// Bedrock handles: client check, upsert/update, retry, governance dispatch.

const bedrock = require('../bedrock');

async function execute(params, governance) {
  const { domain, accountId, intentId, rows, operation } = params;

  if (operation === 'batch_upsert_dm_seen') {
    // Seen receipts: update existing message rows, do not insert new ones.
    // Each row should have instagram_message_id + seen_at + reader_id.
    await bedrock.ugc.markMessagesSeen(
      rows.map(r => ({
        messageId: r.messageId || r.instagram_message_id,
        seenAt: r.seenAt || new Date().toISOString(),
        senderId: r.senderId || null,
      })),
      { accountId, intentId, governance, domain }
    );
  } else {
    // batch_upsert_messages: upsert, skip if exists (don't overwrite)
    await bedrock.ugc.persistMessageEvent(rows, {
      accountId, intentId, governance, domain,
      ignoreDuplicates: true,
    });
  }
}

module.exports = { execute };
