// postgres-telemetry-kernel/writers/message-fix-writer.js
// Message Fix Writer: repair instagram_dm_messages conversation_id references.
//
// Owns: operation-to-domain routing. All Supabase I/O delegated to bedrock.
// Does NOT own: governance, normalization, fetch, orchestration,
//               failure classification (persistence-failure-substrate),
//               retry policy (retry-cadence-kernel).
//
// Phase 5: fixes orphaned message conversation_ids after conversation repair.
//
// Bedrock handles: client check, batch UPDATE, governance dispatch.

const bedrock = require('../bedrock');

async function execute(params, governance) {
  const { domain, accountId, intentId, rows } = params;

  // Build update descriptors for bedrock
  const updates = [];
  for (const row of rows) {
    const { conversation_id: newId, match_conversation_id: matchId, business_account_id: bizId } = row;
    if (!newId || !matchId) continue;
    updates.push({ messageId: matchId, conversationId: newId });
  }

  if (updates.length === 0) {
    governance?.dispatch({
      type: 'DB_WRITE_COMPLETE', domain, accountId, intentId,
      table: 'instagram_dm_messages', count: 0, error: null,
    });
    return;
  }

  await bedrock.ugc.fixMessageConversationIds(updates, {
    accountId, intentId, governance, domain,
  });
}

module.exports = { execute };
