// postgres-telemetry-kernel/writers/message-fix-writer.js
// Message Fix Writer: repair instagram_dm_messages conversation_id references.
//
// Owns: UPDATE instagram_dm_messages SET conversation_id = <uuid>
//        WHERE conversation_id = <threadId> AND business_account_id = <accountId>.
// Does NOT own: governance, normalization, fetch, orchestration,
//               failure classification (persistence-failure-substrate),
//               retry policy (retry-cadence-kernel).
//
// Phase 5: fixes orphaned message conversation_ids after conversation repair.

const { getSupabaseAdmin } = require('../../config/supabase');
const { analyzeFailure } = require('../substrates/persistence-failure-substrate');

async function execute(params, governance) {
  const { domain, accountId, intentId, table, rows } = params;
  const supabase = getSupabaseAdmin();
  if (!supabase) {
    const analysis = analyzeFailure({ message: 'supabase_unavailable' }, 'write', 'supabase', { attemptN: 1, lineageId: intentId, workerName: 'message-fix-writer', primaryKeyField: 'match_conversation_id', primaryKeyValue: rows?.[0]?.match_conversation_id });
    governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId, table, count: 0, rows, analysis, errorShape: { category: analysis.category, subtype: analysis.subtype, retryable: analysis.retryable, retryAfterMs: analysis.rateLimit.retryAfterMs }, error: 'supabase_unavailable' });
    return;
  }

  try {
    let totalFixed = 0;
    for (const row of rows) {
      const { conversation_id: newId, match_conversation_id: matchId, business_account_id: bizId } = row;
      if (!newId || !matchId) continue;

      const { data: fixed } = await supabase
        .from(table)
        .update({ conversation_id: newId, business_account_id: bizId })
        .eq('conversation_id', matchId)
        .eq('business_account_id', bizId)
        .select('instagram_message_id')
        .limit(1000);

      totalFixed += (fixed || []).length;
    }

    governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId, table, count: totalFixed, error: null });
  } catch (err) {
    const analysis = analyzeFailure(err, 'write', 'supabase', { attemptN: 1, lineageId: intentId, workerName: 'message-fix-writer', primaryKeyField: 'match_conversation_id', primaryKeyValue: rows?.[0]?.match_conversation_id });
    governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId, table, count: 0, rows, analysis, errorShape: { category: analysis.category, subtype: analysis.subtype, retryable: analysis.retryable, retryAfterMs: analysis.rateLimit.retryAfterMs }, error: err.message });
  }
}

module.exports = { execute };
