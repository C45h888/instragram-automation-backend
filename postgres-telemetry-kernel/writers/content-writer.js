// postgres-telemetry-kernel/writers/content-writer.js
// Content writer: instagram_media batch upsert (posts + insights).
//
// Owns: Supabase upsert for instagram_media table.
// Does NOT own: governance, normalization, fetch, orchestration,
//               failure classification (persistence-failure-substrate),
//               retry policy (retry-cadence-kernel).

const { getSupabaseAdmin } = require('../../config/supabase');
const { analyzeFailure } = require('../substrates/persistence-failure-substrate');

async function execute(params, governance) {
  const { domain, accountId, intentId, table, rows } = params;
  const supabase = getSupabaseAdmin();
  if (!supabase) {
    const analysis = analyzeFailure({ message: 'supabase_unavailable' }, 'write', 'supabase', { attemptN: 1, lineageId: intentId, workerName: 'content-writer', primaryKeyField: 'instagram_media_id', primaryKeyValue: rows?.[0]?.instagram_media_id });
    governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId, table, count: 0, rows, analysis, errorShape: { category: analysis.category, subtype: analysis.subtype, retryable: analysis.retryable, retryAfterMs: analysis.rateLimit.retryAfterMs }, error: 'supabase_unavailable' });
    return;
  }

  try {
    const { error } = await supabase
      .from(table)
      .upsert(rows, { onConflict: 'instagram_media_id', ignoreDuplicates: false });

    if (error) throw error;

    governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId, table, count: rows.length, error: null });
  } catch (err) {
    const analysis = analyzeFailure(err, 'write', 'supabase', { attemptN: 1, lineageId: intentId, workerName: 'content-writer', primaryKeyField: 'instagram_media_id', primaryKeyValue: rows?.[0]?.instagram_media_id });
    governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId, table, count: 0, rows, analysis, errorShape: { category: analysis.category, subtype: analysis.subtype, retryable: analysis.retryable, retryAfterMs: analysis.rateLimit.retryAfterMs }, error: err.message });
  }
}

module.exports = { execute };
