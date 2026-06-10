// postgres-telemetry-kernel/writers/content-writer.js
// Content writer: instagram_media batch upsert (posts + insights).
//
// Owns: Supabase upsert for instagram_media table.
// Does NOT own: governance, normalization, fetch, orchestration,
//               failure classification (persistence-failure-substrate),
//               retry policy (retry-cadence-kernel).

const { getSupabaseAdmin } = require('../../config/supabase');

async function execute(params, governance) {
  const { domain, accountId, intentId, table, rows } = params;
  const supabase = getSupabaseAdmin();
  if (!supabase) {
    governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId, table, count: 0, rows: rows || [], error: 'supabase_unavailable', rawError: { message: 'supabase_unavailable' }, workerName: 'content-writer', lineageId: intentId, primaryKeyField: 'instagram_media_id', primaryKeyValue: rows?.[0]?.instagram_media_id, attemptN: 1, operation: 'write', source: 'supabase' });
    return;
  }

  try {
    const { error } = await supabase
      .from(table)
      .upsert(rows, { onConflict: 'instagram_media_id', ignoreDuplicates: false });

    if (error) throw error;

    governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId, table, count: rows.length, error: null });
  } catch (err) {
    governance?.dispatch({ type: 'DB_WRITE_FAILED', domain, accountId, intentId, table, count: 0, rows: rows || [], error: err.message, rawError: err, workerName: 'content-writer', lineageId: intentId, primaryKeyField: 'instagram_media_id', primaryKeyValue: rows?.[0]?.instagram_media_id, attemptN: 1, operation: 'write', source: 'supabase' });
  }
}

module.exports = { execute };
