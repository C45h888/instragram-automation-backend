// postgres-telemetry-kernel/writers/api-usage-writer.js
// API Usage Writer: governed write for the api_usage table.
//
// Owns: log_api_request — UPSERT api_usage row for rate-limit tracking.
// Does NOT own: governance policy (FSM), rate-limit decisions,
//               failure classification (persistence-failure-substrate),
//               retry policy (retry-cadence-kernel).
//
// Contract: execute(params, governance) — async, emits DB_WRITE_COMPLETE on
// success or DB_WRITE_FAILED on failure (with errorShape).
// Called via: CK → persist-telemetry FSM → dispatchWrite(log_api_request, ...)

const { getSupabaseAdmin } = require('../../config/supabase');
const { analyzeFailure } = require('../substrates/persistence-failure-substrate');

/**
 * @param {object} params — { domain, accountId, table, rows }
 *   rows[0]: { userId, businessAccountId, endpoint, method, hourBucket, statusCode, success }
 * @param {object} governance — CK reference
 */
async function execute(params, governance) {
  const { domain, accountId, table } = params;
  const row = (params.rows && params.rows[0]) || {};
  const { userId, businessAccountId, endpoint, method, hourBucket, statusCode, success } = row;

  const pkValue = `${userId || '*'}|${hourBucket || '*'}|${endpoint || '*'}`;

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    const analysis = analyzeFailure({ message: 'supabase_unavailable' }, 'write', 'supabase', { attemptN: 1, lineageId: `${userId}-${hourBucket}`, workerName: 'api-usage-writer', primaryKeyField: 'user_id,endpoint,hour_bucket', primaryKeyValue: pkValue });
    governance?.dispatch({
      type: 'DB_WRITE_FAILED',
      domain, accountId, table,
      count: 0, rows, analysis, errorShape: { category: analysis.category, subtype: analysis.subtype, retryable: analysis.retryable, retryAfterMs: analysis.rateLimit.retryAfterMs }, error: 'supabase_unavailable',
    });
    return;
  }

  try {
    const { error } = await supabase
      .from('api_usage')
      .upsert({
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
        onConflict: 'user_id,business_account_id,endpoint,hour_bucket',
        ignoreDuplicates: false,
      });

    if (error) {
      const analysis = analyzeFailure(error, 'write', 'supabase', { attemptN: 1, lineageId: `${userId}-${hourBucket}`, workerName: 'api-usage-writer', primaryKeyField: 'user_id,endpoint,hour_bucket', primaryKeyValue: pkValue });
      governance?.dispatch({
        type: 'DB_WRITE_FAILED',
        domain, accountId, table,
        count: 0, rows, analysis, errorShape: { category: analysis.category, subtype: analysis.subtype, retryable: analysis.retryable, retryAfterMs: analysis.rateLimit.retryAfterMs }, error: error.message,
      });
      return;
    }

    governance?.dispatch({
      type: 'DB_WRITE_COMPLETE',
      domain, accountId, table,
      count: 1, status: 'success', error: null,
    });
  } catch (err) {
    const analysis = analyzeFailure(err, 'write', 'supabase', { attemptN: 1, lineageId: `${userId}-${hourBucket}`, workerName: 'api-usage-writer', primaryKeyField: 'user_id,endpoint,hour_bucket', primaryKeyValue: pkValue });
    governance?.dispatch({
      type: 'DB_WRITE_FAILED',
      domain, accountId, table,
      count: 0, rows, analysis, errorShape: { category: analysis.category, subtype: analysis.subtype, retryable: analysis.retryable, retryAfterMs: analysis.rateLimit.retryAfterMs }, error: err.message,
    });
  }
}

module.exports = { execute };
