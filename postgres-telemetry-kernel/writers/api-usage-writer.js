// postgres-telemetry-kernel/writers/api-usage-writer.js
// API Usage Writer: governed write for the api_usage table.
//
// Owns: log_api_request — UPSERT api_usage row for rate-limit tracking.
// Does NOT own: governance policy (FSM), rate-limit decisions.
//
// Contract: execute(params, governance) — async, emits DB_WRITE_COMPLETE.
// Called via: CK → persist-telemetry FSM → dispatchWrite(log_api_request, ...)

const { getSupabaseAdmin } = require('../../config/supabase');

/**
 * @param {object} params — { domain, accountId, table, rows }
 *   rows[0]: { userId, businessAccountId, endpoint, method, hourBucket, statusCode, success }
 * @param {object} governance — CK reference
 */
async function execute(params, governance) {
  const { domain, accountId, table } = params;
  const row = (params.rows && params.rows[0]) || {};
  const { userId, businessAccountId, endpoint, method, hourBucket, statusCode, success } = row;

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    governance?.dispatch({
      type: 'DB_WRITE_COMPLETE',
      domain, accountId, table,
      count: 0, status: 'failed', error: 'supabase_unavailable',
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
      governance?.dispatch({
        type: 'DB_WRITE_COMPLETE',
        domain, accountId, table,
        count: 0, status: 'failed', error: error.message,
      });
      return;
    }

    governance?.dispatch({
      type: 'DB_WRITE_COMPLETE',
      domain, accountId, table,
      count: 1, status: 'success', error: null,
    });
  } catch (err) {
    governance?.dispatch({
      type: 'DB_WRITE_COMPLETE',
      domain, accountId, table,
      count: 0, status: 'failed', error: err.message,
    });
  }
}

module.exports = { execute };
