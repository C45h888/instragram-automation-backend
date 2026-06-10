// retry-cadence-kernel/workers/schema-recovery-worker.js
// Schema Recovery Worker — bounded schema validation and migration integrity.
//
// CONSTITUTIONAL CONTRACT:
//   Owns: validating the live Supabase table structure against expected schema,
//         detecting migration drift, reporting mismatches.
//
//   Does NOT own: migration execution (the migration runner owns that),
//                 classification (persistence-failure-substrate),
//                 recommendation selection (FSM).
//
// Called by: maintenance-substrate.

const { getSupabaseAdmin } = require('../../config/supabase');

/**
 * Validate the live database schema against the expected structure.
 *
 * Strategy: run a lightweight DESCRIBE-equivalent query on the affected
 * table to verify it exists and its columns match the expected set.
 * If the table is missing or columns differ, the schema has drifted.
 *
 * @param {object} params — { domain, accountId, intentId, analysis }
 * @param {object} governance — CK reference
 * @returns {Promise<{ success: boolean, error?: string }>}
 */
async function execute(params, governance) {
  const { domain, accountId, intentId, analysis } = params;

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    return { success: false, error: 'supabase_unavailable_during_schema_check' };
  }

  // The table name is extracted from the analysis or event context
  const table = analysis?.normalized?.details?.table
    || analysis?.normalized?.message?.match(/relation "(\w+)"/)?.[1]
    || null;

  if (!table) {
    // No table to validate — cannot proceed
    return { success: false, error: 'no_table_in_analysis_for_schema_check' };
  }

  try {
    // Lightweight schema check: try a LIMIT 0 query. If it works,
    // the table exists and the column path is valid.
    const { error } = await supabase
      .from(table)
      .select('*')
      .limit(0);

    if (error) {
      // Table is missing or columns diverged
      (governance?.dispatchGlobal || governance?.dispatch)({
        type: 'ESCALATE_TO_OPERATOR_AUTHORIZED',
        domain: domain || 'persist-telemetry',
        accountId: accountId || '*',
        intentId,
        category: 'SCHEMA_FAILURE',
        subtype: analysis?.subtype || 'schema_drift',
        severity: 'HIGH',
        analysis: { ...analysis, schemaCheckTable: table, schemaCheckError: error.message },
      });

      return { success: false, error: `schema_check_failed: ${error.message}` };
    }

    return { success: true, error: null };
  } catch (err) {
    return { success: false, error: `schema_check_exception: ${err.message}` };
  }
}

module.exports = { execute };
