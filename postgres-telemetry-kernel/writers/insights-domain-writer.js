// postgres-telemetry-kernel/writers/insights-domain-writer.js
// Insights Domain Writer — Postgres time-series writer for instagram_media_insights.
//
// Constitutional position:
//   Semantically blind execution layer. Receives DB_WRITE_REQUESTED events
//   from the CK (Constitutional Kernel) for domain='insights'. Explodes
//   flat normalizer rows into time-series metric rows. Writes to Postgres
//   via Supabase client. Does NOT interpret, classify, or decide.
//
// Owns:
//   - Row explosion (1 flat row → N metric rows)
//   - Postgres UPSERT (instagram_media) + INSERT (instagram_media_insights)
//   - Transactional safety (all rows in one operation)
//   - Completion/failure event emission via CK callback
//
// Does NOT own:
//   - Data normalization (normalizer owns)
//   - Error classification (bedrock owns)
//   - Retry decisions (FSM owns)
//   - State transitions (FSM owns)
//   - Quota or rate-limit (acquisition worker owns)
//
// Chain: CK → DB_WRITE_REQUESTED → this writer → DB_WRITE_COMPLETE / DB_WRITE_FAILED

const { getSupabaseAdmin } = require('../../config/supabase');

// ═══════════════════════════════════════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════════════════════════════════════

const METADATA_TABLE = 'instagram_media';
const INSIGHTS_TABLE = 'instagram_media_insights';

// All metric names extracted by normalizeMediaInsight
const METRIC_NAMES = [
    'reach',
    'impressions',
    'engagement',
    'plays',
    'shares',
    'saved',
    'total_interactions',
    'video_views',
    'clips_replays_count',
    'ig_reels_avg_watch_time',
    'ig_reels_video_view_total_time',
];

// Fields that belong to the metadata table (not metrics)
const METADATA_FIELDS = new Set([
    'instagram_media_id',
    'business_account_id',
    'media_type',
    'caption',
    'media_url',
    'thumbnail_url',
    'permalink',
    'like_count',
    'comments_count',
    'published_at',
]);

// ═══════════════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Extract metadata columns from a flat normalizer row.
 * Only returns fields present in METADATA_FIELDS.
 */
function _extractMetadata(row, capturedAt) {
    const meta = {};
    for (const [key, value] of Object.entries(row)) {
        if (METADATA_FIELDS.has(key)) {
            meta[key] = value;
        }
    }
    meta.updated_at = capturedAt;
    return meta;
}

/**
 * Explode a flat row into metric rows for the time-series table.
 * Skips null values (story saves), includes zero values (measured as zero).
 */
function _explodeMetrics(row, capturedAt) {
    const base = {
        instagram_media_id: row.instagram_media_id,
        business_account_id: row.business_account_id,
        media_type: row.media_type || null,
        captured_at: capturedAt,
        source: 'ig-graph-api',
    };

    const metricRows = [];
    for (const metricName of METRIC_NAMES) {
        const value = row[metricName];

        // NULL means not applicable for this media type (story saved)
        if (value === null || value === undefined) continue;

        metricRows.push({
            ...base,
            metric_name: metricName,
            metric_value: value,
        });
    }
    return metricRows;
}

/**
 * Build the completion event to emit back to CK.
 */
function _buildCompleteEvent(event, count, latencyMs) {
    return {
        type: 'DB_WRITE_COMPLETE',
        domain: event.domain || 'insights',
        accountId: event.accountId,
        intentId: event.intentId,
        table: INSIGHTS_TABLE,
        operation: event.operation || 'batch_upsert_insights',
        rowCount: count,
        metricsWritten: count * METRIC_NAMES.length, // approximate
        latencyMs,
        timestamp: new Date().toISOString(),
    };
}

/**
 * Build the failure event to emit back to CK.
 */
function _buildFailureEvent(event, error, latencyMs) {
    return {
        type: 'DB_WRITE_FAILED',
        domain: event.domain || 'insights',
        accountId: event.accountId,
        intentId: event.intentId,
        table: INSIGHTS_TABLE,
        operation: event.operation || 'batch_upsert_insights',
        error: error.message,
        code: error.code || null,
        latencyMs,
        timestamp: new Date().toISOString(),
    };
}

// ═══════════════════════════════════════════════════════════════════════════
// EXECUTE
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Execute the DB write for insights domain data.
 *
 * Called by the CK when a DB_WRITE_REQUESTED event arrives with domain='insights'.
 * The writer is semantically blind — it performs the write and reports the outcome.
 * The FSM owns state transitions. The writer only executes.
 *
 * @param {object} event — DB_WRITE_REQUESTED event payload
 *   { type, domain, accountId, intentId, table, operation, rows }
 * @param {object} ctx — execution context from CK
 *   { emit(event) — call this to send completion/failure events back to CK }
 * @returns {Promise<{ success: boolean, count: number, error?: string }>}
 */
async function execute(event, ctx) {
    const startTime = Date.now();
    const rows = event.rows || [];
    const accountId = event.accountId;

    if (rows.length === 0) {
        const outcome = { success: true, count: 0 };
        if (ctx && ctx.emit) {
            ctx.emit(_buildCompleteEvent(event, 0, Date.now() - startTime));
        }
        return outcome;
    }

    const client = getSupabaseAdmin();
    if (!client) {
        const err = new Error('supabase_client_unavailable');
        if (ctx && ctx.emit) {
            ctx.emit(_buildFailureEvent(event, err, Date.now() - startTime));
        }
        return { success: false, count: 0, error: 'supabase_client_unavailable' };
    }

    const capturedAt = new Date().toISOString();
    let totalMetricsWritten = 0;

    try {
        // Process rows sequentially to keep transaction scoped per-row if needed,
        // or batch if the client supports it
        for (const row of rows) {
            if (!row.instagram_media_id) continue;

            // 1. UPSERT metadata
            const metadata = _extractMetadata(row, capturedAt);
            const { error: metaError } = await client
                .from(METADATA_TABLE)
                .upsert(metadata, { onConflict: 'instagram_media_id' });

            if (metaError) {
                throw Object.assign(new Error(metaError.message), {
                    code: metaError.code,
                    row: row.instagram_media_id,
                    phase: 'metadata_upsert',
                });
            }

            // 2. EXPLODE + INSERT metrics
            const metricRows = _explodeMetrics(row, capturedAt);
            if (metricRows.length === 0) continue;

            const { error: metricError } = await client
                .from(INSIGHTS_TABLE)
                .insert(metricRows);

            if (metricError) {
                throw Object.assign(new Error(metricError.message), {
                    code: metricError.code,
                    row: row.instagram_media_id,
                    phase: 'metrics_insert',
                });
            }

            totalMetricsWritten += metricRows.length;
        }

        const latencyMs = Date.now() - startTime;

        // Emit completion event back to CK
        if (ctx && ctx.emit) {
            ctx.emit(_buildCompleteEvent(event, rows.length, latencyMs));
        }

        return { success: true, count: rows.length };

    } catch (err) {
        const latencyMs = Date.now() - startTime;

        // Emit failure event back to CK
        if (ctx && ctx.emit) {
            ctx.emit(_buildFailureEvent(event, err, latencyMs));
        }

        return { success: false, count: 0, error: err.message };
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// EXPORTS
// ═══════════════════════════════════════════════════════════════════════════

module.exports = {
    execute,
    // Exported for testing
    _extractMetadata,
    _explodeMetrics,
    METRIC_NAMES,
    METADATA_FIELDS,
    METADATA_TABLE,
    INSIGHTS_TABLE,
};
