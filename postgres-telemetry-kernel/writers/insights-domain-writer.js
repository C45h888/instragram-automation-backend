// postgres-telemetry-kernel/writers/insights-domain-writer.js
// Insights Domain Writer — Postgres time-series writer for instagram_media_insights.
//
// Constitutional position:
//   Semantically blind execution layer. Receives DB_WRITE_REQUESTED events
//   from the CK for domain='insights'. Explodes flat normalizer rows into
//   time-series metric rows. All Supabase I/O delegated to bedrock.
//
// Owns: row explosion (1 flat row → N metric rows), domain logic.
// Does NOT own: Supabase I/O (bedrock), error classification (bedrock),
//               retry decisions (FSM), state transitions (FSM).
//
// Chain: CK → DB_WRITE_REQUESTED → this writer → bedrock → DB_WRITE_COMPLETE/FAILED

const bedrock = require('../bedrock');

// ═══════════════════════════════════════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════════════════════════════════════

// All metric names extracted by normalizeMediaInsight
const METRIC_NAMES = [
    'reach', 'impressions', 'engagement', 'plays', 'shares', 'saved',
    'total_interactions', 'video_views', 'clips_replays_count',
    'ig_reels_avg_watch_time', 'ig_reels_video_view_total_time',
];

// Fields that belong to the metadata table (not metrics)
const METADATA_FIELDS = new Set([
    'instagram_media_id', 'business_account_id', 'media_type', 'caption',
    'media_url', 'thumbnail_url', 'permalink', 'like_count',
    'comments_count', 'published_at',
]);

// ═══════════════════════════════════════════════════════════════════════════
// HELPERS (domain logic — stays in writer)
// ═══════════════════════════════════════════════════════════════════════════

function _extractMetadata(row, capturedAt) {
    const meta = {};
    for (const [key, value] of Object.entries(row)) {
        if (METADATA_FIELDS.has(key)) meta[key] = value;
    }
    meta.updated_at = capturedAt;
    return meta;
}

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
        if (value === null || value === undefined) continue;
        metricRows.push({ ...base, metric_name: metricName, metric_value: value });
    }
    return metricRows;
}

// ═══════════════════════════════════════════════════════════════════════════
// EXECUTE
// ═══════════════════════════════════════════════════════════════════════════

async function execute(event, ctx) {
    const startTime = Date.now();
    const rows = event.rows || [];
    const accountId = event.accountId;
    const intentId = event.intentId;

    if (rows.length === 0) {
        if (ctx && ctx.emit) {
            ctx.emit({ type: 'DB_WRITE_COMPLETE', domain: event.domain || 'insights', accountId, intentId, table: 'instagram_media_insights', count: 0, error: null });
        }
        return { success: true, count: 0 };
    }

    const capturedAt = new Date().toISOString();
    let totalMetricsWritten = 0;

    // Bridge ctx.emit → governance.dispatch for bedrock
    const governance = ctx ? { dispatch: (e) => ctx.emit(e), dispatchGlobal: (e) => ctx.emit(e) } : null;

    try {
        for (const row of rows) {
            if (!row.instagram_media_id) continue;

            // 1. UPSERT metadata → bedrock
            const metadata = _extractMetadata(row, capturedAt);
            const metaResult = await bedrock.insights.persistInsightMedia(metadata, {
                accountId, intentId, governance, domain: event.domain,
            });
            if (!metaResult.success) {
                throw Object.assign(new Error(metaResult.error), {
                    row: row.instagram_media_id, phase: 'metadata_upsert',
                });
            }

            // 2. EXPLODE + INSERT metrics → bedrock
            const metricRows = _explodeMetrics(row, capturedAt);
            if (metricRows.length === 0) continue;

            const metricResult = await bedrock.insights.persistInsightMetrics(metricRows, {
                accountId, intentId, governance, domain: event.domain,
            });
            if (!metricResult.success) {
                throw Object.assign(new Error(metricResult.error), {
                    row: row.instagram_media_id, phase: 'metrics_insert',
                });
            }

            totalMetricsWritten += metricRows.length;
        }

        const latencyMs = Date.now() - startTime;
        if (ctx && ctx.emit) {
            ctx.emit({ type: 'DB_WRITE_COMPLETE', domain: event.domain || 'insights', accountId, intentId, table: 'instagram_media_insights', operation: event.operation, rowCount: rows.length, metricsWritten: totalMetricsWritten, latencyMs, timestamp: new Date().toISOString() });
        }
        return { success: true, count: rows.length };

    } catch (err) {
        const latencyMs = Date.now() - startTime;
        if (ctx && ctx.emit) {
            ctx.emit({ type: 'DB_WRITE_FAILED', domain: event.domain || 'insights', accountId, intentId, table: 'instagram_media_insights', operation: event.operation, error: err.message, code: err.code || null, latencyMs, timestamp: new Date().toISOString() });
        }
        return { success: false, count: 0, error: err.message };
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// EXPORTS
// ═══════════════════════════════════════════════════════════════════════════

module.exports = {
    execute,
    _extractMetadata,
    _explodeMetrics,
    METRIC_NAMES,
    METADATA_FIELDS,
};
