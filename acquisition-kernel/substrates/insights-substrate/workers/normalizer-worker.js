// substrates/insights/workers/normalizer-worker.js
// Insights Normalizer Worker — bedrock-mounted schema validation + normalization.
//
// Constitutional position:
//   Semantically blind normalizer. Mounted directly on the bedrock
//   (ig-reliability-substrate.js) for error classification of schema
//   failures. Owns schema conformance, field normalization, drift
//   assessment. Does NOT own fetch, retry, or state transitions.
//
// Owns:
//   - Schema validation against contract
//   - Field normalization to DB row shape
//   - Drift severity assessment
//   - Conformance reporting
//
// Does NOT own:
//   - Fetch execution
//   - Error classification (bedrock §2 classifies schema failures)
//   - State transitions (FSM infers)
//   - Quota analysis
//   - Retry decisions

const {
    _normalize,     // §1 — error normalization (for schema error wrapping)
    _classify,      // §2 — classification (for schema failure categorization)
} = require('../../../../substrates/ig-reliability-substrate');

const { normalizeMediaInsight } = require('../normalizer');
const substrate = require('../index');

// ═══════════════════════════════════════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════════════════════════════════════

const REQUIRED_MEDIA_FIELDS = ['media_id', 'media_type'];
const OPTIONAL_MEDIA_FIELDS = [
    'timestamp', 'caption', 'media_url', 'thumbnail_url',
    'permalink', 'like_count', 'comments_count', 'insights',
    // Core metrics (0 when API doesn't return them — not degraded)
    'engagement', 'plays', 'shares', 'saved', 'total_interactions',
    // Video/Reels metrics
    'video_views', 'clips_replays_count', 'ig_reels_avg_watch_time',
    'ig_reels_video_view_total_time',
];
const DRIFT_WARN_THRESHOLD = 0.2;  // 20% degraded → emit PAYLOAD_DEGRADATION
const DRIFT_CRITICAL_THRESHOLD = 0.5; // 50% degraded → critical

// Expected types for media insight fields
const FIELD_TYPES = {
    media_id: 'string',
    media_type: 'string',
    timestamp: 'string',
    caption: ['string', 'null'],
    media_url: ['string', 'null'],
    thumbnail_url: ['string', 'null'],
    permalink: ['string', 'null'],
    like_count: 'number',
    comments_count: 'number',
    insights: 'object',  // Array
    // Core metrics
    engagement: 'number',
    plays: 'number',
    shares: 'number',
    saved: ['number', 'null'],
    total_interactions: 'number',
    video_views: 'number',
    clips_replays_count: 'number',
    ig_reels_avg_watch_time: 'number',
    ig_reels_video_view_total_time: 'number',
};

// ═══════════════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Check if a value matches the expected type.
 * Supports union types (e.g. ['string', 'null']).
 */
function _typeMatches(value, expectedType) {
    if (Array.isArray(expectedType)) {
        return expectedType.some(t => _typeMatches(value, t));
    }
    if (expectedType === 'null') return value === null;
    if (expectedType === 'array') return Array.isArray(value);
    if (expectedType === 'object') return Array.isArray(value) || (typeof value === 'object' && value !== null);
    return typeof value === expectedType;
}

/**
 * Validate a single media insight item against the schema contract.
 *
 * Returns { valid, item, issues[] } where:
 *   - valid: true if all required fields present with correct types
 *   - degraded: true if some optional fields are missing/malformed
 *   - rejected: true if required fields are missing/wrong type
 */
function _validateItem(item, index) {
    const issues = [];
    let missingRequired = false;

    // Required fields
    for (const field of REQUIRED_MEDIA_FIELDS) {
        if (!item[field]) {
            missingRequired = true;
            issues.push({
                field,
                issue: 'missing_required',
                expected: FIELD_TYPES[field],
                received: typeof item[field],
            });
        } else if (FIELD_TYPES[field] && !_typeMatches(item[field], FIELD_TYPES[field])) {
            missingRequired = true;
            issues.push({
                field,
                issue: 'type_mismatch',
                expected: FIELD_TYPES[field],
                received: typeof item[field],
            });
        }
    }

    // Optional fields — check types when present
    let degraded = false;
    for (const field of OPTIONAL_MEDIA_FIELDS) {
        if (item[field] !== undefined && item[field] !== null) {
            if (FIELD_TYPES[field] && !_typeMatches(item[field], FIELD_TYPES[field])) {
                degraded = true;
                issues.push({
                    field,
                    issue: 'type_mismatch_optional',
                    expected: FIELD_TYPES[field],
                    received: typeof item[field],
                });
            }
        }
    }

    // Insights sub-validation
    if (item.insights && Array.isArray(item.insights)) {
        for (const insight of item.insights) {
            if (!insight.name || !insight.values) {
                degraded = true;
                issues.push({
                    field: 'insights[]',
                    issue: 'missing_subfield',
                    detail: 'name or values missing in insight entry',
                });
            }
            if (insight.values && Array.isArray(insight.values)) {
                for (const v of insight.values) {
                    if (v.value === undefined) {
                        degraded = true;
                        issues.push({
                            field: 'insights[].values[].value',
                            issue: 'missing_value',
                            metric: insight.name,
                        });
                    }
                }
            }
        }
    } else if (item.insights !== undefined && item.insights !== null && !Array.isArray(item.insights)) {
        degraded = true;
        issues.push({
            field: 'insights',
            issue: 'type_mismatch',
            expected: 'array',
            received: typeof item.insights,
        });
    }

    const itemId = item.media_id || `index_${index}`;

    return {
        itemId,
        valid: !missingRequired,
        degraded,
        rejected: missingRequired,
        issues: issues.length > 0 ? issues : null,
    };
}

/**
 * Build a drift summary from validation results.
 */
function _buildDriftSummary(results, totalItems) {
    const issueCounts = {};
    for (const r of results) {
        if (r.issues) {
            for (const issue of r.issues) {
                const key = `${issue.field}:${issue.issue}`;
                issueCounts[key] = (issueCounts[key] || 0) + 1;
            }
        }
    }

    const mostCommon = Object.entries(issueCounts)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5)
        .map(([key, count]) => ({ issue: key, count }));

    return {
        totalItems,
        mostCommonIssues: mostCommon,
        uniqueIssueTypes: Object.keys(issueCounts).length,
    };
}

// ═══════════════════════════════════════════════════════════════════════════
// NORMALIZE
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Normalize an acquisition outcome into DB-ready rows.
 *
 * @param {object} outcome — AcquisitionOutcome from acquisition worker
 * @param {string} accountId — business account ID
 * @returns {NormalizationOutcome}
 */
function normalize(outcome, accountId) {
    const observations = [];
    let conformantItems = 0;
    let degradedItems = 0;
    let rejectedItems = 0;
    const rows = [];

    const mediaInsights = (outcome && outcome.data && outcome.data.mediaInsights) || [];
    const totalItems = mediaInsights.length;

    if (totalItems === 0) {
        return {
            success: true,
            rows: [],
            schemaConformance: {
                totalItems: 0,
                conformantItems: 0,
                degradedItems: 0,
                rejectedItems: 0,
                driftDetected: false,
                driftSummary: null,
            },
            observations: [{
                type: 'NORMALIZATION_COMPLETED',
                detail: 'no_items_to_normalize',
            }],
            classification: null,
        };
    }

    // ═══════════════════════════════════════════════════════
    // STEP 1 — SCHEMA VALIDATION
    // ═══════════════════════════════════════════════════════
    const validationResults = [];

    for (let i = 0; i < mediaInsights.length; i++) {
        const result = _validateItem(mediaInsights[i], i);
        validationResults.push(result);

        if (result.rejected) {
            rejectedItems++;
            observations.push({
                type: 'SCHEMA_REJECTION',
                itemId: result.itemId,
                issues: result.issues,
            });
        } else if (result.degraded) {
            degradedItems++;
        } else {
            conformantItems++;
        }
    }

    // ═══════════════════════════════════════════════════════
    // STEP 2 — FIELD NORMALIZATION
    // ═══════════════════════════════════════════════════════
    for (let i = 0; i < mediaInsights.length; i++) {
        const item = mediaInsights[i];
        const validation = validationResults[i];

        if (validation.rejected) continue;  // skip rejected items

        try {
            // Use canonical normalizer for DB row shape
            const row = normalizeMediaInsight(item, accountId);
            row._normalized = true;
            if (validation.degraded) {
                row._degraded = true;
                row._degradation_issues = validation.issues;
            }
            rows.push(row);
        } catch (err) {
            rejectedItems++;
            observations.push({
                type: 'NORMALIZATION_ERROR',
                itemId: validation.itemId,
                error: err.message,
            });
        }
    }

    // ═══════════════════════════════════════════════════════
    // STEP 3 — DRIFT ASSESSMENT
    // ═══════════════════════════════════════════════════════
    const degradedRatio = totalItems > 0 ? (degradedItems + rejectedItems) / totalItems : 0;
    const driftDetected = degradedRatio >= DRIFT_WARN_THRESHOLD;
    let driftSummary = null;
    let classification = null;

    if (driftDetected) {
        driftSummary = _buildDriftSummary(validationResults, totalItems);

        if (degradedRatio >= DRIFT_CRITICAL_THRESHOLD) {
            // Classify the drift as a schema failure via bedrock
            const driftError = new Error(
                `schema_drift_critical: ${degradedItems + rejectedItems}/${totalItems} items degraded`
            );
            driftError.driftSummary = driftSummary;
            driftError.degradedRatio = degradedRatio;
            classification = _classify(driftError, 'normalization:drift', 'schema-contract');

            observations.push({
                type: 'SCHEMA_DRIFT',
                severity: 'CRITICAL',
                degradedRatio,
                driftSummary,
                classification,
            });
        } else {
            observations.push({
                type: 'SCHEMA_DRIFT',
                severity: 'WARN',
                degradedRatio,
                driftSummary,
            });
        }
    }

    // ═══════════════════════════════════════════════════════
    // STEP 4 — EMIT OUTCOME
    // ═══════════════════════════════════════════════════════
    const success = rejectedItems === 0;

    const normalizerOutcome = {
        success,
        rows,
        schemaConformance: {
            totalItems,
            conformantItems,
            degradedItems,
            rejectedItems,
            driftDetected,
            driftSummary,
        },
        observations,
        classification,
    };

    // Emit to substrate bridge for DB write routing
    if (success || rows.length > 0) {
        substrate.emitNormalized('normalizer-worker', {
            accountId,
            rowCount: rows.length,
            schemaConformance: normalizerOutcome.schemaConformance,
            rows,
        });
    }

    if (!success) {
        substrate.emit('normalizer-worker', 'NORMALIZATION_FAILED', {
            accountId,
            rejectedItems,
            totalItems,
            classification: classification ? {
                category: classification.category,
                subtype: classification.subtype,
                confidence: classification.confidence,
            } : null,
        });
    }

    // ═══════════════════════════════════════════════════════
    // STEP 5 — RETURN
    // ═══════════════════════════════════════════════════════
    return normalizerOutcome;
}

// ═══════════════════════════════════════════════════════════════════════════
// EXPORTS
// ═══════════════════════════════════════════════════════════════════════════

module.exports = {
    normalize,
    // Internal helpers exported for testing
    _validateItem,
    _typeMatches,
    _buildDriftSummary,
};
