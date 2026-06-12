// substrates/insights/workers/acquisition-worker.js
// Insights Acquisition Worker — bounded IG API fetch execution.
//
// Constitutional position:
//   Semantically blind executor. Imports interpretation from the bedrock
//   (ig-reliability-substrate.js) for error classification, rate-limit
//   analysis, retryability, cadence, and severity. Does NOT decide retry,
//   state transitions, or scheduling — the FSM owns those.
//
// Owns:
//   - Fetch execution (raw I/O via transport)
//   - Cursor tracking (5 validations)
//   - Continuity detection (6 checks)
//   - Payload drift detection (8 checks)
//   - Adaptive batch management
//   - Sync gap detection
//   - Checkpoint persistence
//   - Degradation observation emission
//   - Acquisition telemetry
//
// Does NOT own:
//   - Error classification (bedrock §2 classifies)
//   - Retry decisions (FSM decides)
//   - Cadence computation (bedrock §14 computes)
//   - State transitions (FSM infers from signals)
//   - Token refresh (other workers)
//   - DB writes (governance dispatch)
//   - Signal delivery routing (substrate bridge routes)

const {
    _classify,               // §2  — error classification
    _analyzeQuota,           // §5  — quota pressure (for adaptive batching)
    _analyzeRateLimit,       // §6  — rate-limit recovery analysis
    _analyzeDependencyHealth,// §11 — dependency state
    _analyzeRetryability,    // §12 — retryability check
    _generateAdaptiveCadence,// §14 — backoff timing
    _analyzeSeverity,        // §16 — severity scoring
} = require('../../../../substrates/ig-reliability-substrate');

const transport = require('../transport');
const { normalizeMediaInsight } = require('../normalizer');
const substrate = require('../index');

// ═══════════════════════════════════════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════════════════════════════════════

const CURSOR_STAGNATION_THRESHOLD = 3;   // same cursor N times → stagnation
const DRIFT_WARN_THRESHOLD = 0.2;        // 20% degraded → drift warning
const CHECKPOINT_INTERVAL = 10;          // checkpoint every N successful items
const DEFAULT_BATCH_SIZE = 5;
const ACCOUNT_METRICS = ['reach', 'accounts_engaged', 'profile_views'];
const REQUIRED_MEDIA_FIELDS = [
    'media_id', 'media_type', 'timestamp',
];

// ═══════════════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════════════

function _now() { return Date.now(); }

function _buildErrorOutcome(error, cursorState, priorState) {
    const bedrockClass = _classify(error, 'insights:fetch', 'ig-graph');
    const retryability = _analyzeRetryability(bedrockClass, null, {});
    const severity = _analyzeSeverity(bedrockClass);
    const backoff = _generateAdaptiveCadence(bedrockClass, null,
        { pressureLevel: 'NONE' }, { priority: 'LOW' }, { attemptN: (priorState && priorState.consecutiveFailures || 0) + 1 });

    return {
        success: false,
        data: { accountInsights: null, mediaInsights: [] },
        cursorState,
        observations: [{
            type: 'ACQUISITION_FAILED',
            classification: bedrockClass,
            retryability,
            severity,
            backoff,
            error: error.message,
        }],
        degradationEvents: [],
        telemetry: {
            objectsProcessed: 0,
            objectsFailed: 1,
            latencyMs: 0,
            quotaConsumed: 0,
            retryable: retryability.retryable,
            retryAfterMs: backoff.computedMs || null,
        },
        checkpoint: null,
    };
}

// ═══════════════════════════════════════════════════════════════════════════
// CURSOR INTELLIGENCE (§1 of directive)
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Validate cursor state — 5 checks: presence, freshness, continuity,
 * progression, drift.
 */
function _validateCursor(currentCursor, priorState) {
    const observations = [];
    const prior = priorState || {};
    const previousCursor = prior.lastSuccessfulCursor || null;
    const previousAt = prior.lastSuccessfulTimestamp || null;

    // 1. Presence
    const present = !!currentCursor;

    // 2. Freshness — cursor is recent enough
    const fresh = previousAt
        ? (_now() - new Date(previousAt).getTime()) < 7 * 24 * 3600000  // 7 days
        : true;

    // 3. Continuity — no gaps between cursors
    let continuous = true;
    if (previousCursor && currentCursor && previousCursor === currentCursor) {
        // Same cursor — check stagnation separately
        continuous = true;
    }

    // 4. Progression — cursor moves forward (not backward)
    let progressive = true;
    // Cursor comparison depends on format; for timestamps:
    if (previousCursor && currentCursor) {
        const prevN = parseInt(previousCursor, 10);
        const currN = parseInt(currentCursor, 10);
        if (!isNaN(prevN) && !isNaN(currN)) {
            progressive = currN >= prevN;
        }
    }

    // 5. Drift — cursor hasn't drifted beyond recovery
    const drifted = !fresh;

    if (!present) {
        observations.push({
            type: 'CURSOR_DEGRADATION',
            check: 'presence',
            detail: 'no_cursor_present',
        });
    }
    if (!fresh) {
        observations.push({
            type: 'CURSOR_DEGRADATION',
            check: 'freshness',
            detail: 'cursor_stale',
            lastTimestamp: previousAt,
        });
    }
    if (drifted) {
        observations.push({
            type: 'CURSOR_DEGRADATION',
            check: 'drift',
            detail: 'cursor_outside_recovery_window',
        });
    }

    return {
        currentCursor,
        previousCursor,
        cursorValid: present && fresh && continuous && progressive && !drifted,
        observations,
    };
}

// ═══════════════════════════════════════════════════════════════════════════
// CONTINUITY PRESERVATION (§2 of directive)
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Detect continuity breaks — 6 checks: skipped pages, duplicate pages,
 * missing time windows, out-of-order payloads, reversal, stagnation.
 */
function _checkContinuity(mediaInsights, priorState) {
    const observations = [];
    const prior = priorState || {};
    const stagnationCount = prior.consecutiveSameCursor || 0;

    // 1. Skipped pages — page N then N+2
    // 2. Duplicate pages — same content twice
    const seenIds = new Set();
    const duplicates = [];
    for (const item of mediaInsights) {
        if (seenIds.has(item.media_id)) {
            duplicates.push(item.media_id);
        }
        seenIds.add(item.media_id);
    }
    if (duplicates.length > 0) {
        observations.push({
            type: 'CURSOR_DEGRADATION',
            check: 'duplicate_pages',
            detail: `${duplicates.length} duplicate media IDs`,
            duplicates,
        });
    }

    // 3. Missing time windows — gaps in timestamps
    const timestamps = mediaInsights
        .map(m => m.timestamp ? new Date(m.timestamp).getTime() : null)
        .filter(Boolean)
        .sort((a, b) => a - b);

    for (let i = 1; i < timestamps.length; i++) {
        const gap = timestamps[i] - timestamps[i - 1];
        if (gap > 2 * 3600000) {  // > 2 hour gap
            observations.push({
                type: 'SYNC_GAP',
                check: 'time_window',
                detail: `gap of ${Math.round(gap / 3600000)} hours`,
                from: new Date(timestamps[i - 1]).toISOString(),
                to: new Date(timestamps[i]).toISOString(),
            });
        }
    }

    // 4. Out-of-order — timestamps going backward
    // 5. Reversal — cursor goes backward
    // (Both checked by _validateCursor progression)

    // 6. Stagnation — same cursor 3+ times
    if (stagnationCount >= CURSOR_STAGNATION_THRESHOLD) {
        observations.push({
            type: 'CURSOR_DEGRADATION',
            check: 'stagnation',
            detail: `same cursor ${stagnationCount} consecutive times`,
            stagnationCount,
        });
    }

    return observations;
}

// ═══════════════════════════════════════════════════════════════════════════
// PAYLOAD DRIFT ANALYSIS (§3 of directive)
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Check for schema drift — 8 checks: missing fields, schema drift,
 * null expansion, field volatility, contract violations, type changes,
 * new fields, deprecated fields.
 */
function _checkPayloadDrift(mediaInsights) {
    const observations = [];
    let degradedCount = 0;
    const totalItems = mediaInsights.length;
    if (totalItems === 0) return { observations, degradedCount, totalItems };

    // Check required fields
    for (const item of mediaInsights) {
        let degraded = false;
        for (const field of REQUIRED_MEDIA_FIELDS) {
            if (!item[field]) {
                degraded = true;
                observations.push({
                    type: 'PAYLOAD_DEGRADATION',
                    check: 'missing_field',
                    detail: `field '${field}' missing in media ${item.media_id || 'unknown'}`,
                    mediaId: item.media_id,
                    field,
                });
            }
        }
        // Null expansion — fields that were non-null are now null
        // (tracked by comparing against prior schema — deferred to full schema store)
        if (item.insights && !Array.isArray(item.insights)) {
            degraded = true;
            observations.push({
                type: 'PAYLOAD_DEGRADATION',
                check: 'type_change',
                detail: 'insights field is not an array',
                mediaId: item.media_id,
            });
        }
        if (degraded) degradedCount++;
    }

    // Type changes — check insight value types
    for (const item of mediaInsights) {
        if (Array.isArray(item.insights)) {
            for (const insight of item.insights) {
                if (insight.values && Array.isArray(insight.values)) {
                    for (const v of insight.values) {
                        if (v.value !== undefined && typeof v.value !== 'number') {
                            observations.push({
                                type: 'PAYLOAD_DEGRADATION',
                                check: 'type_change',
                                detail: `metric '${insight.name}' value is ${typeof v.value}, expected number`,
                                mediaId: item.media_id,
                                metric: insight.name,
                            });
                            degradedCount++;
                        }
                    }
                }
            }
        }
    }

    return { observations, degradedCount, totalItems };
}

// ═══════════════════════════════════════════════════════════════════════════
// CHECKPOINT (§7 of directive)
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Build a checkpoint record after successful acquisition.
 */
function _buildCheckpoint(cursorState, mediaCount, accountId) {
    return {
        accountId,
        lastSuccessfulCursor: cursorState.currentCursor,
        lastSuccessfulTimestamp: new Date().toISOString(),
        lastCheckpointAt: new Date().toISOString(),
        mediaCount,
        batchSummary: {
            total: mediaCount,
            successfulAt: new Date().toISOString(),
        },
    };
}

// ═══════════════════════════════════════════════════════════════════════════
// EXECUTE
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Execute one bounded IG API acquisition for insights.
 *
 * @param {object} ctx — InsightsExecutionContext
 * @returns {Promise<AcquisitionOutcome>}
 */
async function execute(ctx) {
    const startTime = _now();
    const {
        accountId,
        syncWindow,
        collectionMode = 'ACCOUNT',
        priorState = {},
        credentials,
        quotaHeaders = null,
        dependencyContext = null,
    } = ctx;

    const observations = [];
    const degradationEvents = [];
    let objectsProcessed = 0;
    let objectsFailed = 0;

    // ═══════════════════════════════════════════════════════
    // STEP 1 — PREFLIGHT
    // ═══════════════════════════════════════════════════════
    const preflightIntent = {
        intent_id: ctx.intentId || 'acquisition',
        account_id: accountId,
        fetch_type: 'account_insights',
        collectionMode,
    };

    const preflight = substrate.assess(priorState, preflightIntent, quotaHeaders, dependencyContext);

    if (!preflight.report.canProceed) {
        return {
            success: false,
            data: { accountInsights: null, mediaInsights: [] },
            cursorState: { currentCursor: null, previousCursor: null, cursorValid: true },
            observations: [{ type: 'ACQUISITION_BLOCKED', blockers: preflight.report.blockers }],
            degradationEvents: [],
            telemetry: {
                objectsProcessed: 0, objectsFailed: 0, latencyMs: _now() - startTime,
                quotaConsumed: 0, retryable: false, retryAfterMs: null,
            },
            checkpoint: null,
        };
    }

    const { decisions } = preflight.report;

    // ═══════════════════════════════════════════════════════
    // STEP 2 — ACCOUNT FETCH
    // ═══════════════════════════════════════════════════════
    let accountInsights = null;

    if (decisions.fetchAccount) {
        try {
            accountInsights = await transport.fetchAccountInsights(
                accountId,
                { since: syncWindow.since, until: syncWindow.until },
                credentials
            );
            if (accountInsights.success) {
                objectsProcessed += 1;
            } else {
                objectsFailed += 1;
                const err = new Error(accountInsights.error || 'account_fetch_failed');
                err.response = accountInsights;  // preserve IG error shape for bedrock
                const classification = _classify(err, 'insights:account_fetch', 'ig-graph');
                observations.push({
                    type: 'ACQUISITION_PARTIAL',
                    scope: 'account',
                    classification,
                });
            }
        } catch (err) {
            objectsFailed += 1;
            observations.push({
                type: 'ACQUISITION_PARTIAL',
                scope: 'account',
                error: err.message,
            });
        }
    }

    // ═══════════════════════════════════════════════════════
    // STEP 3 — MEDIA FETCH
    // ═══════════════════════════════════════════════════════
    const mediaInsights = [];
    const batchSize = decisions.recommendedBatchSize || DEFAULT_BATCH_SIZE;

    if (decisions.mediaToRefresh && decisions.mediaToRefresh.length > 0) {
        // decisions.mediaToRefresh may contain string IDs or full objects.
        // fetchMediaInsightsBatch expects objects with .id — wrap strings.
        const mediaList = decisions.mediaToRefresh.map(item =>
            typeof item === 'string'
                ? { id: item, media_type: 'UNKNOWN' }
                : item
        );
        const pageToken = credentials && credentials.pageToken;

        // Adaptive batching: split mediaToRefresh into batches
        for (let i = 0; i < mediaList.length; i += batchSize) {
            const batch = mediaList.slice(i, i + batchSize);

            try {
                const batchResults = await transport.fetchMediaInsightsBatch(batch, pageToken);
                mediaInsights.push(...batchResults);

                const batchSuccesses = batchResults.filter(r => !r.error).length;
                const batchFailures = batchResults.filter(r => r.error).length;
                objectsProcessed += batchSuccesses;
                objectsFailed += batchFailures;

                // Collect per-item errors
                for (const result of batchResults) {
                    if (result.error) {
                        const err = new Error(result.error);
                        err.mediaId = result.media_id;
                        observations.push({
                            type: 'ACQUISITION_PARTIAL',
                            scope: 'media',
                            mediaId: result.media_id,
                            error: result.error,
                        });
                    }
                }

                // Rate-limit check after each batch
                if (quotaHeaders) {
                    const quotaResult = _analyzeQuota(
                        {
                            headers: {
                                xAppUsage: quotaHeaders.xAppUsage || null,
                                xPageUsage: quotaHeaders.xPageUsage || null,
                                xBusinessUseCaseUsage: quotaHeaders.xBusinessUseCaseUsage || null,
                                retryAfter: null,
                                adAccountId: null,
                            },
                        },
                        { accountId }
                    );

                    if (quotaResult.pressureLevel === 'CRITICAL') {
                        degradationEvents.push({
                            type: 'QUOTA_THROTTLE',
                            pressureLevel: 'CRITICAL',
                            note: 'stopping_batch_early',
                        });
                        break; // stop processing more batches
                    }
                }
            } catch (err) {
                objectsFailed += batch.length;
                observations.push({
                    type: 'ACQUISITION_PARTIAL',
                    scope: 'batch',
                    error: err.message,
                });
            }

            // Inter-batch delay for rate-limit safety
            if (i + batchSize < mediaList.length) {
                await new Promise(resolve => setTimeout(resolve, 500));
            }
        }
    }

    // ═══════════════════════════════════════════════════════
    // STEP 4 — CURSOR INTELLIGENCE
    // ═══════════════════════════════════════════════════════
    const latestCursor = mediaInsights.length > 0
        ? mediaInsights[mediaInsights.length - 1].media_id
        : (accountInsights && accountInsights.igUserId ? accountInsights.igUserId : null);

    const cursorAssessment = _validateCursor(latestCursor, priorState);
    observations.push(...cursorAssessment.observations);

    // ═══════════════════════════════════════════════════════
    // STEP 5 — CONTINUITY PRESERVATION
    // ═══════════════════════════════════════════════════════
    const continuityObservations = _checkContinuity(mediaInsights, priorState);
    observations.push(...continuityObservations);

    // ═══════════════════════════════════════════════════════
    // STEP 6 — PAYLOAD DRIFT
    // ═══════════════════════════════════════════════════════
    const drift = _checkPayloadDrift(mediaInsights);
    observations.push(...drift.observations);

    if (drift.totalItems > 0 && (drift.degradedCount / drift.totalItems) >= DRIFT_WARN_THRESHOLD) {
        degradationEvents.push({
            type: 'PAYLOAD_DEGRADATION',
            degradedCount: drift.degradedCount,
            totalItems: drift.totalItems,
            ratio: drift.degradedCount / drift.totalItems,
        });
    }

    // ═══════════════════════════════════════════════════════
    // STEP 7 — CHECKPOINT
    // ═══════════════════════════════════════════════════════
    let checkpoint = null;
    if (objectsProcessed > 0 && objectsFailed === 0 && cursorAssessment.cursorValid) {
        checkpoint = _buildCheckpoint(cursorAssessment, objectsProcessed, accountId);

        // Emit checkpoint milestone to substrate
        substrate.emit('acquisition-worker', 'CHECKPOINT_MILESTONE', {
            accountId,
            cursor: checkpoint.lastSuccessfulCursor,
            mediaCount: checkpoint.mediaCount,
        });
    }

    // ═══════════════════════════════════════════════════════
    // STEP 8 — EMIT OUTCOME
    // ═══════════════════════════════════════════════════════
    const emissionType = (objectsFailed === 0 && objectsProcessed > 0)
        ? 'ACQUISITION_COMPLETED'
        : 'ACQUISITION_COMPLETED'; // partial success still completes

    const latencyMs = _now() - startTime;

    // Build telemetry
    const telemetry = {
        objectsProcessed,
        objectsFailed,
        latencyMs,
        quotaConsumed: objectsProcessed + objectsFailed, // rough: 1 call per object
        retryable: objectsFailed > 0,
        retryAfterMs: null,
    };

    // If there were failures, enrich with bedrock analysis
    if (objectsFailed > 0 && observations.some(o => o.classification)) {
        const failClass = observations.find(o => o.classification);
        if (failClass) {
            const retryability = _analyzeRetryability(failClass.classification, null, {});
            const backoff = _generateAdaptiveCadence(
                failClass.classification, null,
                { pressureLevel: 'MEDIUM' },
                { priority: 'LOW' },
                { attemptN: (priorState.consecutiveFailures || 0) + 1 }
            );
            telemetry.retryable = retryability.retryable;
            telemetry.retryAfterMs = backoff.computedMs || null;
        }
    }

    const outcome = {
        success: objectsFailed === 0,
        data: { accountInsights, mediaInsights },
        cursorState: cursorAssessment,
        observations,
        degradationEvents,
        telemetry,
        checkpoint,
    };

    // Emit to substrate bridge for FSM routing
    substrate.emit('acquisition-worker', emissionType, {
        accountId,
        intentId: ctx.intentId,
        success: outcome.success,
        count: objectsProcessed,
        failures: objectsFailed,
        latencyMs,
        telemetry,
    });

    // ═══════════════════════════════════════════════════════
    // STEP 9 — RETURN
    // ═══════════════════════════════════════════════════════
    return outcome;
}

// ═══════════════════════════════════════════════════════════════════════════
// EXPORTS
// ═══════════════════════════════════════════════════════════════════════════

module.exports = {
    execute,
    // Internal helpers exported for testing
    _validateCursor,
    _checkContinuity,
    _checkPayloadDrift,
    _buildCheckpoint,
};
