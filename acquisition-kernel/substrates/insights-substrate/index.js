// substrates/insights/index.js
// Insights Substrate — Signal-Emission Layer (Phase A)
//
// Constitutional position:
//   Read-only signal emitter. Imports interpretation from the bedrock
//   (ig-reliability-substrate.js), runs preflight assessment, and emits
//   structured signals that the FSM will later consume to infer state
//   transitions for the worker.
//
// Does NOT own:
//   - State machine (FSM infers from signals)
//   - State tracking (FSM tracks state)
//   - Worker execution (future phase)
//   - Retry decisions (FSM decides)
//   - Error classification (bedrock §2 classifies)
//   - Cadence computation (bedrock §14 computes)
//
// Owns:
//   - Preflight assessment (6 ordered guards)
//   - Signal emission (structured output for FSM consumption)
//   - Refresh policy constants
//   - Degradation signal definitions
//   - Backward compat fetch/persist (delegates to existing transport/normalizer)

const {
    _analyzeQuota,           // §5  — quota pressure assessment
    _analyzeDependencyHealth,// §11 — dependency state evaluation
    _analyzePrioritization,  // §7  — operation priority + deferral
    _analyzeRateLimit,       // §6  — rate-limit recovery (future signal enrichment)
    _classify,               // §2  — error classification (future signal enrichment)
    _normalize,              // §1  — error normalization (future signal enrichment)
} = require('../../../substrates/ig-reliability-substrate');

// Existing deps — preserved for backward compat
const InsightsWorker = require('./fetch-workers/insights-fetcher');
const { normalizeMediaInsight } = require('./normalizer');
const { syncHashtagsFromCaptions } = require('./hashtag-sync');
const { getSupabaseAdmin } = require('../../../config/supabase');
const { resolveAccountCredentials } =
    require('../../../graph-capability-kernel/substrates/credential-resolver');

// ═══════════════════════════════════════════════════════════════════════════
// CONFIG CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════

const REFRESH_POLICY = {
    account: { day: 24, week: 168 },   // hours — staleness thresholds
    media:   { lifetime: 12 },         // hours — per-media staleness
};

const MAX_BATCH_SIZES = {
    NONE:     5,
    LOW:      5,
    MEDIUM:   3,
    HIGH:     2,
    CRITICAL: 0,
};

// ═══════════════════════════════════════════════════════════════════════════
// SIGNAL DEFINITIONS
// ═══════════════════════════════════════════════════════════════════════════

const SIGNAL_DEFINITIONS = {
    // ── Bedrock-derived (active now) ──────────────────────────────────
    QUOTA_PRESSURE: {
        level: 'WARN',
        description: 'Quota usage at HIGH or above — batch size reduced',
    },
    DEPENDENCY_DEGRADATION: {
        level: 'ERROR',
        description: 'IG Graph API dependency DEGRADED or FAILED',
    },
    // ── Preflight-specific (blocker signals) ──────────────────────────
    TOKEN_BLOCKED: {
        level: 'ERROR',
        description: 'Token expired or invalid — cannot proceed',
    },
    DEPENDENCY_BLOCKED: {
        level: 'ERROR',
        description: 'Dependency health FAILED — cannot proceed',
    },
    QUOTA_BLOCKED: {
        level: 'ERROR',
        description: 'Quota at CRITICAL — cannot proceed',
    },
    STALENESS_SKIP: {
        level: 'INFO',
        description: 'Data still fresh — skipping fetch',
    },
    STALENESS_REFRESH: {
        level: 'INFO',
        description: 'Data stale — media queued for refresh',
    },
    PRIORITY_DEFERRED: {
        level: 'WARN',
        description: 'Operation deferred due to priority + quota pressure',
    },
    PLAN_GENERATED: {
        level: 'INFO',
        description: 'Preflight complete — acquisition plan ready',
    },
    // ── Future worker-detected (definitions present, emission stubbed) ─
    THROTTLE_PRESSURE: {
        level: 'WARN',
        description: 'Rate-limit throttle recommended by bedrock §6',
    },
    LATENCY_ESCALATION: {
        level: 'WARN',
        description: 'IG API latency > 5s threshold',
    },
    CURSOR_DEGRADATION: {
        level: 'ERROR',
        description: 'Cursor validation failed — gap/reversal/stagnation',
    },
    PAYLOAD_DEGRADATION: {
        level: 'ERROR',
        description: 'Response payload schema drift detected',
    },
    SYNC_GAP: {
        level: 'WARN',
        description: 'Time window missing from acquisition data',
    },
    CHECKPOINT_RISK: {
        level: 'WARN',
        description: 'N consecutive fetches without checkpoint persist',
    },
};

// ═══════════════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Build a synthetic normalized envelope from raw quota headers.
 * Bedrock §5 _analyzeQuota reads normalized.headers.xAppUsage etc.
 * When we're in preflight (no error), we construct this envelope
 * so bedrock can analyze quota pressure from headers alone.
 */
function _buildNormalizedFromHeaders(quotaHeaders) {
    return {
        httpStatus: null,
        graphCode: null,
        graphSubcode: null,
        message: 'preflight_quota_check',
        errorType: null,
        errorUserTitle: null,
        errorUserMsg: null,
        isTransient: null,
        fbtraceId: null,
        requestId: null,
        executionMs: null,
        headers: {
            retryAfter: (quotaHeaders && quotaHeaders.retryAfter) || null,
            xAppUsage: (quotaHeaders && quotaHeaders.xAppUsage) || null,
            xBusinessUseCaseUsage: (quotaHeaders && quotaHeaders.xBusinessUseCaseUsage) || null,
            xPageUsage: (quotaHeaders && quotaHeaders.xPageUsage) || null,
            adAccountId: null,
        },
        raw: null,
    };
}

/**
 * Build a synthetic classified object for preflight bedrock calls.
 * Bedrock §11 _analyzeDependencyHealth needs a classified object
 * for reclassification logic. During preflight there is no error,
 * so we pass UNKNOWN.
 */
function _buildPreflightClassified() {
    return {
        category: 'UNKNOWN',
        subtype: 'preflight_assessment',
        confidence: 1.0,
        reasoning: ['preflight_synthetic'],
    };
}

function _now() {
    return Date.now();
}

/**
 * Make a signal object in the canonical shape the FSM will consume.
 */
function _makeSignal(signalName, source, payload) {
    const def = SIGNAL_DEFINITIONS[signalName];
    return {
        signal: signalName,
        level: def ? def.level : 'INFO',
        source,
        payload: payload || {},
        timestamp: new Date().toISOString(),
    };
}

// ═══════════════════════════════════════════════════════════════════════════
// PREFLIGHT ASSESSMENT
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Run preflight assessment — 6 ordered guards.
 *
 * Returns { report, signals } for FSM consumption.
 * The FSM infers state transitions from the signals.
 * This function is read-only. No mutations. No API calls.
 *
 * @param {object} priorState — from governance DB_READ
 *   { lastAccountFetch, mediaFetchMap, tokenAuthorization }
 * @param {object} intent — the acquisition intent
 *   { intent_id, account_id, fetch_type, collectionMode }
 * @param {object} quotaHeaders — from last IG API response
 *   { xAppUsage, xPageUsage, xBusinessUseCaseUsage, retryAfter }
 * @param {object} dependencyContext — from health substrate
 *   { state, lastIncidentAt }
 * @returns {{ report: object, signals: object[] }}
 */
function assess(priorState, intent, quotaHeaders, dependencyContext) {
    const signals = [];
    const blockers = [];
    const now = _now();

    // Safe defaults for missing inputs
    const state = priorState || {};
    const mode = (intent && intent.collectionMode) || 'ACCOUNT';
    const accountId = (intent && intent.account_id) || null;
    const intentId = (intent && intent.intent_id) || null;
    const tokenAuth = state.tokenAuthorization || 'unknown';

    // ── Default decisions (populated by guards) ──────────────────
    const decisions = {
        fetchAccount: false,
        accountMetrics: [],
        accountPeriod: 'day',
        mediaToRefresh: [],
        mediaToSkip: [],
        recommendedBatchSize: MAX_BATCH_SIZES.NONE,
        estimatedCallCount: 0,
    };

    // ── Default assessments (populated by guards) ───────────────
    let canProceed = true;
    let pressureLevel = 'NONE';
    let quotaAssessment = {
        pressureLevel: 'NONE',
        appUsagePercent: null,
        pageUsagePercent: null,
        throttledCallTypes: [],
    };
    let dependencyState = 'UNKNOWN';
    const stalenessMap = {
        accountInsightsAge: null,
        mediaInsightsAges: {},
    };

    // ═══════════════════════════════════════════════════════════
    // GUARD 1 — TOKEN
    // ═══════════════════════════════════════════════════════════
    if (tokenAuth === 'expired' || tokenAuth === 'invalid') {
        canProceed = false;
        blockers.push('token');
        signals.push(_makeSignal('TOKEN_BLOCKED', 'guard:token', {
            tokenAuthorization: tokenAuth,
            accountId,
        }));
        return _buildResult(canProceed, decisions, blockers, quotaAssessment,
            stalenessMap, dependencyState, accountId, intentId, signals);
    }

    // ═══════════════════════════════════════════════════════════
    // GUARD 2 — DEPENDENCY
    // ═══════════════════════════════════════════════════════════
    if (dependencyContext) {
        try {
            const synthNorm = _buildNormalizedFromHeaders(quotaHeaders);
            const synthClass = _buildPreflightClassified();
            const depResult = _analyzeDependencyHealth(synthNorm, synthClass, {
                dependencyHealth: dependencyContext,
            });

            dependencyState = depResult.dependencyState;

            if (depResult.dependencyState === 'FAILED' || depResult.dependencyState === 'DEGRADED') {
                canProceed = false;
                blockers.push('dependency');
                signals.push(_makeSignal('DEPENDENCY_DEGRADATION', 'guard:dependency', {
                    dependencyState: depResult.dependencyState,
                    lastIncidentAt: dependencyContext.lastIncidentAt || null,
                    elevatedLatency: depResult.elevatedLatency,
                }));
                signals.push(_makeSignal('DEPENDENCY_BLOCKED', 'guard:dependency', {
                    dependencyState: depResult.dependencyState,
                }));
            }
        } catch (err) {
            // Bedrock failure during dependency check → degrade, don't crash
            signals.push(_makeSignal('DEPENDENCY_DEGRADATION', 'guard:dependency', {
                error: 'bedrock_call_failed',
                message: err.message,
            }));
            dependencyState = 'UNKNOWN';
            // Proceed with caution — don't block on bedrock failure
        }
    }

    if (!canProceed) {
        return _buildResult(canProceed, decisions, blockers, quotaAssessment,
            stalenessMap, dependencyState, accountId, intentId, signals);
    }

    // ═══════════════════════════════════════════════════════════
    // GUARD 3 — QUOTA
    // ═══════════════════════════════════════════════════════════
    try {
        const synthNorm = _buildNormalizedFromHeaders(quotaHeaders);
        const quotaResult = _analyzeQuota(synthNorm, { accountId });

        pressureLevel = quotaResult.pressureLevel;
        quotaAssessment = {
            pressureLevel: quotaResult.pressureLevel,
            appUsagePercent: quotaResult.appUsagePercent,
            pageUsagePercent: quotaResult.pageUsagePercent,
            throttledCallTypes: quotaResult.throttledCallTypes || [],
        };

        if (quotaResult.pressureLevel === 'CRITICAL') {
            canProceed = false;
            blockers.push('quota');
            signals.push(_makeSignal('QUOTA_BLOCKED', 'guard:quota', {
                pressureLevel: 'CRITICAL',
                appUsagePercent: quotaResult.appUsagePercent,
                pageUsagePercent: quotaResult.pageUsagePercent,
            }));
        } else if (quotaResult.pressureLevel === 'HIGH') {
            signals.push(_makeSignal('QUOTA_PRESSURE', 'guard:quota', {
                pressureLevel: 'HIGH',
                appUsagePercent: quotaResult.appUsagePercent,
                pageUsagePercent: quotaResult.pageUsagePercent,
                note: 'degraded_path_proceeding',
            }));
            // canProceed stays true — degraded path
        }
    } catch (err) {
        // Bedrock failure during quota check → assume pressure, degrade
        signals.push(_makeSignal('QUOTA_PRESSURE', 'guard:quota', {
            error: 'bedrock_call_failed',
            message: err.message,
        }));
        pressureLevel = 'MEDIUM'; // conservative default
        quotaAssessment.pressureLevel = 'MEDIUM';
    }

    if (!canProceed) {
        return _buildResult(canProceed, decisions, blockers, quotaAssessment,
            stalenessMap, dependencyState, accountId, intentId, signals);
    }

    // ═══════════════════════════════════════════════════════════
    // GUARD 4 — STALENESS
    // ═══════════════════════════════════════════════════════════
    const lastAccountFetch = state.lastAccountFetch || null;

    if (lastAccountFetch) {
        stalenessMap.accountInsightsAge = (now - lastAccountFetch) / 3600000;

        if (stalenessMap.accountInsightsAge < REFRESH_POLICY.account.day) {
            // Account data is fresh — skip account fetch
            signals.push(_makeSignal('STALENESS_SKIP', 'guard:staleness', {
                scope: 'account',
                age: stalenessMap.accountInsightsAge,
                threshold: REFRESH_POLICY.account.day,
            }));
        } else {
            // Account data is stale — fetch needed
            decisions.fetchAccount = true;
            decisions.accountMetrics = ['reach', 'accounts_engaged', 'profile_views'];
            decisions.accountPeriod = 'day';
        }
    } else {
        // No prior state — first fetch
        decisions.fetchAccount = true;
        decisions.accountMetrics = ['reach', 'accounts_engaged', 'profile_views'];
        decisions.accountPeriod = 'day';
    }

    // Per-media staleness
    const mediaMap = state.mediaFetchMap || {};
    for (const [mediaId, lastFetch] of Object.entries(mediaMap)) {
        const age = (now - lastFetch) / 3600000;
        stalenessMap.mediaInsightsAges[mediaId] = age;

        if (age > REFRESH_POLICY.media.lifetime) {
            decisions.mediaToRefresh.push(mediaId);
        } else {
            decisions.mediaToSkip.push(mediaId);
        }
    }

    if (decisions.mediaToRefresh.length > 0) {
        signals.push(_makeSignal('STALENESS_REFRESH', 'guard:staleness', {
            count: decisions.mediaToRefresh.length,
            threshold: REFRESH_POLICY.media.lifetime,
        }));
    }

    // ═══════════════════════════════════════════════════════════
    // GUARD 5 — PRIORITY
    // ═══════════════════════════════════════════════════════════
    const isForcedMode = (mode === 'BACKFILL' || mode === 'RECOVERY');

    try {
        const prioResult = _analyzePrioritization('insights', {
            quotaMetadata: { pressureLevel },
        });

        if (prioResult.deferrable && pressureLevel === 'HIGH' && !isForcedMode) {
            canProceed = false;
            blockers.push('priority');
            signals.push(_makeSignal('PRIORITY_DEFERRED', 'guard:priority', {
                priority: prioResult.priority,
                pressureLevel,
                mode,
                reason: 'insights_is_deferrable_under_quota_pressure',
            }));
        }
    } catch (err) {
        // Bedrock failure on priority → don't block, log warning
        signals.push(_makeSignal('PRIORITY_DEFERRED', 'guard:priority', {
            error: 'bedrock_call_failed',
            message: err.message,
            note: 'proceeding_despite_priority_check_failure',
        }));
    }

    if (!canProceed) {
        return _buildResult(canProceed, decisions, blockers, quotaAssessment,
            stalenessMap, dependencyState, accountId, intentId, signals);
    }

    // ═══════════════════════════════════════════════════════════
    // GUARD 6 — PLAN GENERATION
    // ═══════════════════════════════════════════════════════════
    decisions.recommendedBatchSize = MAX_BATCH_SIZES[pressureLevel] || MAX_BATCH_SIZES.NONE;
    decisions.estimatedCallCount =
        (decisions.fetchAccount ? 2 : 0) + decisions.mediaToRefresh.length;

    signals.push(_makeSignal('PLAN_GENERATED', 'guard:plan', {
        fetchAccount: decisions.fetchAccount,
        accountMetrics: decisions.accountMetrics,
        mediaToRefreshCount: decisions.mediaToRefresh.length,
        mediaToSkipCount: decisions.mediaToSkip.length,
        batchSize: decisions.recommendedBatchSize,
        estimatedCalls: decisions.estimatedCallCount,
    }));

    return _buildResult(true, decisions, blockers, quotaAssessment,
        stalenessMap, dependencyState, accountId, intentId, signals);
}

// ═══════════════════════════════════════════════════════════════════════════
// RESULT BUILDER
// ═══════════════════════════════════════════════════════════════════════════

function _buildResult(canProceed, decisions, blockers, quotaAssessment,
    stalenessMap, dependencyState, accountId, intentId, signals) {

    return {
        report: {
            canProceed,
            decisions,
            blockers,
            quotaAssessment,
            stalenessMap,
            dependencyState,
            accountId,
            intentId,
        },
        signals,
    };
}

// ═══════════════════════════════════════════════════════════════════════════
// BACKWARD COMPAT — fetch + persist (preserved from original substrate)
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Fetch raw data from Instagram API for insights domain.
 * Factory-creates an InsightsWorker and delegates the bounded call.
 *
 * Step 7: substrate resolves credentials internally.
 *
 * @param {string} accountId
 * @param {object} params — { since?, until? }
 * @returns {Promise<object>} raw transport response
 */
async function fetch(accountId, params) {
    const credentials = await resolveAccountCredentials(accountId);
    const worker = new InsightsWorker();
    return worker.execute(accountId, params, credentials);
}

/**
 * Persist insights data to Supabase.
 * Constitutional path: normalize → CK(DB_WRITE_REQUESTED) → writer → hashtag sync.
 * Uses canonical normalizer — no inline field mapping.
 */
async function persist(accountId, rawData, extra = {}) {
    const governance = extra._governance;

    if (!rawData.insights || rawData.insights.length === 0) return { count: 0 };

    const rows = rawData.insights
        .filter(item => item && item.media_id)
        .map(item => normalizeMediaInsight(item, accountId));

    if (rows.length === 0) return { count: 0 };

    governance?.dispatch({
        type: 'DB_WRITE_REQUESTED',
        domain: 'insights', accountId, intentId: null,
        table: 'instagram_media',
        operation: 'batch_upsert_insights',
        rows,
    });

    // Side-effect: extract hashtags from captions into ugc_monitored_hashtags
    const captions = rawData.insights.map(p => p.caption).filter(Boolean);
    if (captions.length > 0) {
        const supabase = getSupabaseAdmin();
        if (supabase) syncHashtagsFromCaptions(supabase, accountId, captions).catch(() => {});
    }

    return { count: rows.length };
}

// ═══════════════════════════════════════════════════════════════════════════
// SUBSTRATE BRIDGE — worker emission → FSM signal translation
// ═══════════════════════════════════════════════════════════════════════════

// In-memory signal buffer — FSM drains via getPendingSignals()
const _pendingSignals = [];

/**
 * Worker → Substrate → FSM emission routing.
 *
 * Workers call this to publish structured emissions. The substrate
 * validates, enriches with bedrock context, translates to FSM signals,
 * and buffers them for FSM consumption.
 *
 * @param {string} workerId — 'acquisition-worker' | 'normalizer-worker'
 * @param {string} emissionType — e.g. 'ACQUISITION_COMPLETED'
 * @param {object} payload — worker-specific data
 * @returns {{ accepted: boolean, signalEmitted: string | null, error: string | null }}
 */
function emit(workerId, emissionType, payload) {
    if (!workerId || !emissionType) {
        return { accepted: false, signalEmitted: null, error: 'missing workerId or emissionType' };
    }

    const translation = EMISSION_TRANSLATION[emissionType];
    if (!translation) {
        // Unknown emission — accept but don't translate
        _pendingSignals.push({
            signalId: _makeSignalId(),
            signal: emissionType,
            level: 'INFO',
            accountId: (payload && payload.accountId) || null,
            intentId: (payload && payload.intentId) || null,
            workerId,
            timestamp: new Date().toISOString(),
            bedrockContext: null,
            payload: payload || {},
        });
        return { accepted: true, signalEmitted: emissionType, error: null };
    }

    // Enrich with bedrock context where applicable
    let bedrockContext = null;
    if (translation.enrichWith) {
        bedrockContext = _enrichSignal(emissionType, payload, translation.enrichWith);
    }

    const signal = {
        signalId: _makeSignalId(),
        signal: translation.signal,
        level: translation.level || 'INFO',
        accountId: (payload && payload.accountId) || null,
        intentId: (payload && payload.intentId) || null,
        workerId,
        timestamp: new Date().toISOString(),
        bedrockContext,
        payload: payload || {},
    };

    _pendingSignals.push(signal);

    return { accepted: true, signalEmitted: translation.signal, error: null };
}

/**
 * Normalizer worker emission — routes normalized rows to DB_WRITE_REQUESTED
 * and emits normalization signals.
 *
 * @param {string} workerId — 'normalizer-worker'
 * @param {object} outcome — normalizer outcome { accountId, rowCount, schemaConformance, rows }
 * @returns {{ accepted: boolean, rowsWritten: number, signalEmitted: string | null }}
 */
function emitNormalized(workerId, outcome) {
    if (!outcome || !outcome.accountId) {
        return { accepted: false, rowsWritten: 0, signalEmitted: null };
    }

    const { accountId, rowCount, schemaConformance, rows } = outcome;
    let signalEmitted = null;

    if (rows && rows.length > 0) {
        // Route to governance dispatch if available (injected at runtime)
        // The DB write is performed by the caller; here we just signal readiness
        _pendingSignals.push({
            signalId: _makeSignalId(),
            signal: 'DB_WRITE_READY',
            level: 'INFO',
            accountId,
            intentId: null,
            workerId,
            timestamp: new Date().toISOString(),
            bedrockContext: null,
            payload: {
                table: 'instagram_media',
                operation: 'batch_upsert_insights',
                rowCount,
                rows,
            },
        });
        signalEmitted = 'DB_WRITE_READY';
    }

    if (schemaConformance && schemaConformance.driftDetected) {
        _pendingSignals.push({
            signalId: _makeSignalId(),
            signal: 'PAYLOAD_DEGRADATION',
            level: schemaConformance.rejectedItems > schemaConformance.totalItems * 0.5
                ? 'ERROR' : 'WARN',
            accountId,
            intentId: null,
            workerId,
            timestamp: new Date().toISOString(),
            bedrockContext: null,
            payload: { schemaConformance },
        });
    }

    return { accepted: true, rowsWritten: rowCount || 0, signalEmitted };
}

/**
 * Drain pending signals. Called by the FSM to consume all buffered signals.
 *
 * @returns {object[]} array of signal objects
 */
function getPendingSignals() {
    return _pendingSignals.splice(0, _pendingSignals.length);
}

// ═══════════════════════════════════════════════════════════════════════════
// EMISSION → SIGNAL TRANSLATION TABLE
// ═══════════════════════════════════════════════════════════════════════════

const EMISSION_TRANSLATION = {
    // ── Acquisition worker emissions ─────────────────────────────
    ACQUISITION_STARTED: {
        signal: 'ACQUISITION_STARTED',
        level: 'INFO',
    },
    ACQUISITION_COMPLETED: {
        signal: 'ACQUISITION_COMPLETED',
        level: 'INFO',
        enrichWith: ['telemetry'],
    },
    ACQUISITION_FAILED: {
        signal: 'ACQUISITION_FAILED',
        level: 'ERROR',
        enrichWith: ['classification', 'retryability', 'backoff'],
    },
    ACQUISITION_BLOCKED: {
        signal: 'ACQUISITION_BLOCKED',
        level: 'WARN',
    },
    ACQUISITION_PARTIAL: {
        signal: 'ACQUISITION_PARTIAL',
        level: 'WARN',
        enrichWith: ['classification'],
    },
    CURSOR_DEGRADATION: {
        signal: 'CURSOR_DEGRADATION',
        level: 'ERROR',
    },
    PAYLOAD_DEGRADATION: {
        signal: 'PAYLOAD_DEGRADATION',
        level: 'ERROR',
    },
    SYNC_GAP: {
        signal: 'SYNC_GAP',
        level: 'WARN',
    },
    CHECKPOINT_MILESTONE: {
        signal: 'CHECKPOINT_WRITTEN',
        level: 'INFO',
    },
    QUOTA_THROTTLE: {
        signal: 'QUOTA_THROTTLE',
        level: 'WARN',
        enrichWith: ['quota', 'rateLimit'],
    },
    // ── Normalizer worker emissions ──────────────────────────────
    NORMALIZATION_STARTED: {
        signal: 'NORMALIZATION_STARTED',
        level: 'INFO',
    },
    NORMALIZATION_COMPLETED: {
        signal: 'NORMALIZATION_COMPLETED',
        level: 'INFO',
    },
    NORMALIZATION_FAILED: {
        signal: 'NORMALIZATION_FAILED',
        level: 'ERROR',
        enrichWith: ['classification'],
    },
    SCHEMA_DRIFT: {
        signal: 'SCHEMA_DRIFT',
        level: 'WARN',
    },
    SCHEMA_REJECTION: {
        signal: 'SCHEMA_REJECTION',
        level: 'ERROR',
    },
    NORMALIZATION_ERROR: {
        signal: 'NORMALIZATION_ERROR',
        level: 'ERROR',
    },
};

// ═══════════════════════════════════════════════════════════════════════════
// SIGNAL ENRICHMENT
// ═══════════════════════════════════════════════════════════════════════════

function _makeSignalId() {
    return require('crypto').randomUUID();
}

/**
 * Enrich a signal with bedrock analysis context.
 * Called during emit() when the translation table specifies enrichWith.
 */
function _enrichSignal(emissionType, payload, enrichKeys) {
    const context = {};

    for (const key of enrichKeys) {
        try {
            switch (key) {
                case 'classification':
                    if (payload.error || payload.classification) {
                        const rawErr = payload.error
                            ? new Error(typeof payload.error === 'string' ? payload.error : 'worker_error')
                            : null;
                        if (rawErr && payload.response) rawErr.response = payload.response;
                        if (rawErr || payload.classification) {
                            context.classification = payload.classification
                                || _classify(rawErr || new Error('unknown'), 'insights:bridge', 'ig-graph');
                        }
                    }
                    break;

                case 'retryability':
                    if (context.classification) {
                        context.retryability = _analyzeRetryability(context.classification,
                            { retryAfterMs: null }, {});
                    }
                    break;

                case 'backoff':
                    if (context.classification) {
                        context.backoff = _generateAdaptiveCadence(
                            context.classification,
                            { retryAfterMs: null },
                            { pressureLevel: 'MEDIUM' },
                            { priority: 'LOW' },
                            { attemptN: 1 }
                        );
                    }
                    break;

                case 'quota':
                    if (payload.quotaHeaders) {
                        const synthNorm = _buildNormalizedFromHeaders(payload.quotaHeaders);
                        context.quotaPressure = _analyzeQuota(synthNorm, {}).pressureLevel;
                    }
                    break;

                case 'rateLimit':
                    if (context.classification) {
                        context.rateLimit = _analyzeRateLimit(
                            { headers: { retryAfter: null } },
                            context.classification,
                            {}
                        );
                    }
                    break;

                case 'telemetry':
                    context.telemetry = payload.telemetry || null;
                    break;

                default:
                    break;
            }
        } catch (err) {
            // Enrichment failure is non-fatal — emit what we have
            context._enrichmentError = { key, message: err.message };
        }
    }

    return Object.keys(context).length > 0 ? context : null;
}

// ═══════════════════════════════════════════════════════════════════════════
// EXPORTS
// ═══════════════════════════════════════════════════════════════════════════

module.exports = {
    // Phase A — signal-emitting substrate
    assess,
    REFRESH_POLICY,
    MAX_BATCH_SIZES,
    SIGNAL_DEFINITIONS,

    // Phase A — helpers (exported for testing)
    _buildNormalizedFromHeaders,
    _buildPreflightClassified,
    _makeSignal,

    // Phase B — substrate bridge
    emit,
    emitNormalized,
    getPendingSignals,
    EMISSION_TRANSLATION,

    // Backward compat
    fetch,
    persist,
};
