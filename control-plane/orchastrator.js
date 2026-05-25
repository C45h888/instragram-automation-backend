// control-plane/orchastrator.js
// Orchestrator: deterministic composition root.
//
// Owns: wiring runtime modules together, routing governance authority
//        downward and runtime signals upward.
// Does NOT own: mechanics of any individual module — delegates everything.
// Does NOT own: governance legality — delegates to governance-kernel.
//
// Architecture (invariant: signals ↑, authority ↓):
//
//   Cadence (90s) ──► governance.dispatch(CADENCE_TICK)
//                          │
//                          └── Kernel evaluates → emits [SCAN_DATABASE, REFRESH_LIFECYCLE, CHECK_SAFETY, REPORT_METRICS]
//                               └── Orchestrator executes actions, dispatches results back
//
//   signalIntake ──► buffer.ingest()  +  governance.dispatch(BUFFER_EVENT_INGESTED)
//   buffer.onFlush ──► governance.dispatch(BUFFER_FLUSH_READY)
//                          │
//                          ├── HSM transition  (BUFFERING → EVALUATING)
//                          └── onAction(EVALUATE) → executeEvaluationPipeline()
//                               ├── evaluator.evaluate()
//                               ├── emitter.emitMutation()
//                               └── emitter.emit()
//                                    └── governance.dispatch(EMISSION_OBSERVATION)
//
//   Sync substrate: START_INTENT_DISCOVERY → poll Redis → ACQUISITION_INTENT_RECEIVED → Kernel → EXECUTE_ACQUISITION
//
// Metrics substrate rehydrates from Redis on boot for crash-survival.
//
// This module is the SINGLE place where modules are wired together.
// No module imports another module directly — all wiring lives here.

const { getRedisClient } = require('../config/redis');
const governance = require('./governance/governance-kernel');
const executionBridge = require('./execution-bridge');
const metricsSubstrate = require('../substrates/metrics-substrate');
const syncSubstrate = require('../substrates/sync-substrate');
const dbWorker = require('./execution/db-worker');
const signalIntake = require('./runtime/signal-intake');
const buffer = require('./runtime/buffer');
const cadence = require('./runtime/cadence');
const evaluator = require('./runtime/evaluation');
const emitter = require('./runtime/emission');
const lifecycle = require('./runtime/lifecycle');
const safety = require('./runtime/operational-safety');
const dbScanner = require('./runtime/db-scanner');
const persistence = require('../substrates/persistence');

// IG Fetchers — pure transport, per domain. No DB writes. No orchestration.
const igFetcherComments = require('./execution/ig-fetcher-comments');
const igFetcherMessages = require('./execution/ig-fetcher-messages');
const igFetcherUgc     = require('./execution/ig-fetcher-ugc');
const igFetcherInsights = require('./execution/ig-fetcher-insights');
const igFetcherMedia   = require('./execution/ig-fetcher-media');
const igFetcherPublish = require('./execution/ig-fetcher-publish');

const REFRESH_INTERVAL_MS = 90 * 1000; // 90s cadence
const DEBOUNCE_MS = 500;
const GOVERNANCE_TICK_MS = 10_000; // 10s watchdog tick

// ── Domain routing: maps domain → { fetch, persist } ─────────────────────────
// Used by executeGovernedAcquisition() to route intents to the correct
// IG fetcher (pure transport) and persistence function (pure DB write).

const DOMAIN_ROUTING = {
  comments: {
    fetch: (accountId, params, creds) => {
      if (params.media_id) {
        return igFetcherComments.fetchComments(accountId, params.media_id, params.limit, creds);
      }
      return igFetcherComments.fetchRecentMediaComments(accountId, params.maxPosts, params.limit, creds);
    },
    persist: (accountId, rawData) => {
      if (rawData.batches) {
        return persistence.storeCommentBatches(accountId, rawData.batches);
      }
      if (rawData.records) {
        return persistence.storeCommentBatches(accountId, [{ mediaId: 'direct', comments: rawData.records }]);
      }
      return { count: 0 };
    },
  },
  messages: {
    fetch: (accountId, params, creds) => {
      if (params.conversation_id) {
        return igFetcherMessages.fetchMessages(accountId, params.conversation_id, params.limit, creds);
      }
      return igFetcherMessages.fetchConversations(accountId, params.convLimit || params.limit, creds);
    },
    persist: async (accountId, rawData) => {
      if (rawData.rawMessages) {
        const stored = await persistence.storeMessageBatches(
          accountId, [{ conversationId: rawData.conversationId || 'direct', rawMessages: rawData.rawMessages }],
          rawData.igUserId, rawData.pageId, null
        );
        return { count: stored?.count || rawData.count || 0 };
      }
      if (rawData.rawConversations) {
        const stored = await persistence.storeConversationBatches(
          accountId, rawData.rawConversations, rawData.igUserId, rawData.pageId
        );
        return { count: stored?.count || rawData.count || 0 };
      }
      return { count: 0 };
    },
  },
  ugc: {
    fetch: (accountId, params, creds) => {
      if (params.hashtag) {
        return igFetcherUgc.fetchHashtagMedia(accountId, params.hashtag, params.limit, creds);
      }
      return igFetcherUgc.fetchTaggedMedia(accountId, params.limit, creds);
    },
    persist: async (accountId, rawData) => {
      if (!rawData.records || rawData.records.length === 0) return { count: 0 };
      const { mapRawPostToUgcContent } = require('../substrates/normalization');
      const source = rawData.cleanHashtag ? 'hashtag' : 'tagged';
      const records = rawData.records
        .filter(p => p.id)
        .map(p => mapRawPostToUgcContent(p, accountId, source, rawData.cleanHashtag || null));
      await persistence.storeUgcContentBatch(records);
      return { count: records.length };
    },
  },
  insights: {
    fetch: async (accountId, params, creds) => {
      const sevenDaysAgo = params.since || Math.floor((Date.now() - 7 * 24 * 3600000) / 1000);
      const now = params.until || Math.floor(Date.now() / 1000);
      const feedResult = await igFetcherInsights.fetchMediaFeed(accountId, sevenDaysAgo, now, creds);
      if (!feedResult.success) return feedResult;
      const insights = await igFetcherInsights.fetchMediaInsightsBatch(feedResult.mediaList, creds.pageToken);
      return { success: true, insights, mediaList: feedResult.mediaList, _usagePct: feedResult._usagePct };
    },
    persist: async (accountId, rawData) => {
      if (!rawData.insights || rawData.insights.length === 0) return { count: 0 };
      const captions = (rawData.mediaList || []).map(m => m.caption).filter(Boolean);
      await persistence.storeMediaInsightsBatch(accountId, rawData.insights, captions);
      return { count: rawData.insights.length };
    },
  },
  media: {
    fetch: (accountId, params) => igFetcherMedia.fetchBusinessPosts(accountId, params.limit),
    persist: (accountId, rawData) => {
      if (!rawData.posts || rawData.posts.length === 0) return { count: 0 };
      return persistence.storeBusinessPosts(accountId, rawData.posts);
    },
  },
  'publish:media': {
    fetch: async (accountId, params, creds) => {
      const { action_type, payload, queue_row_id, scheduled_post_id, intent_type } = params;
      const actionType = action_type || 'publish_post';

      if (queue_row_id) {
        await dbWorker.markPostQueueProcessing(queue_row_id, 'pending');
      }

      let resolvedPayload = payload || params;
      if (intent_type === 'scheduled_post' && resolvedPayload?.asset_id) {
        const asset = await dbWorker.resolveAsset(resolvedPayload.asset_id);
        if (!asset?.storage_path) {
          if (scheduled_post_id) {
            await dbWorker.markScheduledPostFailed(scheduled_post_id);
          }
          return { success: false, count: 0, error: 'Asset not found', retryable: false, error_category: 'permanent' };
        }
        resolvedPayload = { image_url: asset.storage_path, caption: asset.caption || '', media_type: asset.media_type || 'IMAGE', scheduled_post_id };
      }

      return igFetcherPublish.executePublishAction(actionType, accountId, creds, resolvedPayload);
    },
    persist: async (accountId, rawData, execParams) => {
      const { queue_row_id, scheduled_post_id } = execParams || {};
      if (queue_row_id && rawData.instagram_id) {
        await dbWorker.markPostQueueSent(queue_row_id, rawData.instagram_id);
      }
      if (scheduled_post_id && rawData.instagram_id) {
        await dbWorker.markScheduledPostPublished(scheduled_post_id, rawData.instagram_id);
      }
      return { count: 1 };
    },
  },
  'publish:ugc': {
    fetch: async (accountId, params, creds) => {
      const { queue_row_id } = params;
      if (queue_row_id) {
        await dbWorker.markPostQueueProcessing(queue_row_id, 'pending');
      }
      return igFetcherPublish.executePublishAction('repost_ugc', accountId, creds, params.payload || params);
    },
    persist: async (accountId, rawData, execParams) => {
      const { queue_row_id, payload } = execParams || {};
      if (queue_row_id && rawData.instagram_id) {
        await dbWorker.markPostQueueSent(queue_row_id, rawData.instagram_id);
      }
      if (payload?.permission_id && rawData.instagram_id) {
        await dbWorker.markUgcPermissionReposted(payload.permission_id, rawData.instagram_id);
      }
      return { count: 1 };
    },
  },
  'publish:messaging': {
    fetch: async (accountId, params, creds) => {
      const { queue_row_id, action_type } = params;
      if (queue_row_id) {
        await dbWorker.markPostQueueProcessing(queue_row_id, 'pending');
      }
      return igFetcherPublish.executePublishAction(action_type, accountId, creds, params.payload || params);
    },
    persist: async (accountId, rawData, execParams) => {
      const { queue_row_id } = execParams || {};
      if (queue_row_id && rawData.instagram_id) {
        await dbWorker.markPostQueueSent(queue_row_id, rawData.instagram_id);
      }
      return { count: 1 };
    },
  },
};

// ── Helpers ──────────────────────────────────────────────────────────────────

function isDegraded() {
  const state = governance.getState();
  return state.startsWith('DEGRADED.');
}

// ── Governed Acquisition Pipeline ────────────────────────────────────────────

/**
 * Execute an HSM-governed acquisition for a single intent.
 * Called by the governance action subscriber when EXECUTE_ACQUISITION is emitted.
 *
 * Flow:
 *   1. Dispatch ACQUISITION_EXECUTING → HSM transitions EVALUATING → EMITTING
 *   2. Call execution-bridge with domain-specific fetch + persist
 *   3. Report ACQUISITION_COMPLETE → HSM transitions EMITTING → IDLE
 *
 * @param {string} accountId
 * @param {string} domain - e.g. 'comments', 'messages', 'publish:media'
 * @param {string} intentId
 * @param {object} params - intent parameters
 */
async function executeGovernedAcquisition(accountId, domain, intentId, params) {
  const routing = DOMAIN_ROUTING[domain];
  if (!routing) {
    console.error(`[orchestrator] Unknown acquisition domain: ${domain}`);
    governance.dispatch({
      type: 'ACQUISITION_COMPLETE', accountId, domain, intentId,
      result: { status: 'failed', count: 0, error: `unknown domain: ${domain}` },
    });
    return;
  }

  governance.dispatch({ type: 'ACQUISITION_EXECUTING', accountId, domain, intentId });

  const outcome = await executionBridge.executeWithRetry(
    accountId, intentId, domain,
    async (acctId, execParams) => {
      const creds = await persistence.resolveAccountCredentials(acctId);
      const rawData = await routing.fetch(acctId, execParams, creds);
      if (!rawData.success) return rawData;
      const persistResult = await routing.persist(acctId, rawData, execParams);
      return {
        success: true,
        count: persistResult?.count || rawData.count || 0,
        _usagePct: rawData._usagePct,
        instagram_id: rawData.instagram_id,
      };
    },
    params
  );

  governance.dispatch({
    type: 'ACQUISITION_COMPLETE', accountId, domain, intentId,
    result: { status: outcome.status, count: outcome.count, error: outcome.error || null },
  });
}

/**
 * Write acquisition result to Redis for agent consumption.
 * Called by governance action subscriber when WRITE_ACQUISITION_RESULT is emitted.
 */
async function writeAcquisitionResult(accountId, domain, intentId, result) {
  const redis = getRedisClient();
  if (!redis || redis.status !== 'ready') return;

  const resultKey = `supervisor:acquisition_results:${accountId}:${intentId}`;
  try {
    await redis.set(resultKey, JSON.stringify({
      intent_id: intentId,
      account_id: accountId,
      domain,
      status: result.status,
      result: { count: result.count },
      error: result.error,
      completed_at: new Date().toISOString(),
    }), 'EX', 3600);
  } catch (err) {
    console.error(`[orchestrator] Failed to write result key ${resultKey}:`, err.message);
  }
}

// ── Wiring ───────────────────────────────────────────────────────────────────

/**
 * Execute the evaluation → mutation → emission pipeline for a single account.
 * Called by the governance action subscriber when a EVALUATE action is emitted.
 * After execution, reports completion back to governance.
 */
async function executeEvaluationPipeline(accountId, events) {
  const startTime = Date.now();
  try {
    const result = evaluator.evaluate(accountId, events);

    for (const mut of result.mutations) {
      await emitter.emitMutation(mut);
    }

    const emitResult = result.intents.length > 0
      ? await emitter.emit(result.intents)
      : { ok: true, error: null };

    governance.dispatch({
      type: 'EMISSION_OBSERVATION',
      status: result.intents.length === 0 ? 'empty' : (emitResult.ok ? 'ok' : 'error'),
      accountId,
      metadata: {
        intentCount: result.intents.length,
        mutationsApplied: result.mutations.length,
        reason: emitResult.error || null,
        latencyMs: Date.now() - startTime,
      },
    });
  } catch (err) {
    console.error(`[orchestrator] Evaluation pipeline error for ${accountId}:`, err.message);
    governance.dispatch({
      type: 'EMISSION_OBSERVATION',
      status: 'error',
      accountId,
      metadata: {
        intentCount: 0,
        mutationsApplied: 0,
        reason: err.message,
        latencyMs: Date.now() - startTime,
      },
    });
  }
}

/**
 * Wire all runtime modules together. Called once on startup.
 *
 * Architecture: runtime signals flow UPWARD (dispatch), governance authority
 * flows DOWNWARD (onAction). The orchestrator is the sole routing fabric.
 */
function _wire() {
  buffer.setDebounceMs(DEBOUNCE_MS);

  // Governance action subscriber — routes governance intents to runtime modules.
  governance.onAction((action) => {
    // Existing lifecycle actions
    if (action.type === 'EVALUATE') {
      executeEvaluationPipeline(action.accountId, action.events);
    }
    if (action.type === 'EXECUTE_ACQUISITION') {
      executeGovernedAcquisition(action.accountId, action.domain, action.intentId, action.params);
    }
    if (action.type === 'WRITE_ACQUISITION_RESULT') {
      writeAcquisitionResult(action.accountId, action.domain, action.intentId, action.result);
    }
    if (action.type === 'LOG_DEGRADED') {
      console.warn(`[orchestrator] Runtime DEGRADED.${action.substate}: ${action.reason}`);
    }
    if (action.type === 'LOG_RECOVERY') {
      console.warn(`[orchestrator] Runtime RECOVERY.${action.substate}`);
    }
    if (action.type === 'LOG_HALT') {
      console.error(`[orchestrator] Runtime HALTED: ${action.reason}`);
    }

    // Maintenance actions (from CADENCE_TICK kernel dispatch)
    if (action.type === 'SCAN_DATABASE') {
      dbScanner.runScan().then(r => {
        if (r.totalEmitted > 0) {
          console.log(`[orchestrator] DB scanner emitted ${r.totalEmitted} intents`);
        }
        governance.dispatch({ type: 'DATABASE_SCANNED', intentCount: r.totalEmitted });
      }).catch(err => {
        console.error('[orchestrator] DB scanner error:', err.message);
        governance.dispatch({ type: 'DATABASE_SCANNED', intentCount: 0 });
      });
    }
    if (action.type === 'REFRESH_LIFECYCLE') {
      lifecycle.refresh().then(() => {
        return persistence.getActiveAccounts();
      }).then(accounts => {
        governance.dispatch({ type: 'LIFECYCLE_REFRESHED', accountIds: accounts.map(a => a.id) });
      }).catch(err => {
        console.error('[orchestrator] Lifecycle refresh error:', err.message);
        governance.dispatch({ type: 'LIFECYCLE_REFRESHED', accountIds: [] });
      });
    }
    if (action.type === 'CHECK_SAFETY') {
      safety.runChecks().then(() => {
        governance.dispatch({ type: 'SAFETY_CHECK_COMPLETE' });
      }).catch(err => {
        console.error('[orchestrator] Safety check error:', err.message);
        governance.dispatch({ type: 'SAFETY_CHECK_COMPLETE' });
      });
    }
    if (action.type === 'REPORT_METRICS') {
      const signals = metricsSubstrate.getHealthSignals();
      governance.dispatch({
        type: 'WORKER_METRICS_REPORTED',
        total: signals.total,
        failed: signals.failed,
        failureRate: signals.failureRate,
        windowMs: signals.windowMs,
      });
    }

    // Sync substrate control
    if (action.type === 'START_INTENT_DISCOVERY') {
      syncSubstrate.start(getRedisClient(), governance);
    }
    if (action.type === 'STOP_INTENT_DISCOVERY') {
      syncSubstrate.stop();
    }
  });

  buffer.onFlush(async (accountId, events) => {
    governance.dispatch({ type: 'BUFFER_FLUSH_READY', accountId, events, eventCount: events.length });
  });

  lifecycle.onRemove((accountId) => {
    buffer.destroy(accountId);
  });
}

// ── Public API ───────────────────────────────────────────────────────────────

async function startAllWorkers() {
  console.log('[orchestrator] Starting governance kernel...');
  _wire();

  // Metrics substrate: rehydrate from Redis for crash-survival
  await metricsSubstrate.init();

  // 1. Signal intake: subscribe to realtime → signal bus.
  await signalIntake.start(null, (event) => {
    buffer.ingest(event);
    governance.dispatch({ type: 'BUFFER_EVENT_INGESTED', accountId: event.accountId });
  });

  // 2. Initial account discovery (orchestrator fetches, dispatches to kernel)
  await lifecycle.refresh();
  const accounts = await persistence.getActiveAccounts();
  governance.dispatch({ type: 'LIFECYCLE_REFRESHED', accountIds: accounts.map(a => a.id) });

  // 3. Boot complete — governance transitions BOOTING → HEALTHY.IDLE
  governance.dispatch({ type: 'BOOT_COMPLETE' });

  // 4. Start governance watchdog loop (10s tick — stale state detection only)
  governance.startLoop(GOVERNANCE_TICK_MS);

  // 5. Sync substrate starts polling when kernel is HEALTHY.IDLE
  // Kernel emits START_INTENT_DISCOVERY on IDLE entry, STOP on leave
  const redis = getRedisClient();
  if (redis && redis.status === 'ready') {
    syncSubstrate.start(redis, governance);
  }

  // 6. Cadence: 90-second maintenance loop — dumb signal only
  cadence.every(REFRESH_INTERVAL_MS, async () => {
    governance.dispatch({ type: 'CADENCE_TICK' });
  });

  console.log(`[orchestrator] Governance kernel running — ${accounts.length} account(s) — state: ${governance.getState()}`);
}

async function stopAllWorkers() {
  console.log('[orchestrator] Stopping governance kernel...');

  governance.stopLoop();
  syncSubstrate.stop();
  await cadence.stop();
  await signalIntake.stop();
  buffer.destroyAll();
  lifecycle.stopAll();

  console.log('[orchestrator] Governance kernel stopped');
}

module.exports = { startAllWorkers, stopAllWorkers };
