// substrates/webhook-acquisition-substrate/index.js
// Webhook Acquisition Substrate: the substrate-level entry point for
// Meta Instagram webhooks. Lives in the acquisition kernel.
//
// Owns: routing the Meta webhook payload to the right bounded worker,
//       generating intentIds, fire-and-forget async dispatch so Meta
//       gets a 200 fast. Pure orchestration — no IG-specific knowledge.
// Does NOT own: payload shape validation (worker), normalization
//               (substrate normalizer), failure classification (bedrock),
//               DB writes (Phase 2), state persistence (FSM).
//
// Bounded workers (one per event type) live alongside:
//   ./workers/messages-worker.js
//   ./workers/comments-worker.js
//   ./workers/mentions-worker.js
//   ./workers/story-mentions-worker.js
//
// Mounted on: substrates/ig-reliability-substrate.js (analyzeFailure).
//
// Phase 1: workers normalize, classify failure (if any), and dispatch
//          WEBHOOK_EVENT_RECEIVED into CK. The acquisition-fsm holds the
//          canonical event. No DB write path yet.

const crypto = require('crypto');

// ── Worker imports (one per event type) ────────────────────────────────────
const messagesWorker       = require('./workers/messages-worker');
const commentsWorker       = require('./workers/comments-worker');
const mentionsWorker       = require('./workers/mentions-worker');
const storyMentionsWorker  = require('./workers/story-mentions-worker');

// ── Governance reference (set by orchastrator at boot) ─────────────────────
let _governance = null;

function setGovernance(governance) {
  _governance = governance;
}

function getGovernance() {
  return _governance;
}

// ── Intent id generator (substrate-owned) ──────────────────────────────────

function _newIntentId(prefix) {
  return `${prefix || 'webhook'}-${Date.now()}-${crypto.randomBytes(4).toString('hex')}`;
}

// ── Routing helpers (semantic separation: pure dispatch, no logic) ──────────

/**
 * Route one entry to the right worker based on the entry's shape.
 * @returns {Promise<{ processed: number, discarded: number, intentIds: string[] }>}
 */
async function _processEntry(entry, accountId) {
  const results = [];
  const intentIds = [];
  let processed = 0;
  let discarded = 0;

  if (!entry || typeof entry !== 'object') {
    return { processed, discarded, intentIds, results: [{ status: 'discarded', reason: 'entry_not_object' }] };
  }

  // ── DM path: entry.messaging[] ────────────────────────────────────────
  if (Array.isArray(entry.messaging) && entry.messaging.length > 0) {
    for (const item of entry.messaging) {
      const intentId = _newIntentId('messaging');
      const r = await messagesWorker.execute(item, accountId, intentId, _governance);
      results.push(r);
      if (r.status === 'staged') {
        processed++;
        intentIds.push(intentId);
      } else {
        discarded++;
      }
    }
    return { processed, discarded, intentIds, results };
  }

  // ── Changes path: entry.changes[] (comments / mentions / story_mentions) ─
  if (Array.isArray(entry.changes) && entry.changes.length > 0) {
    for (const change of entry.changes) {
      const worker = _resolveChangesWorker(change?.field);
      if (!worker) {
        results.push({
          status: 'discarded',
          reason: `unsupported_change_field:${change?.field}`,
        });
        discarded++;
        continue;
      }
      const intentId = _newIntentId(change.field);
      const r = await worker.execute(change, accountId, intentId, _governance);
      results.push(r);
      if (r.status === 'staged') {
        processed++;
        intentIds.push(intentId);
      } else {
        discarded++;
      }
    }
    return { processed, discarded, intentIds, results };
  }

  // ── Unrecognized entry shape ─────────────────────────────────────────
  return {
    processed: 0,
    discarded: 1,
    intentIds: [],
    results: [{ status: 'discarded', reason: 'no_messaging_or_changes_in_entry' }],
  };
}

function _resolveChangesWorker(field) {
  switch (field) {
    case 'comments':       return commentsWorker;
    case 'mentions':       return mentionsWorker;
    case 'story_mentions': return storyMentionsWorker;
    default:               return null;
  }
}

// ── Public entry point (called by routes/webhook.js) ──────────────────────

/**
 * Process a Meta Instagram webhook payload. Returns a routing summary;
 * the heavy work runs async via setImmediate so Meta gets a 200 fast.
 *
 * @param {object} payload  — the parsed Meta webhook body
 * @param {string} accountId — the IG account id this webhook is for
 * @returns {{ accepted: number, dropped: number, entries: number }}
 */
function processWebhook(payload, accountId) {
  if (!payload || typeof payload !== 'object') {
    return { accepted: 0, dropped: 0, entries: 0, reason: 'payload_not_object' };
  }
  if (payload.object !== 'instagram') {
    return { accepted: 0, dropped: 0, entries: 0, reason: 'not_instagram_object' };
  }
  if (!Array.isArray(payload.entry) || payload.entry.length === 0) {
    return { accepted: 0, dropped: 0, entries: 0, reason: 'empty_entry' };
  }

  const entries = payload.entry;
  const totalEntries = entries.length;
  let totalAccepted = 0;
  let totalDropped = 0;

  // Fire-and-forget per entry. Meta must see a 200 within ~10s; per-entry
  // work is bounded but the substrate does not block the response.
  for (const entry of entries) {
    setImmediate(async () => {
      try {
        const { processed, discarded, intentIds } = await _processEntry(entry, accountId);
        totalAccepted += processed;
        totalDropped += discarded;

        // Phase 2: eagerly fire PERSIST_STAGED_EVENT for each staged
        // canonical event. The FSM hydrates → resolves → emits
        // DB_WRITE_REQUESTED → persist-telemetry-fsm → writers.
        if (_governance && typeof _governance.dispatch === 'function') {
          for (const intentId of intentIds) {
            const dispatchResult = _governance.dispatch({
              type: 'PERSIST_STAGED_EVENT',
              accountId,
              intentId,
              eventId: intentId, // substrate owns both at this point
            });
            if (process.env.WEBHOOK_DEBUG && dispatchResult && typeof dispatchResult.then === 'function') {
              dispatchResult.then((r) => {
                console.log('[substrate] PERSIST_STAGED_EVENT result:', r?.allowed, r?.reason);
              });
            }
          }
        }
      } catch (err) {
        // Defensive: a worker throwing past its own _emitFailure guard
        // would otherwise be silent. Log + emit a discard signal.
        try {
          if (_governance && typeof _governance.dispatch === 'function') {
            _governance.dispatch({
              type: 'WEBHOOK_PROCESS_FAILED',
              accountId,
              reason: `substrate_threw:${err.message}`,
            });
          }
        } catch (_) {}
      }
    });
  }

  return {
    accepted: 0,        // not synchronously known — work runs async
    dropped: 0,
    entries: totalEntries,
    asyncDispatched: true,
  };
}

module.exports = {
  setGovernance,
  getGovernance,
  processWebhook,
  /**
   * Wire the substrate into the constitutional flow. Subscribes to
   * DB_WRITE_REQUESTED actions emitted by the acquisition-fsm and
   * re-dispatches them through CK so they route to the
   * persist-telemetry-fsm via DOMAIN_EVENT_MAP. This closes the
   * cross-kernel write loop.
   */
  wireConstitutionalBridge,
};

// ── Constitutional bridge: acquisition-fsm → persist-telemetry-fsm ─────────
// The acquisition-fsm emits DB_WRITE_REQUESTED as an action. CK's
// _emitActions routes to subscribers, but the persist-telemetry-fsm
// is a domain (not a subscriber). This bridge subscribes to the
// action and re-dispatches through CK so DOMAIN_EVENT_MAP routes it.
let _bridgedGovernance = null;

function wireConstitutionalBridge(governance) {
  _bridgedGovernance = governance;
  if (!governance || typeof governance.subscribeAction !== 'function') return;
  governance.subscribeAction('DB_WRITE_REQUESTED', (action) => {
    if (!action || !action.type) return;
    // Only re-dispatch writes that originated from the acquisition-fsm
    // (via the webhook acquisition substrate). Other DB_WRITE_REQUESTED
    // actions have different lineageDomain values.
    if (action.lineageDomain !== 'acquisition-fsm') return;
    if (process.env.WEBHOOK_DEBUG) {
      console.log('[bridge] re-dispatching DB_WRITE_REQUESTED:', action.table, 'rows:', action.rows?.length);
    }
    if (typeof _bridgedGovernance.dispatch === 'function') {
      _bridgedGovernance.dispatch({
        type: 'DB_WRITE_REQUESTED',
        accountId: action.accountId,
        intentId: action.intentId,
        table: action.table,
        operation: action.operation,
        rows: action.rows,
        lineageId: action.lineageId,
        lineageDomain: action.lineageDomain,
        domain: action.domain || 'persist-telemetry',
        extra: action.extra,
      });
    }
  });
}
