// substrates/webhook-acquisition-substrate/intake.js
// Thin classifier: routes one Meta webhook entry to the right bounded worker.
//
// Owns: pure dispatch — payload slice → worker selection → execute.
// Does NOT own: signature verification, intentId generation, persistence
//               trigger, setImmediate timing, governance calls.
//
// Each entry from Meta carries either:
//   entry.messaging[]  → DM events (messages-worker)
//   entry.changes[]    → comment/mention/story-mention events (per field)
//
// This module is PURE — no async timing, no governance, no state.
// It returns results to the caller (the substrate) which owns the fire-and-
// forget timing and the persistence trigger.

const { newIntentId } = require('./intent-id');

// ── Worker imports (one per event type) ────────────────────────────────────
const messagesWorker        = require('./workers/messages-worker');
const commentsWorker        = require('./workers/comments-worker');
const mentionsWorker        = require('./workers/mentions-worker');
const storyMentionsWorker   = require('./workers/story-mentions-worker');
const commentRepliesWorker  = require('./workers/comment-replies-worker');
const liveCommentsWorker    = require('./workers/live-comments-worker');
const messageReactionsWorker= require('./workers/message-reactions-worker');
const messageSeenWorker     = require('./workers/message-seen-worker');
const standbyWorker         = require('./workers/standby-worker');
const mediaPublishWorker    = require('./workers/media-publish-worker');
const tagsWorker            = require('./workers/tags-worker');

// ── Field → worker map ─────────────────────────────────────────────────────

const _CHANGES_WORKER_MAP = {
  comments:         commentsWorker,
  mentions:         mentionsWorker,
  story_mentions:   storyMentionsWorker,
  live_comments:    liveCommentsWorker,
  message_reactions:messageReactionsWorker,
  message_seen:     messageSeenWorker,
  standby:          standbyWorker,
  media:            mediaPublishWorker,
  tags:             tagsWorker,
};

function _resolveChangesWorker(change) {
  if (!change || typeof change !== 'object') return null;
  // Comment replies arrive as field='comments' with value.parent_id set.
  // Route those to the reply worker; everything else routes by field name.
  if (change.field === 'comments' && change.value && change.value.parent_id) {
    return commentRepliesWorker;
  }
  return _CHANGES_WORKER_MAP[change.field] || null;
}

// ── Entry classification ────────────────────────────────────────────────────

/**
 * Classify an entry and return its dispatchable items.
 *
 * @param {object} entry — single entry from payload.entry[]
 * @returns {{ kind: string, items: Array }}
 *   kind: 'messaging' | 'changes' | 'unknown'
 *   items: array of { item, worker, intentId, domain }
 */
function classifyEntry(entry) {
  if (!entry || typeof entry !== 'object') {
    return { kind: 'unknown', items: [] };
  }

  // ── DM path ───────────────────────────────────────────────────────────
  if (Array.isArray(entry.messaging) && entry.messaging.length > 0) {
    return {
      kind: 'messaging',
      items: entry.messaging.map(item => ({
        item,
        worker: messagesWorker,
        intentId: newIntentId('messaging', entry.id || null),
        domain: 'webhook:messages',
      })),
    };
  }

  // ── Changes path ───────────────────────────────────────────────────────
  if (Array.isArray(entry.changes) && entry.changes.length > 0) {
    return {
      kind: 'changes',
      items: entry.changes.map(change => {
        const worker = _resolveChangesWorker(change);
        // Domain is derived from the worker (resolves comment-replies vs
        // comments disambiguation); falls back to the raw field name when
        // no worker is bound.
        const domain = worker
          ? _workerDomain(worker)
          : `webhook:unknown:${change?.field || 'unknown'}`;
        return {
          item: change,
          worker: worker || null,
          intentId: newIntentId(change?.field || 'unknown', entry.id || null),
          domain,
        };
      }),
    };
  }

  return { kind: 'unknown', items: [] };
}

// ── Worker → domain mapping (reversed once at module load) ────────────────

const _WORKER_DOMAIN_MAP = new Map([
  [commentsWorker,        'webhook:comments'],
  [commentRepliesWorker,  'webhook:comment-replies'],
  [mentionsWorker,        'webhook:mentions'],
  [storyMentionsWorker,   'webhook:story-mentions'],
  [liveCommentsWorker,    'webhook:live-comments'],
  [messageReactionsWorker,'webhook:message-reactions'],
  [messageSeenWorker,     'webhook:message-seen'],
  [standbyWorker,         'webhook:standby'],
  [mediaPublishWorker,    'webhook:media-publish'],
  [tagsWorker,            'webhook:tags'],
]);

function _workerDomain(worker) {
  return _WORKER_DOMAIN_MAP.get(worker) || 'webhook:unknown';
}

// ── Per-entry processor (called by the substrate's setImmediate callback) ───

/**
 * Process one entry: classify it, dispatch each item to the matching worker,
 * collect results.
 *
 * @param {object} entry     — single entry from payload.entry[]
 * @param {string} accountId — IG account id (from entry.id)
 * @param {object} governance — CK reference (for worker.execute)
 * @returns {Promise<{ processed: number, discarded: number, intentIds: string[], results: object[] }>}
 */
async function processEntry(entry, accountId, governance) {
  const { kind, items } = classifyEntry(entry);

  if (kind === 'unknown') {
    return { processed: 0, discarded: 1, intentIds: [], results: [{ status: 'discarded', reason: 'entry_not_classified' }] };
  }

  let processed = 0;
  let discarded = 0;
  const intentIds = [];
  const results = [];

  for (const { item, worker, intentId, domain } of items) {
    // Unsupported field — no worker
    if (!worker) {
      results.push({ status: 'discarded', reason: `unsupported_change_field:${domain.split(':').pop()}`, intentId });
      discarded++;
      continue;
    }

    // Worker execute — may throw; the caller (substrate) owns the try/catch
    let result;
    try {
      result = await worker.execute(item, accountId, intentId, governance);
    } catch (err) {
      // Worker threw past its own _emitFailure guard — treat as discarded
      result = { status: 'discarded', eventId: null, eventType: null, reason: `worker_threw:${err.message}` };
    }

    results.push({ ...result, intentId, domain });
    if (result.status === 'staged') {
      processed++;
      intentIds.push(intentId);
    } else {
      discarded++;
    }
  }

  return { processed, discarded, intentIds, results };
}

module.exports = { classifyEntry, processEntry };