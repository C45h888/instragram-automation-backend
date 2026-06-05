// substrates/parsing/index.js
// Parsing substrate: async parsing job dispatcher.
//
// Owns: dispatching parse→normalize→persist jobs per domain,
//        tracking job state (pending/completed/failed).
// Does NOT own: fetch, transport, orchestration, governance policy.
//
// Workers run asynchronously via setImmediate. dispatch() returns immediately.
// The retry-worker should NOT await the result — it emits PARSING_DISPATCHED
// to CK and continues. When the worker completes, it emits PARSING_COMPLETE.

const { getWorker } = require('./domain-map');
const crypto = require('crypto');

// ── Job state ─────────────────────────────────────────────────────────────────
const _jobs = new Map(); // jobId → { domain, accountId, intentId, status, result, createdAt }

// ── Governance reference (set by orchastrator.js at boot) ────────────────────
let _governance = null;

/**
 * Set the governance reference for CK event emission.
 * Called by orchastrator.js during boot sequence.
 * @param {object} governance — constitutional kernel module (has .dispatch())
 */
function setGovernance(governance) {
  _governance = governance;
}

/**
 * Dispatch a parsing job for raw data from a domain fetch.
 * Runs async — call returns immediately with jobId.
 *
 * @param {string} domain — 'comments' | 'messages' | 'ugc' | 'insights' | 'media'
 * @param {object} rawData — raw transport response
 * @param {string} accountId
 * @param {string} intentId
 * @param {object} [extra] — { igUserId, pageId, pageToken, credentials }
 * @returns {{ jobId: string, status: 'pending' }}
 */
function dispatch(domain, rawData, accountId, intentId, extra = {}) {
  const jobId = crypto.randomUUID();
  const now = Date.now();

  _jobs.set(jobId, {
    domain, accountId, intentId,
    status: 'pending',
    result: null,
    createdAt: now,
  });

  const worker = getWorker(domain);
  if (!worker) {
    _jobs.set(jobId, { ..._jobs.get(jobId), status: 'failed', result: { count: 0, error: `unknown domain: ${domain}` } });
    _emitComplete(jobId, accountId, domain, intentId, { status: 'failed', count: 0, error: `unknown domain: ${domain}` });
    return { jobId, status: 'pending' };
  }

  // Run async — fire and forget
  setImmediate(async () => {
    try {
      const result = await worker.execute(rawData, accountId, intentId, extra, _governance);
      _jobs.set(jobId, { ..._jobs.get(jobId), status: 'completed', result });
      _emitComplete(jobId, accountId, domain, intentId, { status: 'completed', count: result.count || 0 });
    } catch (err) {
      _jobs.set(jobId, { ..._jobs.get(jobId), status: 'failed', result: { count: 0, error: err.message } });
      _emitComplete(jobId, accountId, domain, intentId, { status: 'failed', count: 0, error: err.message });
    }
  });

  return { jobId, status: 'pending' };
}

/**
 * Emit PARSING_COMPLETE to CK when worker finishes.
 */
function _emitComplete(jobId, accountId, domain, intentId, result) {
  if (!_governance || typeof _governance.dispatch !== 'function') return;
  try {
    _governance.dispatch({
      type: 'PARSING_COMPLETE',
      accountId, domain, intentId, jobId,
      result,
    });
  } catch (err) {
    console.error('[parsing] Failed to emit PARSING_COMPLETE:', err.message);
  }
}

/**
 * Get job state by ID.
 * @param {string} jobId
 * @returns {{ status: string, result: object|null }|null}
 */
function getJob(jobId) {
  const job = _jobs.get(jobId);
  if (!job) return null;
  return { status: job.status, result: job.result };
}

/**
 * Get job statistics.
 * @returns {{ pending: number, completed: number, failed: number }}
 */
function getStats() {
  let pending = 0, completed = 0, failed = 0;
  for (const job of _jobs.values()) {
    if (job.status === 'pending') pending++;
    else if (job.status === 'completed') completed++;
    else failed++;
  }
  return { pending, completed, failed };
}

module.exports = { dispatch, getJob, getStats, setGovernance };
