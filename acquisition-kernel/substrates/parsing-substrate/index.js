// substrates/parsing/index.js
// Parsing substrate: async parsing job dispatcher.
//
// Owns: dispatching parse→normalize→persist jobs per domain.
// Does NOT own: fetch, transport, orchestration, governance policy.
//
// Workers run asynchronously via setImmediate. dispatch() returns immediately.
// The retry-cadence worker should NOT await the result — it emits PARSING_DISPATCHED
// to CK and continues. When the worker completes, it emits PARSING_COMPLETE.
//
// Constitutional rule: domain→worker binding is owned by substrate-registry.
// This file MUST NOT maintain a sibling domain map. It calls
// substrateRegistry.getParsingWorker(domain) to resolve.

const substrateRegistry = require('../../substrate-registry');
const crypto = require('crypto');

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

  const worker = substrateRegistry.getParsingWorker(domain);
  if (!worker) {
    _emitComplete(jobId, accountId, domain, intentId, { status: 'failed', count: 0, error: `unknown domain: ${domain}` });
    return { jobId, status: 'pending' };
  }

  // Run async — fire and forget
  setImmediate(async () => {
    try {
      const result = await worker.execute(rawData, accountId, intentId, extra, _governance);
      _emitComplete(jobId, accountId, domain, intentId, { status: 'completed', count: result.count || 0 });
    } catch (err) {
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

module.exports = { dispatch, setGovernance };
