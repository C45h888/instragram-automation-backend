// tests/helpers/sync-barriers.js
// Deterministic synchronization barriers for constitutional test suites.
//
// Replaces nondeterministic sleep() calls with polling-based barriers
// that verify runtime conditions have been met before proceeding.
// Each barrier polls at 50ms intervals and throws with a descriptive
// message on timeout to prevent silent test passes from stale timing.
//
// Usage:
//   const { waitForLedgerEntryCount, waitForCursorAdvance, ... } = require('./helpers/sync-barriers');
//   await waitForLedgerEntryCount(15, 5000);

const DEFAULT_POLL_MS = 50;
const DEFAULT_TIMEOUT_MS = 5000;

/**
 * Poll until the Redis ledger reaches at least `target` entries.
 * Uses lineageLedger.getSize() which queries Redis llen.
 *
 * @param {number} target — minimum expected ledger entry count
 * @param {number} [timeoutMs=5000] — max time to wait in ms
 * @returns {Promise<number>} final ledger size
 */
async function waitForLedgerEntryCount(target, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const lineageLedger = require('../../control-plane/governance/lineage-ledger');
  const deadline = Date.now() + timeoutMs;
  let lastSize = 0;
  while (Date.now() < deadline) {
    lastSize = await lineageLedger.getSize();
    if (lastSize >= target) return lastSize;
    await new Promise(r => setTimeout(r, DEFAULT_POLL_MS));
  }
  throw new Error(
    `[sync-barrier] waitForLedgerEntryCount timed out after ${timeoutMs}ms: ` +
    `expected >= ${target}, got ${lastSize}`
  );
}

/**
 * Poll until the observability transition log advances past `target` cursor.
 * Uses observability.query.getLogSize() (synchronous, in-memory).
 *
 * @param {number} target — cursor value must exceed this
 * @param {number} [timeoutMs=5000]
 * @returns {Promise<number>} final log size
 */
async function waitForCursorAdvance(target, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const observability = require('../../control-plane/observability');
  const deadline = Date.now() + timeoutMs;
  let lastSize = target;
  while (Date.now() < deadline) {
    lastSize = observability.query.getLogSize();
    if (lastSize > target) return lastSize;
    await new Promise(r => setTimeout(r, DEFAULT_POLL_MS));
  }
  throw new Error(
    `[sync-barrier] waitForCursorAdvance timed out after ${timeoutMs}ms: ` +
    `expected > ${target}, got ${lastSize}`
  );
}

/**
 * Poll until a registered consumer's lag drops to at most `maxLag`.
 * Uses observability.query.getConsumerLag().
 *
 * @param {string} consumerName — registered consumer name
 * @param {number} [maxLag=0] — acceptable lag ceiling
 * @param {number} [timeoutMs=5000]
 * @returns {Promise<{cursor: number, head: number, lag: number}>}
 */
async function waitForConsumerLag(consumerName, maxLag = 0, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const observability = require('../../control-plane/observability');
  const deadline = Date.now() + timeoutMs;
  let lastLag = { lag: -1 };
  while (Date.now() < deadline) {
    lastLag = observability.query.getConsumerLag(consumerName);
    if (lastLag.lag >= 0 && lastLag.lag <= maxLag) return lastLag;
    await new Promise(r => setTimeout(r, DEFAULT_POLL_MS));
  }
  throw new Error(
    `[sync-barrier] waitForConsumerLag timed out after ${timeoutMs}ms: ` +
    `consumer '${consumerName}' lag=${lastLag.lag}, expected <= ${maxLag}`
  );
}

/**
 * Poll until the lineage worker's in-memory entry count stabilizes.
 * "Stable" = no growth for 2 consecutive polls, meaning ingestion
 * has caught up to the current transition log tail.
 *
 * @param {number} [timeoutMs=5000]
 * @returns {Promise<number>} final stable entry count
 */
async function waitForProjectionFlush(timeoutMs = DEFAULT_TIMEOUT_MS) {
  const lineageWorker = require('../../control-plane/governance/lineage-worker');
  const deadline = Date.now() + timeoutMs;
  let lastCount = lineageWorker.getLedgerSize();
  let stablePolls = 0;
  while (Date.now() < deadline) {
    await new Promise(r => setTimeout(r, DEFAULT_POLL_MS));
    const current = lineageWorker.getLedgerSize();
    if (current === lastCount) {
      stablePolls++;
      if (stablePolls >= 2) return current;
    } else {
      stablePolls = 0;
      lastCount = current;
    }
  }
  throw new Error(
    `[sync-barrier] waitForProjectionFlush timed out after ${timeoutMs}ms: ` +
    `count did not stabilize (last=${lastCount})`
  );
}

/**
 * Poll until the observability transition log reaches at least `target` entries.
 * Synchronous version for fast in-memory checks.
 *
 * @param {number} target — minimum expected log size
 * @param {number} [timeoutMs=5000]
 * @returns {Promise<number>} final log size
 */
async function waitForLogSize(target, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const observability = require('../../control-plane/observability');
  const deadline = Date.now() + timeoutMs;
  let lastSize = 0;
  while (Date.now() < deadline) {
    lastSize = observability.query.getLogSize();
    if (lastSize >= target) return lastSize;
    await new Promise(r => setTimeout(r, DEFAULT_POLL_MS));
  }
  throw new Error(
    `[sync-barrier] waitForLogSize timed out after ${timeoutMs}ms: ` +
    `expected >= ${target}, got ${lastSize}`
  );
}

/**
 * Poll until the Redis ledger contains at least one entry matching `predicate`.
 * Used when a specific entry type is expected (e.g., SEMANTIC_PROJECTION_TRANSITION).
 *
 * @param {Function} predicate — (entry) => boolean
 * @param {number} [lookback=200] — how many recent entries to check per poll
 * @param {number} [timeoutMs=10000]
 * @returns {Promise<object>} the first matching entry found
 */
async function waitForLedgerEntry(predicate, lookback = 200, timeoutMs = 10000) {
  const lineageLedger = require('../../control-plane/governance/lineage-ledger');
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const entries = await lineageLedger.getLineage(lookback);
    const match = entries.find(predicate);
    if (match) return match;
    await new Promise(r => setTimeout(r, DEFAULT_POLL_MS));
  }
  throw new Error(
    `[sync-barrier] waitForLedgerEntry timed out after ${timeoutMs}ms: ` +
    `no entry matched predicate in last ${lookback} entries`
  );
}

/**
 * Wait for the observability transition log to advance to a specific cursor.
 * Stores the current size, then polls until getLogSize() exceeds it.
 *
 * @param {number} [timeoutMs=30000] — constitutional deadlock protection
 * @returns {Promise<number>} the new cursor position after advancement
 * @throws {Error} on timeout
 */
async function waitForLogAdvance(timeoutMs = 30000) {
  const observability = require('../../control-plane/observability');
  const before = observability.query.getLogSize();
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const current = observability.query.getLogSize();
    if (current > before) return current;
    await new Promise(r => setTimeout(r, DEFAULT_POLL_MS));
  }
  const current = observability.query.getLogSize();
  throw new Error(
    `[sync-barrier] waitForLogAdvance timed out after ${timeoutMs}ms: ` +
    `expected > ${before}, got ${current}`
  );
}

/**
 * Wait until the lineage worker's cursor has advanced past the given entryId.
 * This is the deterministic constitutional visibility primitive — callers wait
 * for cursor legitimacy, not poll completion or elapsed time.
 *
 * Uses lineageWorker.waitForCommit() internally.
 *
 * @param {string|number} entryId — ledger entry ID to await commit for
 * @param {number} [timeoutMs=30000] — constitutional deadlock protection timeout
 * @returns {Promise<string>} the committed entryId
 * @throws {Error} on timeout or constitutional failure
 */
async function waitForCommit(entryId, timeoutMs = 30000) {
  const lineageWorker = require('../../control-plane/governance/lineage-worker');
  return lineageWorker.waitForCommit(entryId, timeoutMs);
}

module.exports = {
  waitForLedgerEntryCount,
  waitForCursorAdvance,
  waitForConsumerLag,
  waitForProjectionFlush,
  waitForLogSize,
  waitForLedgerEntry,
  waitForLogAdvance,
  waitForCommit,
  DEFAULT_POLL_MS,
  DEFAULT_TIMEOUT_MS,
};
