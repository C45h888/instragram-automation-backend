/**
 * StateInspector — Pre/post snapshot + diff (Phase 7 contract §8)
 * ═════════════════════════════════════════════════════════════════
 *
 * Captures the actual database state, projection rows, governance
 * state, and capability state. Diffs two snapshots. A test asserts
 * the diff, not a function return value.
 *
 * Phase 7 contract:
 *   "A correct function output with an incorrect state mutation is
 *    not a pass. A correct state mutation through an incorrect
 *    governance route is also not a pass."
 *
 * Usage:
 *   const insp = new StateInspector({ ck, ledger, capabilityFsm, postgres });
 *   const before = await insp.snapshot();
 *   ... do work ...
 *   const after = await insp.snapshot();
 *   const diff = insp.diff(before, after);
 */

const crypto = require('crypto');

function deepClone(obj) {
  if (obj === null || obj === undefined) return obj;
  if (typeof obj !== 'object') return obj;
  return JSON.parse(JSON.stringify(obj));
}

function hashObject(obj) {
  if (obj === null || obj === undefined) return 'null';
  return crypto
    .createHash('sha256')
    .update(JSON.stringify(obj))
    .digest('hex')
    .slice(0, 16);
}

class StateInspector {
  /**
   * @param {object} sources
   * @param {object} sources.ck — Constitutional Kernel
   * @param {object} sources.lineageLedger — lineage ledger
   * @param {object} sources.capabilityFsm — graph-capability fsm
   * @param {object} [sources.postgres] — pg client (optional)
   * @param {object} [sources.redis] — redis client (optional)
   */
  constructor({ ck, lineageLedger, capabilityFsm, postgres = null, redis = null }) {
    this._ck = ck;
    this._ledger = lineageLedger;
    this._capabilityFsm = capabilityFsm;
    this._postgres = postgres;
    this._redis = redis;
  }

  /**
   * Capture a full state snapshot.
   * @returns {Promise<{
   *   timestamp: number,
   *   governance: object,
   *   capability: object,
   *   lineage: { size: number, hash: string, tail: object[] },
   *   db: object,
   *   redis: object,
   *   hash: string
   * }>}
   */
  async snapshot() {
    const governance = this._ck && typeof this._ck.getState === 'function'
      ? deepClone(this._ck.getState())
      : {};

    const capability = this._capabilityFsm && typeof this._capabilityFsm.getState === 'function'
      ? deepClone(this._capabilityFsm.getState())
      : {};

    let lineage = { size: 0, hash: 'null', tail: [] };
    if (this._ledger) {
      try {
        if (typeof this._ledger.getSize === 'function') {
          lineage.size = this._ledger.getSize();
        } else if (typeof this._ledger.size === 'function') {
          lineage.size = this._ledger.size();
        }
        if (typeof this._ledger.getTail === 'function') {
          lineage.tail = deepClone(this._ledger.getTail(10));
        } else if (typeof this._ledger.tail === 'function') {
          lineage.tail = deepClone(this._ledger.tail(10));
        }
        lineage.hash = hashObject(lineage.tail);
      } catch (_) {
        // ledger may not be initialized in early boot
      }
    }

    let db = {};
    if (this._postgres) {
      try {
        // Caller supplies their own pg-aware snapshot. Default: leave empty.
        db = this._postgres.snapshot ? deepClone(this._postgres.snapshot()) : {};
      } catch (_) {
        // ignore
      }
    }

    let redis = {};
    if (this._redis) {
      try {
        redis = this._redis.snapshot ? deepClone(this._redis.snapshot()) : {};
      } catch (_) {
        // ignore
      }
    }

    const ts = Date.now();
    const hash = hashObject({ ts, governance, capability, lineage, db, redis });
    return { timestamp: ts, governance, capability, lineage, db, redis, hash };
  }

  /**
   * Diff two snapshots. Returns per-store change summary.
   * @param {object} before
   * @param {object} after
   * @returns {{
   *   changed: boolean,
   *   hashBefore: string,
   *   hashAfter: string,
   *   governance: { changed: boolean, diff: object },
   *   capability: { changed: boolean, diff: object },
   *   lineage: { changed: boolean, sizeBefore: number, sizeAfter: number,
   *              newEntries: number },
   *   db: { changed: boolean, diff: object },
   *   redis: { changed: boolean, diff: object }
   * }}
   */
  diff(before, after) {
    if (!before || !after) {
      return { changed: false, error: 'snapshot missing' };
    }

    const governanceChanged = before.hash !== after.hash;
    return {
      changed: governanceChanged,
      hashBefore: before.hash,
      hashAfter: after.hash,
      governance: {
        changed: !deepEqual(before.governance, after.governance),
        diff: shallowDiff(before.governance, after.governance),
      },
      capability: {
        changed: !deepEqual(before.capability, after.capability),
        diff: shallowDiff(before.capability, after.capability),
      },
      lineage: {
        changed: before.lineage.hash !== after.lineage.hash,
        sizeBefore: before.lineage.size,
        sizeAfter: after.lineage.size,
        newEntries: (after.lineage.size || 0) - (before.lineage.size || 0),
      },
      db: {
        changed: !deepEqual(before.db, after.db),
        diff: shallowDiff(before.db, after.db),
      },
      redis: {
        changed: !deepEqual(before.redis, after.redis),
        diff: shallowDiff(before.redis, after.redis),
      },
    };
  }

  /**
   * Assert a snapshot matches an expectation. Throws on mismatch.
   * @param {object} actual
   * @param {object} expectation — partial shape to match
   * @param {string} [label='state']
   */
  assertMatches(actual, expectation, label = 'state') {
    const violations = [];
    for (const key of Object.keys(expectation)) {
      if (!deepContains(actual[key], expectation[key])) {
        violations.push({ key, expected: expectation[key], actual: actual[key] });
      }
    }
    if (violations.length > 0) {
      const err = new Error(
        `StateInspector: ${label} mismatch (${violations.length} violation${violations.length === 1 ? '' : 's'})`
      );
      err.violations = violations;
      err.actual = actual;
      err.expectation = expectation;
      throw err;
    }
  }
}

function deepEqual(a, b) {
  if (a === b) return true;
  if (a == null || b == null) return a === b;
  if (typeof a !== typeof b) return false;
  if (typeof a !== 'object') return a === b;
  const ak = Object.keys(a);
  const bk = Object.keys(b);
  if (ak.length !== bk.length) return false;
  for (const k of ak) {
    if (!deepEqual(a[k], b[k])) return false;
  }
  return true;
}

function shallowDiff(a, b) {
  const diff = {};
  if (!a || !b) return diff;
  const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
  for (const k of keys) {
    if (!deepEqual(a[k], b[k])) {
      diff[k] = { before: a[k], after: b[k] };
    }
  }
  return diff;
}

function deepContains(haystack, needle) {
  if (needle === undefined) return true;
  if (haystack === undefined || haystack === null) return false;
  if (typeof needle !== 'object' || needle === null || Array.isArray(needle)) {
    return deepEqual(haystack, needle);
  }
  for (const k of Object.keys(needle)) {
    if (!deepContains(haystack[k], needle[k])) return false;
  }
  return true;
}

module.exports = { StateInspector };
