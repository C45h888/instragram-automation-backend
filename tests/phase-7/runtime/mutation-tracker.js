/**
 * MutationTracker — Per-store mutation delta (Phase 7 contract §8)
 * ════════════════════════════════════════════════════════════════
 *
 * Records every state mutation that occurs during a test, attributed
 * to the source that caused it (worker, FSM, CK, substrate). The
 * canonical mutation surface is:
 *   - lineage rows (governance writes)
 *   - projection rows (telemetry writes)
 *   - governance state changes (CK transitions)
 *   - capability state changes (graph-capability transitions)
 *
 * Usage:
 *   const tr = new MutationTracker();
 *   tr.attach(ck, lineageLedger, capabilityFsm);
 *   ... do work ...
 *   const mutations = tr.drain();
 *   tr.assertMutation({ store: 'lineage', predicate: (m) => ... });
 */

class MutationTracker {
  constructor() {
    this._mutations = [];
    this._counter = 0;
    this._attached = false;
  }

  /**
   * Wire the tracker to live mutation sources.
   * Hooks are additive; existing observability hooks untouched.
   */
  attach({ ck, lineageLedger, capabilityFsm, observability }) {
    if (this._attached) return;
    this._attached = true;

    const self = this;

    // Hook substrate mutation writes via the observability plane.
    // onWrite fires for every DB write the substrate emits, including
    // worker-stage projections that are not governed transitions.
    if (observability && typeof observability.onWrite === 'function') {
      observability.onWrite((entry) => {
        self._record({
          store: 'mutation-substrate',
          op: 'write',
          source: entry.source || entry.domain || 'mutation-substrate',
          entry: {
            type: entry.type || entry.eventType,
            domain: entry.domain,
            entityId: entry.entityId,
            table: entry.table || entry.entity,
          },
        });
      });
    }

    // Hook lineage writes
    if (lineageLedger && typeof lineageLedger.onWrite === 'function') {
      lineageLedger.onWrite((entry) => {
        self._record({
          store: 'lineage',
          op: 'append',
          source: entry.source || entry.authority || 'unknown',
          entry: {
            type: entry.type || entry.eventType,
            domain: entry.domain,
            entityId: entry.entityId,
          },
        });
      });
    }

    // Hook governance state changes
    if (ck && typeof ck.onTransition === 'function') {
      ck.onTransition((t) => {
        self._record({
          store: 'governance',
          op: 'transition',
          source: t.source || 'ck',
          entry: { from: t.from, to: t.to, via: t.via },
        });
      });
    }

    // Hook capability state changes
    if (capabilityFsm && typeof capabilityFsm.onTransition === 'function') {
      capabilityFsm.onTransition((t) => {
        self._record({
          store: 'capability',
          op: 'transition',
          source: t.source || 'graph-capability',
          entry: { from: t.from, to: t.to, accountId: t.accountId },
        });
      });
    }
  }

  _record(mutation) {
    this._counter++;
    this._mutations.push({
      id: this._counter,
      timestamp: Date.now(),
      ...mutation,
    });
  }

  /**
   * Manually record a mutation observed via a substrate hook.
   * Use this for stores without a built-in onWrite event.
   */
  record(mutation) {
    this._record(mutation);
  }

  /**
   * Drain all recorded mutations (returns and clears).
   * @returns {object[]}
   */
  drain() {
    const out = this._mutations.slice();
    this._mutations = [];
    return out;
  }

  /**
   * Peek at recorded mutations without draining.
   * @param {object} [filter]
   * @param {string} [filter.store]
   * @param {string} [filter.source]
   */
  peek(filter = {}) {
    return this._mutations.filter((m) => {
      if (filter.store && m.store !== filter.store) return false;
      if (filter.source && m.source !== filter.source) return false;
      return true;
    });
  }

  /**
   * Assert at least one mutation matching the predicate exists.
   */
  assertMutation({ store, predicate, label = 'mutation' }) {
    const found = this._mutations.find(
      (m) => (!store || m.store === store) && (!predicate || predicate(m))
    );
    if (!found) {
      const err = new Error(
        `MutationTracker: no ${label} found${store ? ` in store=${store}` : ''}`
      );
      err.observed = this._mutations.slice();
      throw err;
    }
    return found;
  }

  /** Reset. */
  reset() {
    this._mutations = [];
    this._counter = 0;
  }

  get size() {
    return this._mutations.length;
  }
}

module.exports = { MutationTracker };
