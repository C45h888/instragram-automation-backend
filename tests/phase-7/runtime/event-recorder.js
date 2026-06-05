/**
 * EventRecorder — Causal timeline per test (Phase 7 contract §7)
 * ════════════════════════════════════════════════════════════════
 *
 * Canonical source of runtime truth during testing. Captures every
 * event with timestamp, type, source, destination, payload snapshot,
 * and any linked state transition. Test failures are explainable as
 * a sequence, not a mystery.
 *
 * Phase 7 contract:
 *   "Every event should be captured with a timestamp, an event type,
 *    a source, a destination, a payload snapshot, and any linked
 *    state transition."
 *
 * Usage:
 *   const rec = new EventRecorder();
 *   rec.attach(observability);
 *   rec.record({ type, source, destination, payload, transition });
 *   const tl = rec.timeline();
 *   const filtered = rec.filter({ type, since, until, source });
 */

class EventRecorder {
  constructor() {
    this._events = [];
    this._counter = 0;
    this._attached = false;
  }

  /**
   * Record a single event into the timeline.
   * @param {object} evt
   * @param {string} evt.type — event type
   * @param {string} [evt.source] — emitter
   * @param {string} [evt.destination] — receiver
   * @param {object} [evt.payload] — payload snapshot (deep-copied)
   * @param {object} [evt.transition] — linked state transition
   * @param {string} [evt.correlationId] — causal link
   * @param {number} [evt.timestamp] — ms (default Date.now())
   * @returns {object} the recorded event with assigned id
   */
  record({
    type,
    source = null,
    destination = null,
    payload = null,
    transition = null,
    correlationId = null,
    timestamp = Date.now(),
    ...rest
  }) {
    const id = ++this._counter;
    const entry = {
      id,
      timestamp,
      type,
      source,
      destination,
      payload: payload ? JSON.parse(JSON.stringify(payload)) : null,
      transition: transition ? JSON.parse(JSON.stringify(transition)) : null,
      correlationId,
      ...rest,
    };
    this._events.push(entry);
    return entry;
  }

  /**
   * Attach to an observability plane. Wires onWrite / onEmit hooks
   * (whichever exist) so that every event the plane records flows
   * into this timeline. Idempotent.
   */
  attach(observability) {
    if (this._attached) return;
    this._attached = true;

    const self = this;
    if (typeof observability.onWrite === 'function') {
      observability.onWrite((entry) => {
        self.record({
          type: entry.type || entry.eventType || 'OBSERVABILITY_WRITE',
          source: entry.source || entry.domain || 'observability',
          destination: entry.destination || null,
          payload: entry.raw || entry.payload || entry,
          transition: entry.previousState && entry.nextState
            ? { from: entry.previousState, to: entry.nextState }
            : null,
          correlationId: entry.correlationId || null,
        });
      });
    }

    if (typeof observability.onEmit === 'function') {
      observability.onEmit((evt) => {
        self.record({
          type: evt.type || 'OBSERVABILITY_EMIT',
          source: evt.source || 'observability',
          destination: evt.destination || null,
          payload: evt.payload || evt,
        });
      });
    }
  }

  /** Return full timeline (read-only copy). */
  timeline() {
    return this._events.slice();
  }

  /** Filter timeline. All filters optional. */
  filter({ type, since, until, source, destination, correlationId } = {}) {
    return this._events.filter((e) => {
      if (type && e.type !== type) return false;
      if (since != null && e.timestamp < since) return false;
      if (until != null && e.timestamp > until) return false;
      if (source && e.source !== source) return false;
      if (destination && e.destination !== destination) return false;
      if (correlationId && e.correlationId !== correlationId) return false;
      return true;
    });
  }

  /**
   * Assert a specific chain of event types occurred in order.
   * @param {string[]} chain — ordered list of event types
   * @returns {{ matched: boolean, indices: number[] }}
   */
  matchChain(chain) {
    const indices = [];
    let cursor = 0;
    for (let i = 0; i < this._events.length && cursor < chain.length; i++) {
      if (this._events[i].type === chain[cursor]) {
        indices.push(i);
        cursor++;
      }
    }
    return { matched: cursor === chain.length, indices };
  }

  /** Reset the recorder. */
  reset() {
    this._events = [];
    this._counter = 0;
  }

  /** Total events recorded. */
  get size() {
    return this._events.length;
  }
}

module.exports = { EventRecorder };
