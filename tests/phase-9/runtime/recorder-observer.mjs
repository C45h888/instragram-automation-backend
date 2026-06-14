// Phase 9 — Recorder Observer (PASSIVE).
//
// The recorder is a SUBSCRIBER, not an actor. It reads from the
// lineage ledger — the constitutional source of truth per the
// architecture: "Lineage is canonical truth; runtime state is a
// projection." It also reads from the observability plane's
// EventRecorder as a secondary source for events that have not
// yet been lineage-recorded.
//
// This module exposes ONLY observation methods. There is no
// .ingress() / .governance() / .fsm() / .worker() / .mutation()
// write API. Phase 9 tests cannot fabricate constitutional
// events; they can only assert against what the runtime did.

import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);
const lineageLedger = require('../../../control-plane/governance/lineage-ledger');

export class RecorderObserver {
  constructor() {
    this._attached = false;
    this._simulator = null;
  }

  attach(simulator) {
    if (this._attached) return;
    this._simulator = simulator;
    this._attached = true;
  }

  /**
   * Returns the observation log — a flat list of events captured
   * from the lineage ledger (canonical) and the observability
   * EventRecorder (secondary). Each entry has: event_id, kind,
   * source, payload, ts.
   */
  async snapshot() {
    const observed = [];
    // Source 1: lineage ledger (canonical constitutional events)
    try {
      const lineage = await lineageLedger.getLineage(500);
      for (const entry of lineage) {
        observed.push(this._normalizeLineage(entry));
      }
    } catch (_) {
      // ledger unavailable (no Redis) — fall through
    }
    // Source 2: observability EventRecorder (secondary, for events
    // that have not yet been lineage-recorded)
    if (this._simulator) {
      const timeline = this._simulator.timeline();
      for (const e of timeline) observed.push(this._normalizeObservability(e));
      const mutations = this._simulator.mutations ? this._simulator.mutations() : [];
      for (const m of mutations) {
        observed.push({
          event_id: m.correlationId || m.entry?.entityId || `mut-${m.id}`,
          kind: 'mutation',
          source: m.source || m.entry?.domain || 'mutation-substrate',
          payload: m.entry || {},
          ts: m.timestamp,
        });
      }
    }
    return observed;
  }

  _normalizeLineage(entry) {
    const t = (entry.type || entry.eventType || '').toUpperCase();
    const id = entry.intentId || entry.eventId || entry.correlationId || `lineage-${entry.id || entry.ts || Math.random()}`;
    return {
      event_id: id,
      kind: this._inferKind(t),
      source: entry.domain || entry.authority || entry.source || 'lineage-ledger',
      payload: entry,
      ts: entry.ts || entry.emittedAt || entry.timestamp || 0,
    };
  }

  _normalizeObservability(e) {
    const id = e.correlationId || `evt-${e.id}`;
    return {
      event_id: id,
      kind: this._inferKind((e.type || '').toUpperCase()),
      source: e.source || 'runtime',
      payload: e.payload || null,
      ts: e.timestamp,
    };
  }

  _inferKind(t) {
    if (t.includes('VALIDATE') || t.includes('GOVERN') || t.includes('DISPATCH') || t.includes('WEBHOOK_EVENT_RECEIVED') || t.includes('PERSIST')) return 'governance';
    if (t.includes('WORKER')) return 'worker';
    if (t.includes('TRANSITION') || t.includes('FSM') || t.includes('STATE')) return 'fsm';
    if (t.includes('MUTATION') || t.includes('WRITE') || t.includes('UPSERT') || t.includes('DB_WRITE')) return 'mutation';
    return 'ingress';
  }

  /**
   * Return the recorder-shape summary — the legacy phase-8 view
   * derived from the runtime's actual events. Each event_id is
   * summarized with the first ts of each kind.
   */
  async summarize() {
    const obs = await this.snapshot();
    const byId = new Map();
    for (const o of obs) {
      if (!byId.has(o.event_id)) {
        byId.set(o.event_id, {
          event_id: o.event_id,
          ingress_ts: null,
          governance_ts: null,
          fsm_ts: null,
          worker_count: 0,
          mutation_count: 0,
        });
      }
      const e = byId.get(o.event_id);
      if (o.kind === 'ingress') e.ingress_ts = e.ingress_ts || o.ts;
      else if (o.kind === 'governance') e.governance_ts = e.governance_ts || o.ts;
      else if (o.kind === 'fsm') e.fsm_ts = e.fsm_ts || o.ts;
      else if (o.kind === 'worker') e.worker_count += 1;
      else if (o.kind === 'mutation') e.mutation_count += 1;
    }
    for (const e of byId.values()) {
      const t = [e.ingress_ts, e.governance_ts, e.fsm_ts];
      e.ordering_ok = t.every((x) => x != null) ? (t[0] <= t[1] && t[1] <= t[2]) : null;
    }
    return Array.from(byId.values());
  }
}
