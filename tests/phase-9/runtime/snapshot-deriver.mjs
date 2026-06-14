// Phase 9 — Snapshot Deriver.
// Reads the recorder-observer snapshot and projects it into the
// legacy phase-8 "constitutional-path" shape so that downstream
// assertions and reports retain continuity. The recorder shape
// is a DERIVED VIEW; the source of truth is the timeline.
//
// This module is intentionally a pure derivation: no I/O, no
// side effects, no writes. Tests call derive() and assert on
// the result.

export class SnapshotDeriver {
  constructor() {
    this._attached = false;
    this._simulator = null;
    this._recorder = null;
  }

  attach(simulator) {
    if (this._attached) return;
    this._simulator = simulator;
    this._recorder = simulator; // harness wires recorder later; for now we read from the simulator
    this._attached = true;
  }

  /**
   * Derive the constitutional-path snapshot from the runtime's
   * actual events. Returns:
   *   {
   *     derived_at, events: { event_id → bucket }, constitutional_paths: [...], drift_findings: []
   *   }
   */
  derive() {
    if (!this._simulator) return { derived_at: Date.now(), events: {}, constitutional_paths: [], drift_findings: [] };
    const timeline = this._simulator.timeline();
    const mutations = this._simulator.mutations ? this._simulator.mutations() : [];
    const byId = new Map();

    const ensure = (id) => {
      if (!byId.has(id)) {
        byId.set(id, {
          event_id: id,
          ingress_ts: null,
          governance_ts: null,
          fsm_ts: null,
          worker_count: 0,
          mutation_count: 0,
          ordering_ok: null,
          kernels_touched: new Set(),
        });
      }
      return byId.get(id);
    };

    for (const e of timeline) {
      const id = e.correlationId || `evt-${e.id}`;
      const t = (e.type || '').toUpperCase();
      const rec = ensure(id);
      if (t.includes('VALIDATE') || t.includes('GOVERN') || t.includes('DISPATCH')) {
        rec.governance_ts = rec.governance_ts || e.timestamp;
        if (e.source) rec.kernels_touched.add(e.source);
      } else if (t.includes('WORKER')) {
        rec.worker_count += 1;
        if (e.source) rec.kernels_touched.add(e.source);
      } else if (t.includes('TRANSITION') || t.includes('FSM') || t.includes('STATE')) {
        rec.fsm_ts = rec.fsm_ts || e.timestamp;
        if (e.source) rec.kernels_touched.add(e.source);
      } else {
        rec.ingress_ts = rec.ingress_ts || e.timestamp;
        if (e.source) rec.kernels_touched.add(e.source);
      }
    }
    for (const m of mutations) {
      const id = m.correlationId || m.entry?.entityId || `mut-${m.id}`;
      const rec = ensure(id);
      rec.mutation_count += 1;
      if (m.entry?.domain) rec.kernels_touched.add(m.entry.domain);
    }
    const paths = [];
    const events = {};
    for (const [k, v] of byId.entries()) {
      const t = [v.ingress_ts, v.governance_ts, v.fsm_ts];
      v.ordering_ok = t.every((x) => x != null) ? (t[0] <= t[1] && t[1] <= t[2]) : null;
      v.kernels_touched = Array.from(v.kernels_touched);
      const ok = v.ingress_ts && v.governance_ts && v.fsm_ts && v.worker_count > 0 && v.mutation_count > 0 && v.ordering_ok;
      paths.push({ event_id: k, ok: !!ok, ordering_ok: v.ordering_ok, worker_count: v.worker_count, mutation_count: v.mutation_count });
      events[k] = v;
    }
    return {
      derived_at: Date.now(),
      events,
      constitutional_paths: paths,
      drift_findings: [],
    };
  }
}
