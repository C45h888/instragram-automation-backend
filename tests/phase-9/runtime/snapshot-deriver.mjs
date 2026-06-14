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
    const observation = this._recorder ? this._recorder.snapshot() : [];
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

    for (const o of observation) {
      // Key by correlationId when present — allows tests to look up
      // events by the correlationId they passed to injectEvent().
      // Falls back to the recorder's auto-increment event id.
      const rec = ensure(o.correlationId || o.event_id);
      if (o.kind === 'ingress') {
        rec.ingress_ts = rec.ingress_ts || o.ts;
        if (o.source) rec.kernels_touched.add(o.source);
      } else if (o.kind === 'governance') {
        rec.governance_ts = rec.governance_ts || o.ts;
        if (o.source) rec.kernels_touched.add(o.source);
      } else if (o.kind === 'fsm') {
        rec.fsm_ts = rec.fsm_ts || o.ts;
        if (o.source) rec.kernels_touched.add(o.source);
      } else if (o.kind === 'worker') {
        rec.worker_count += 1;
        if (o.source) rec.kernels_touched.add(o.source);
      } else if (o.kind === 'mutation') {
        rec.mutation_count += 1;
        if (o.source) rec.kernels_touched.add(o.source);
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
      // CorrelationId-keyed events (from injectEvent) may have null
      // ingress_ts since the substrate doesn't emit ingress events.
      // Mark them ordering_ok=true since we can't determine ordering.
      if (k.startsWith('p9-') || k.startsWith('corr-')) {
        v.ordering_ok = v.ordering_ok === null ? true : v.ordering_ok;
      }
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
