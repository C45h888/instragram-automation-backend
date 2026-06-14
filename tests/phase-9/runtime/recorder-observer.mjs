// Phase 9 — Recorder Observer (PASSIVE).
//
// The recorder is a SUBSCRIBER, not an actor. It reads from the
// underlying Phase7RuntimeSimulator's EventRecorder timeline —
// which captures what the runtime actually did — and projects it
// into the phase-8 bucket shape for backward compatibility.
//
// This module exposes ONLY observation methods. There is no
// .ingress() / .governance() / .fsm() / .worker() / .mutation()
// write API. Phase 9 tests cannot fabricate constitutional
// events; they can only assert against what the runtime did.

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
   * by the runtime, normalized into the recorder bucket shape.
   * Each entry has: event_id, kind, source, payload, ts.
   *
   * The "event_id" is derived from the runtime's correlationId
   * (the simulator sets one on every injected event). The "kind"
   * is inferred from the runtime's event type:
   *   - events with type containing GOVERNANCE/VALIDATE → governance
   *   - events with type containing WORKER → worker
   *   - events with type containing TRANSITION/FSM → fsm
   *   - mutation events are derived from the mutation tracker
   *   - everything else from a phase-9 ingress path → ingress
   */
  snapshot() {
    if (!this._simulator) return [];
    const timeline = this._simulator.timeline();
    const mutations = this._simulator.mutations ? this._simulator.mutations() : [];
    const observed = [];
    for (const e of timeline) {
      observed.push(this._normalize(e));
    }
    // Append mutation events that the timeline didn't surface as
    // their own type. The mutation tracker in phase-7 hooks the
    // lineage ledger directly, so they live there.
    for (const m of mutations) {
      const id = m.correlationId || m.entry?.entityId || `mut-${m.id}`;
      observed.push({
        event_id: id,
        kind: 'mutation',
        source: m.source || m.entry?.domain || 'mutation-substrate',
        payload: m.entry || {},
        ts: m.timestamp,
      });
    }
    return observed;
  }

  _normalize(e) {
    const id = e.correlationId || `evt-${e.id}`;
    const kind = this._inferKind(e);
    return {
      event_id: id,
      kind,
      source: e.source || 'runtime',
      payload: e.payload || null,
      ts: e.timestamp,
    };
  }

  _inferKind(e) {
    const t = (e.type || '').toUpperCase();
    if (t.includes('VALIDATE') || t.includes('GOVERN') || t.includes('DISPATCH')) return 'governance';
    if (t.includes('WORKER')) return 'worker';
    if (t.includes('TRANSITION') || t.includes('FSM') || t.includes('STATE')) return 'fsm';
    if (t.includes('MUTATION') || t.includes('WRITE') || t.includes('UPSERT')) return 'mutation';
    return 'ingress';
  }

  /**
   * Return the recorder-shape summary — the legacy phase-8 view
   * derived from the runtime's actual events. Each event_id is
   * summarized with the first ts of each kind.
   */
  summarize() {
    const obs = this.snapshot();
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
