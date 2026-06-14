// Phase 9 — Ownership Tracer.
// Reads the recorder-observer snapshot and the underlying simulator's
// decision/transition logs to project the OWNER of each link in the
// constitutional chain for each event_id.
//
// Architecture-mandated owners (verified by the test):
//   ingress    → runtime/ingress
//   governance → constitutional-kernel (CK)
//   fsm        → <domain>-fsm (e.g., acquisition-fsm, publishing-fsm)
//   worker     → <kernel> (e.g., acquisition-kernel, publishing-kernel)
//   mutation   → mutation-substrate (owner) + <kernel> + <table>

export class OwnershipTracer {
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
   * Project the ownership chain for every observed event_id.
   * For each kind, the owner is derived from the source field of
   * the underlying event — which the runtime itself sets when it
   * emits the event. We do NOT infer owners; we READ them.
   */
  snapshot() {
    if (!this._simulator) return {};
    const timeline = this._simulator.timeline();
    const mutations = this._simulator.mutations ? this._simulator.mutations() : [];
    const governanceDecisions = this._simulator.governanceObserver
      ? this._simulator.governanceObserver.decisions()
      : [];

    const byId = new Map();
    const ensure = (id) => {
      if (!byId.has(id)) {
        byId.set(id, {
          event_id: id,
          ingress:    { owner: null, ts: null },
          governance: { owner: null, ts: null, actor: null },
          fsm:        { owner: null, ts: null, from: null, to: null },
          worker:     { owner: null, ts: null, worker: null },
          mutation:   { owner: null, ts: null, kernel: null, table: null },
        });
      }
      return byId.get(id);
    };

    for (const e of timeline) {
      const id = e.correlationId || `evt-${e.id}`;
      const t = (e.type || '').toUpperCase();
      const owner = e.source || 'runtime';
      const ts = e.timestamp;
      const rec = ensure(id);
      if (t.includes('VALIDATE') || t.includes('GOVERN') || t.includes('DISPATCH')) {
        rec.governance = rec.governance.ts ? rec.governance : { owner, ts, actor: t };
      } else if (t.includes('WORKER')) {
        rec.worker = rec.worker.ts ? rec.worker : { owner, ts, worker: owner };
      } else if (t.includes('TRANSITION') || t.includes('FSM') || t.includes('STATE')) {
        rec.fsm = rec.fsm.ts ? rec.fsm : { owner, ts, from: null, to: t };
      } else {
        rec.ingress = rec.ingress.ts ? rec.ingress : { owner, ts };
      }
    }
    for (const m of mutations) {
      const id = m.correlationId || m.entry?.entityId || `mut-${m.id}`;
      const rec = ensure(id);
      rec.mutation = {
        owner: 'mutation-substrate',
        ts: m.timestamp,
        kernel: m.entry?.domain || m.source || null,
        table: m.entry?.table || m.entry?.entity || null,
      };
    }
    // Fill governance actor from the governance observer's
    // recorded decisions (richer than the timeline entry).
    for (const d of governanceDecisions) {
      // The decisions array doesn't carry correlationId in phase-7;
      // we only use it for stats. Skip per-event correlation for now.
    }

    const out = {};
    for (const [k, v] of byId.entries()) out[k] = v;
    return out;
  }
}
