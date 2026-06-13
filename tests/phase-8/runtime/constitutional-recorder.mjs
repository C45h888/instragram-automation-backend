// Phase 8 — Constitutional Recorder.
// Single source of truth for "did X cause Y" assertions. Tests
// instrument ingress / governance / fsm / worker / state-mutation
// boundaries by calling `recorder.<bucket>(event_id, ...)` here.
//
// summarize() returns the causal record; assertConstitutionalPath()
// validates the chain (ingress → governance → fsm → worker → mutation)
// and the ordering between them.

export class ConstitutionalRecorder {
  constructor() {
    this.events = [];
    this.byId = new Map();
  }

  _id(eventId) {
    if (!this.byId.has(eventId)) {
      this.byId.set(eventId, {
        event_id: eventId,
        ingress: null,
        governance: null,
        fsm: null,
        workers: [],
        mutations: [],
        timeline: [],
      });
    }
    return this.byId.get(eventId);
  }

  _record(eventId, kind, source, payload) {
    const e = this._id(eventId);
    const entry = { event_id: eventId, kind, source, ts: Date.now(), payload };
    e.timeline.push(entry);
    this.events.push(entry);
  }

  ingress(eventId, payload) {
    const e = this._id(eventId);
    e.ingress = { ts: Date.now(), payload };
    this._record(eventId, 'ingress', 'webhook|graph', payload);
  }
  governance(eventId, decision) {
    const e = this._id(eventId);
    e.governance = { ts: Date.now(), decision };
    this._record(eventId, 'governance', decision?.actor || 'unknown', decision);
  }
  fsm(eventId, transition) {
    const e = this._id(eventId);
    e.fsm = { ts: Date.now(), transition };
    this._record(eventId, 'fsm', transition?.fsm || 'unknown', transition);
  }
  worker(eventId, workerName, action) {
    const e = this._id(eventId);
    e.workers.push({ ts: Date.now(), worker: workerName, action });
    this._record(eventId, 'worker', workerName, action);
  }
  mutation(eventId, mutation) {
    const e = this._id(eventId);
    e.mutations.push({ ts: Date.now(), mutation });
    this._record(eventId, 'mutation', mutation?.kernel || 'unknown', mutation);
  }

  summarize() {
    return Array.from(this.byId.values()).map((e) => ({
      event_id: e.event_id,
      ingress_ts:   e.ingress?.ts   || null,
      governance_ts:e.governance?.ts|| null,
      fsm_ts:       e.fsm?.ts       || null,
      worker_count: e.workers.length,
      mutation_count: e.mutations.length,
      ordering_ok: this._orderingOk(e),
    }));
  }

  _orderingOk(e) {
    const t = [e.ingress?.ts, e.governance?.ts, e.fsm?.ts];
    if (t.some((x) => x == null)) return null;
    return t[0] <= t[1] && t[1] <= t[2];
  }

  assertConstitutionalPath(eventId) {
    const e = this._id(eventId);
    const violations = [];
    if (!e.ingress)    violations.push('missing ingress');
    if (!e.governance) violations.push('missing governance decision');
    if (!e.fsm)        violations.push('missing fsm transition');
    if (e.workers.length === 0) violations.push('no worker invocation');
    if (e.mutations.length === 0) violations.push('no state mutation');
    if (e.governance && e.workers.length) {
      const minWorkerTs = Math.min(...e.workers.map((w) => w.ts));
      if (e.governance.ts > minWorkerTs) {
        violations.push('worker dispatched before governance (self-dispatch)');
      }
    }
    if (e.fsm && e.governance && e.fsm.ts < e.governance.ts) {
      violations.push('fsm transitioned before governance decision');
    }
    if (e.workers.length && e.fsm) {
      const minWorkerTs = Math.min(...e.workers.map((w) => w.ts));
      if (minWorkerTs < e.fsm.ts) {
        violations.push('worker ran before fsm transition');
      }
    }
    if (e.mutations.length && e.workers.length) {
      const minWorkerTs = Math.min(...e.workers.map((w) => w.ts));
      const minMutationTs = Math.min(...e.mutations.map((m) => m.ts));
      if (minMutationTs < minWorkerTs) {
        violations.push('mutation preceded worker');
      }
    }
    return { event_id: eventId, ok: violations.length === 0, violations };
  }

  assertAllConstitutional(eventIds) {
    return eventIds.map((id) => this.assertConstitutionalPath(id));
  }

  /**
   * Clear all recorder state. Each test file should call this in
   * beforeAll() so it does not see events recorded by a prior
   * file in the same vitest run. Without this, the recorder
   * accumulates state across the entire test process — and
   * because event_ids are content-hashed, the same webhook
   * fixture delivered twice will reuse the same id, causing
   * workers[] to accumulate historical entries whose
   * timestamps predate the current tick's governance record.
   */
  reset() {
    this.events = [];
    this.byId = new Map();
  }
}

const singleton = new ConstitutionalRecorder();

export const recorder = singleton;
export default singleton;
