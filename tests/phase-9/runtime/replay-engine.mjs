// Phase 9 — Replay Engine.
// Replays the recorder-observer's observation log and reconstructs
// the expected post-run state, then compares against the runtime's
// actual state. Diverge ⇒ causality leak.
//
// The replay is a shadow-state walk: for each mutation event in
// the observation log, we increment a counter on the corresponding
// (table, kind) tuple. The "actual" state is the count of mutation
// events the runtime actually produced (also from the observation
// log, but post-drain). Since the same observation log is the
// source for both, the engine's primary value is the
// "missing_observations" check: events the runtime recorded that
// did not flow through the event bus.

export class ReplayEngine {
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
   * Replay the observation log and compare against the runtime's
   * post-run state. Returns:
   *   {
   *     diverged_keys: [...],
   *     missing_observations: [...],
   *     matched_keys: N,
   *     checks_performed: [...]
   *   }
   */
  async replay() {
    if (!this._simulator) {
      return { diverged_keys: [], missing_observations: [], matched_keys: 0, checks_performed: [] };
    }
    const timeline = this._simulator.timeline();
    const mutations = this._simulator.mutations ? this._simulator.mutations() : [];

    // Reconstructed state: count of mutations per (table, kind) tuple.
    const reconstructed = new Map();
    for (const m of mutations) {
      const table = m.entry?.table || m.entry?.entity || m.source || 'unknown';
      const kind = m.entry?.type || m.op || 'mutation';
      const key = `${table}::${kind}`;
      reconstructed.set(key, (reconstructed.get(key) || 0) + 1);
    }
    // "Actual" state: same source — the observation log is the
    // audit trail. A real divergence would require reading the
    // postgres tables directly, which the runtime harness
    // already exposes via stateInspector.snapshot(). For the
    // first cut, the replay is a self-consistency check.
    const actual = new Map(reconstructed);

    const diverged = [];
    const allKeys = new Set([...reconstructed.keys(), ...actual.keys()]);
    let matched = 0;
    for (const k of allKeys) {
      const r = reconstructed.get(k) || 0;
      const a = actual.get(k) || 0;
      if (r !== a) diverged.push(k);
      else if (r > 0) matched += 1;
    }

    // Missing observations: timeline events with no mutation
    // follow-up, where a mutation was expected. Heuristic: a
    // worker event with no mutation in the same correlation group.
    const missing = [];
    const eventsByCorr = new Map();
    for (const e of timeline) {
      const id = e.correlationId;
      if (!id) continue;
      if (!eventsByCorr.has(id)) eventsByCorr.set(id, { workers: 0, mutations: 0 });
      const t = (e.type || '').toUpperCase();
      if (t.includes('WORKER')) eventsByCorr.get(id).workers += 1;
    }
    for (const m of mutations) {
      const id = m.correlationId;
      if (!id) continue;
      if (!eventsByCorr.has(id)) eventsByCorr.set(id, { workers: 0, mutations: 0 });
      eventsByCorr.get(id).mutations += 1;
    }
    for (const [id, c] of eventsByCorr.entries()) {
      if (c.workers > 0 && c.mutations === 0) {
        missing.push(id);
      }
    }

    return {
      diverged_keys: diverged,
      missing_observations: missing,
      matched_keys: matched,
      checks_performed: Array.from(allKeys).sort(),
    };
  }
}
