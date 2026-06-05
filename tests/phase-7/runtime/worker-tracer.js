/**
 * WorkerTracer — Per-worker execution record (Phase 7 contract §11, §15)
 * ═════════════════════════════════════════════════════════════════════
 *
 * Records every worker invocation with start, success, fail, duration,
 * retry, and degradation. A worker that self-authorizes or routes
 * around governance is detectable here.
 *
 * Usage:
 *   const tracer = new WorkerTracer();
 *   tracer.attach({ observability });
 *   ... do work ...
 *   const records = tracer.records('comments-worker');
 *   tracer.assertNoFailures('comments-worker');
 */

class WorkerTracer {
  constructor() {
    this._records = [];
    this._counter = 0;
  }

  /**
   * Wire tracer to a worker. Use this in worker entry points:
   *
   *   const trace = tracer.startTrace('comments-worker', { accountId, payload });
   *   try { ... } catch (e) { trace.fail(e); throw e; }
   *   trace.success(result);
   *
   * Or attach to observability to auto-capture worker emits.
   */
  attach({ observability }) {
    if (!observability) return;
    const self = this;

    if (typeof observability.onWrite === 'function') {
      observability.onWrite((entry) => {
        const t = entry && entry.type;
        if (!t) return;
        if (t.includes('WORKER_') || t.includes('worker.start') || t.includes('worker.fail')) {
          self._record({
            worker: entry.worker || entry.source || 'unknown',
            phase: t,
            duration: entry.duration,
            payload: entry.payload || entry.raw,
            error: entry.error,
          });
        }
      });
    }
  }

  /**
   * Start a trace for a worker invocation. Returns a tracer object
   * with success/fail/retry/degrade methods.
   */
  startTrace(worker, context = {}) {
    const id = ++this._counter;
    const start = Date.now();
    const self = this;

    return {
      id,
      worker,
      context,
      startedAt: start,
      success(result) {
        self._record({
          id,
          worker,
          phase: 'success',
          duration: Date.now() - start,
          result,
          context,
        });
      },
      fail(error) {
        self._record({
          id,
          worker,
          phase: 'fail',
          duration: Date.now() - start,
          error: error && (error.message || String(error)),
          context,
        });
      },
      retry(reason) {
        self._record({
          id,
          worker,
          phase: 'retry',
          duration: Date.now() - start,
          reason,
          context,
        });
      },
      degrade(reason) {
        self._record({
          id,
          worker,
          phase: 'degrade',
          duration: Date.now() - start,
          reason,
          context,
        });
      },
    };
  }

  /** Manually record a worker event. */
  record(entry) {
    this._record(entry);
  }

  _record(entry) {
    if (!entry.id) entry.id = ++this._counter;
    if (!entry.timestamp) entry.timestamp = Date.now();
    this._records.push(entry);
  }

  /** Return records for a worker (or all if no name). */
  records(worker) {
    if (!worker) return this._records.slice();
    return this._records.filter((r) => r.worker === worker);
  }

  /** Assert no failures for a worker. */
  assertNoFailures(worker) {
    const failures = this.records(worker).filter((r) => r.phase === 'fail');
    if (failures.length > 0) {
      const err = new Error(
        `WorkerTracer: ${failures.length} failure(s) for ${worker || 'any worker'}`
      );
      err.failures = failures;
      throw err;
    }
  }

  /** Assert worker did NOT self-authorize (no CK.* calls in its context). */
  assertNoSelfAuthorization(worker) {
    const records = this.records(worker);
    const violations = records.filter(
      (r) =>
        r.context &&
        (r.context.selfAuthorized === true ||
          r.context.bypassedGovernance === true ||
          (r.context.callsMade &&
            (r.context.callsMade.includes('CK.validate') ||
              r.context.callsMade.includes('CK.dispatch'))))
    );
    if (violations.length > 0) {
      const err = new Error(
        `WorkerTracer: ${worker} self-authorized (${violations.length} violation${violations.length === 1 ? '' : 's'})`
      );
      err.violations = violations;
      throw err;
    }
  }

  reset() {
    this._records = [];
    this._counter = 0;
  }

  get size() {
    return this._records.length;
  }
}

module.exports = { WorkerTracer };
