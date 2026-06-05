/**
 * _kernel-battery-contract — Shared 7-category assertion surface (Phase 7 contract §5)
 * ═══════════════════════════════════════════════════════════════════════════════════════
 *
 * Every kernel battery, without exception, internally covers seven
 * categories of validation. This is the surface that makes Phase 7
 * runtime-true instead of unit-true.
 *
 *   1. State mutation correctness
 *   2. Event causality correctness
 *   3. Governance correctness
 *   4. Cadence correctness
 *   5. Worker correctness
 *   6. Persistence correctness
 *   7. Observability correctness
 *
 * A battery file calls runKernelBattery({ kernel, simulator, fixtures, assertions })
 * with seven assertion functions. The helper wires the simulator's
 * observation surface into each assertion, produces uniform failure
 * reports, and ensures no category is silently skipped.
 *
 * Usage (per battery file):
 *
 *   import { runKernelBattery } from './_kernel-battery-contract.js';
 *
 *   runKernelBattery({
 *     kernel: 'acquisition',
 *     simulator,
 *     assertions: {
 *       stateMutation:     async (s) => { ... },
 *       eventCausality:    async (s) => { ... },
 *       governance:        async (s) => { ... },
 *       cadence:           async (s) => { ... },
 *       workerCorrectness: async (s) => { ... },
 *       persistence:       async (s) => { ... },
 *       observability:     async (s) => { ... },
 *     },
 *   });
 *
 * Each assertion receives a context: { simulator, fixtures, kernel, category }
 * and must throw on failure.
 */

const REQUIRED_CATEGORIES = [
  'stateMutation',
  'eventCausality',
  'governance',
  'cadence',
  'workerCorrectness',
  'persistence',
  'observability',
];

const CATEGORY_LABELS = {
  stateMutation: 'state mutation correctness',
  eventCausality: 'event causality correctness',
  governance: 'governance correctness',
  cadence: 'cadence correctness',
  workerCorrectness: 'worker correctness',
  persistence: 'persistence correctness',
  observability: 'observability correctness',
};

/**
 * Run a kernel battery against the 7-category surface.
 *
 * @param {object} opts
 * @param {string} opts.kernel — kernel name
 * @param {object} opts.simulator — Phase7RuntimeSimulator instance
 * @param {object} [opts.fixtures] — fixture data
 * @param {object} opts.assertions — map of category => async (ctx) => void
 * @param {function} [opts.beforeAll] — async (ctx) => optional setup
 * @param {function} [opts.afterAll] — async (ctx) => optional teardown
 * @returns {Promise<{passed: string[], failed: Array, summary: object}>}
 */
async function runKernelBattery({
  kernel,
  simulator,
  fixtures = {},
  assertions,
  beforeAll = null,
  afterAll = null,
}) {
  if (!kernel) throw new Error('runKernelBattery: kernel name required');
  if (!simulator) throw new Error('runKernelBattery: simulator required');
  if (!assertions) throw new Error('runKernelBattery: assertions required');

  // Verify all 7 categories are present
  for (const cat of REQUIRED_CATEGORIES) {
    if (typeof assertions[cat] !== 'function') {
      throw new Error(
        `runKernelBattery[${kernel}]: missing assertion for category "${cat}" (${CATEGORY_LABELS[cat]})`
      );
    }
  }

  const ctx = { simulator, fixtures, kernel, results: {} };
  const passed = [];
  const failed = [];

  try {
    if (beforeAll) await beforeAll(ctx);

    for (const category of REQUIRED_CATEGORIES) {
      const label = CATEGORY_LABELS[category];
      const catCtx = { ...ctx, category };
      try {
        await assertions[category](catCtx);
        passed.push(category);
        ctx.results[category] = { ok: true };
      } catch (e) {
        failed.push({ category, label, error: e });
        ctx.results[category] = { ok: false, error: e };
        // Continue running remaining categories so the report shows all
      }
    }
  } finally {
    if (afterAll) {
      try { await afterAll(ctx); } catch (_) { /* swallow teardown */ }
    }
  }

  const summary = {
    kernel,
    runId: simulator.runId,
    passed: passed.length,
    failed: failed.length,
    total: REQUIRED_CATEGORIES.length,
    timestamp: new Date().toISOString(),
  };

  if (failed.length > 0) {
    const err = new Error(
      `Kernel battery "${kernel}" failed: ${failed.length}/${REQUIRED_CATEGORIES.length} categories`
    );
    err.kernel = kernel;
    err.summary = summary;
    err.failed = failed;
    err.passed = passed;
    try { simulator.report({ error: err, label: `kernel-battery:${kernel}` }); } catch (_) {}
    throw err;
  }

  return { passed, failed, summary };
}

/**
 * Lower-level helper for vitest describe blocks. Use this when the
 * battery must be split into per-category it() blocks for granular
 * reporting.
 */
function describeKernelBattery(name, factory) {
  return {
    name,
    categories: REQUIRED_CATEGORIES.slice(),
    factory,
  };
}

module.exports = {
  runKernelBattery,
  describeKernelBattery,
  REQUIRED_CATEGORIES,
  CATEGORY_LABELS,
};
