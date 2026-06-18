// graph-capability-kernel/index.js
// Kernel root façade. Wires FSM to constitutional kernel, exposes public surface.
//
// Architecture (Phase D — constitutional contract):
//   server.js → gck.install({ck}) → builds dispatch ctx → fsm.setDispatchCtx(ctx)
//     → fsm.setGovernance(ck) → registers membranes
//     → fsm.setMembrane('<name>', {substrate}) → starts the graph-capability
//     substrate → binds signal-dispatch to fsm + ctx
//
//   The FSM is the constitutional ingress for substrate emissions.
//   The CK provides the action fabric (subscribeAction) which the substrate
//   subscribes to via substrate.start(ck). The substrate is a delegated
//   executor orchestrated by the FSM — it never talks to the CK directly
//   for emissions, and the CK never calls the substrate directly.
//
// Membranes (Pass 2, 2026-06-11):
//   health              → TokenHealthWorker (token validation, UAT refresh, recovery)
//   quota-intelligence  → QuotaIntelligenceWorker (usage monitoring, pressure detection)
//   webhook-sync        → WebhookSyncWorker (event dedup, health, drift detection)
//   dependency-recovery → DependencyRecoveryWorker (circuit breaker, endpoint health)
//   permission-recovery → PermissionRecoveryWorker (scope drift, role changes)
//   account-sync        → AccountSyncWorker (cross-domain reconciliation)
//   escalation          → EscalationWorker (unrecoverable condition handling)

const wiring = require('./substrates/graph-capability/wiring');
const fsm = require('./fsm');
const signalDispatch = require('./substrates/vault/signal-dispatch');
const healthSubstrate = require('./substrates/health-substrate');
const orchestrator = require('./orchestrator');

// Capability-check workers (registered via CK's invokeWorker gate)
const credentialCapWorker = require('./substrates/capability-check-substrate/workers/credential-capability-worker');
const quotaIntWorker = require('./substrates/capability-check-substrate/workers/quota-intelligence-worker');

// Worker imports (Pass 2)
const QuotaIntelligenceWorker = require('./substrates/workers/quota-intelligence-worker');
const WebhookSyncWorker = require('./substrates/workers/webhook-sync-worker');
const DependencyRecoveryWorker = require('./substrates/workers/dependency-recovery-worker');
const PermissionRecoveryWorker = require('./substrates/workers/permission-recovery-worker');
const AccountSyncWorker = require('./substrates/workers/account-sync-worker');
const EscalationWorker = require('./substrates/workers/escalation-worker');

// Re-export the public surface from the kernel substrates
const vault = require('./substrates/vault');
const health = require('./substrates/health-substrate');

let _installed = false;
let _started = false;
let _lastBoundCk = null;

// ── Worker instances (singleton per kernel boot) ───────────────────────────

let _quotaWorker = null;
let _webhookWorker = null;
let _dependencyWorker = null;
let _permissionWorker = null;
let _accountSyncWorker = null;
let _escalationWorker = null;

// ── Dispatch ctx — shared with the FSM and the signal-dispatch module ──────

function _buildCtx(ck) {
  return {
    validate: (from, to, event) => {
      if (ck && typeof ck.validateDomainTransition === 'function') {
        return ck.validateDomainTransition('graph-capability', from, to, event);
      }
      return { allowed: true };
    },
    dispatchGlobal: (event) => {
      if (ck && typeof ck.dispatch === 'function') {
        return ck.dispatch(event);
      }
      return { allowed: false, reason: 'CK not available' };
    },
    getGlobalState: () => {
      if (ck && typeof ck.getState === 'function') {
        return ck.getState();
      }
      return 'UNKNOWN';
    },
    sanityCheck: async () => ({ allowed: true }),
  };
}

/**
 * Install the graph-capability kernel into the runtime.
 * Wires FSM to CK, registers all 7 membranes, starts the substrate.
 */
function install({ ck } = {}) {
  if (_installed) {
    if (ck === _lastBoundCk) {
      return {
        fsm,
        started: _started,
        healthStarted: health.isStarted ? health.isStarted() : false,
      };
    }
    const ctx = _buildCtx(ck);
    fsm.setDispatchCtx(ctx);
    fsm.setGovernance(ck);
    signalDispatch.bindFsm(fsm, ctx);
    _lastBoundCk = ck;
    return {
      fsm,
      started: _started,
      healthStarted: health.isStarted ? health.isStarted() : false,
    };
  }

  // 1. Build the dispatch ctx
  const ctx = _buildCtx(ck);

  // 2. Wire the FSM to the constitutional kernel and ctx
  fsm.setDispatchCtx(ctx);
  fsm.setGovernance(ck);

  // 3. Bind signal-dispatch to FSM + ctx
  signalDispatch.bindFsm(fsm, ctx);

  // 4. Register all 7 delegated executor membranes with the FSM.
  fsm.setMembrane('health', { substrate: healthSubstrate });

  // Pass 2: new worker membranes (each implements start/stop/isStarted)
  _quotaWorker = new QuotaIntelligenceWorker();
  _webhookWorker = new WebhookSyncWorker();
  _dependencyWorker = new DependencyRecoveryWorker();
  _permissionWorker = new PermissionRecoveryWorker();
  _accountSyncWorker = new AccountSyncWorker();
  _escalationWorker = new EscalationWorker();

  fsm.setMembrane('quota-intelligence', { substrate: _quotaWorker });
  fsm.setMembrane('webhook-sync', { substrate: _webhookWorker });
  fsm.setMembrane('dependency-recovery', { substrate: _dependencyWorker });
  fsm.setMembrane('permission-recovery', { substrate: _permissionWorker });
  fsm.setMembrane('account-sync', { substrate: _accountSyncWorker });
  fsm.setMembrane('escalation', { substrate: _escalationWorker });

  // 5. Register capability-check workers with CK's invokeWorker gate.
  //    This enables ctx.invokeWorker() inside the FSM's CAPABILITY_CHECK
  //    buildActions — CK validates ownership, contract, and system sanity
  //    before execution, and emits WORKER_RESULT to the observability ledger.
  if (ck && typeof ck.registerWorker === 'function') {
    ck.registerWorker('graph-capability', 'credential-capability', credentialCapWorker);
    ck.registerWorker('graph-capability', 'quota-intelligence', quotaIntWorker);
  }

  // 6. Wire the evaluation worker — subscribes to CAPABILITY_EVALUATION_STARTED
  //    to trigger re-inference when vault state changes.
  const evaluationWorker = require('./substrates/workers/evaluation-worker');
  evaluationWorker.start(ck);

  // 7. Start the graph-capability substrate (binding only)
  const result = wiring.install({ ck });
  _started = result.started;
  _installed = true;
  _lastBoundCk = ck;

  console.log('[graph-capability-kernel] 7 membranes registered: health, quota-intelligence, webhook-sync, dependency-recovery, permission-recovery, account-sync, escalation');

  return { fsm, started: _started, healthStarted: health.isStarted ? health.isStarted() : false };
}

function uninstall() {
  wiring.uninstall();
  signalDispatch.bindFsm(null, null);
  fsm.setDispatchCtx(null);
  fsm.setGovernance(null);
  fsm.resetMembrane();
  try {
    const ev = require('./substrates/workers/evaluation-worker');
    ev.stop();
  } catch (_) {}
  _quotaWorker = null;
  _webhookWorker = null;
  _dependencyWorker = null;
  _permissionWorker = null;
  _accountSyncWorker = null;
  _escalationWorker = null;
  _installed = false;
  _started = false;
  _lastBoundCk = null;
}

function isInstalled() {
  return _installed;
}

module.exports = {
  install,
  uninstall,
  isInstalled,
  vault,
  health,
  fsm,
};
