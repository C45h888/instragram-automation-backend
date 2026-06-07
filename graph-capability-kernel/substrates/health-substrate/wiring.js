// graph-capability-kernel/substrates/health-substrate/wiring.js
// Boot-time wiring for the health substrate.
// Mirrors graph-capability/wiring.js pattern.
//
// Per Layer 4 plan: health-substrate uses the SAME signal-dispatch as the
// vault substrates. No separate adapter. The only thing this wiring does
// is guard that signal-dispatch is bound (set by graph-capability/wiring.js
// at install time). It is a no-op when the canonical install order is
// followed (graph-capability first, health second).
//
// If a caller installs ONLY the health substrate (e.g. in a focused test),
// this wiring will issue a one-shot warning if signal-dispatch is not bound.
// That makes the contract explicit: the health substrate is a constitutional
// citizen only when the graph-capability substrate is installed first.
//
// Contract:
//   install() → { started: boolean }
//   uninstall() → void
//   isInstalled() → boolean

const substrate = require('./index');
const signalDispatch = require('../vault/signal-dispatch');

let _installed = false;

function install() {
  if (_installed) {
    return { started: substrate.isStarted() };
  }

  // Guard: signal-dispatch must be bound for the health substrate to be
  // a first-class constitutional citizen.
  if (!signalDispatch.getCk()) {
    console.warn('[health] wiring.install() called without signal-dispatch CK bound. ' +
      'Call graphCapabilityWiring.install({ck}) first so vault signal paths are governed. ' +
      'Health events will still run, but their signals will be dropped.');
  }

  substrate.start();
  _installed = true;
  console.log('[health] Wiring installed — substrate live');
  return { started: true };
}

function uninstall() {
  if (!_installed) return;
  substrate.stop();
  _installed = false;
  console.log('[health] Wiring uninstalled — substrate stopped');
}

function isInstalled() {
  return _installed;
}

module.exports = {
  install,
  uninstall,
  isInstalled,
};
