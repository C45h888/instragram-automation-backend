// control-plane/observability/context.js
// Runtime Context: causal chain propagation for observability plane.
//
// Owns: generating and threading traceId, correlationId, causationId through
//        every orchestration → execution chain in the runtime.
//
// Does NOT own: business logic, state transitions, policy decisions.
// The context is a passive carrier — it observes the chain without influencing it.
//
// Usage:
//   const ctx = createContext({ domain: 'acquisition', authority: 'retry-worker' })
//   withContext(ctx, async () => { await doWork() }) // propagates context
//   getCurrentContext() // returns the active context for this call chain
//
// Thread-safety note:
//   This uses a simple module-level stack. In a single-threaded Node.js event loop
//   this is safe — contexts are always synchronous and don't leak across event loop ticks.

const crypto = require('crypto');

// Module-level context stack — last element is current
const _stack = [];

/**
 * Generate a new runtime context.
 *
 * @param {object} overrides — fields to set on the new context
 * @param {string} [overrides.correlationId] — groups related execution chains
 * @param {string} [overrides.causationId] — the originating event/intent ID
 * @param {string} [overrides.domain] — 'acquisition'|'publishing'|'scheduling'|'execution'|'governance'|'realtime'
 * @param {string} [overrides.authority] — the subsystem initiating this chain
 * @returns {object} a new context object
 */
function createContext(overrides = {}) {
  return {
    traceId: overrides.traceId || crypto.randomUUID(),
    correlationId: overrides.correlationId || null,
    causationId: overrides.causationId || null,
    domain: overrides.domain || null,
    authority: overrides.authority || null,
  };
}

/**
 * Push a context onto the current thread's context stack.
 * All subsequent getCurrentContext() calls return this context until it's popped.
 *
 * @param {object} ctx — context from createContext()
 */
function pushContext(ctx) {
  _stack.push(ctx);
}

/**
 * Pop the current context from the stack.
 * Restores the previous context as active.
 */
function popContext() {
  _stack.pop();
}

/**
 * Returns the currently active context for this call chain.
 * Returns a default context if no context has been pushed.
 *
 * @returns {object}
 */
function getCurrentContext() {
  if (_stack.length > 0) {
    return _stack[_stack.length - 1];
  }
  // Return a default root context for calls not yet threaded
  return {
    traceId: crypto.randomUUID(),
    correlationId: null,
    causationId: null,
    domain: null,
    authority: null,
  };
}

/**
 * Execute a function within a given context. The context becomes active
 * for the duration of the call, then the previous context is restored.
 *
 * @param {object} ctx — context from createContext()
 * @param {Function} fn — async or sync function to execute
 * @returns {*} the return value of fn
 */
async function withContext(ctx, fn) {
  pushContext(ctx);
  try {
    return await fn();
  } finally {
    popContext();
  }
}

/**
 * Execute a sync function within a given context.
 * @param {object} ctx
 * @param {Function} fn — sync function to execute
 * @returns {*} the return value of fn
 */
function withContextSync(ctx, fn) {
  pushContext(ctx);
  try {
    return fn();
  } finally {
    popContext();
  }
}

module.exports = {
  createContext,
  pushContext,
  popContext,
  getCurrentContext,
  withContext,
  withContextSync,
};
