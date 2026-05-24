// control-plane/signal-bus.js
// Signal Bus: decouples event emitters from subscribers.
//
// Owns: topic subscription management, event dispatch.
// Does NOT own: business logic, evaluation, persistence.
//
// Usage:
//   signalBus.subscribe('db:insert', handler)
//   signalBus.emit('db:insert', data)
//
// Topics:
//   'db:insert' — fired by realtime substrate on DB INSERT events
//     payload: { accountId: string, table: string, record: object }

class SignalBus {
  constructor() {
    this._handlers = new Map(); // topic → Set<Function>
  }

  subscribe(topic, handler) {
    if (!this._handlers.has(topic)) {
      this._handlers.set(topic, new Set());
    }
    this._handlers.get(topic).add(handler);
  }

  unsubscribe(topic, handler) {
    const handlers = this._handlers.get(topic);
    if (handlers) {
      handlers.delete(handler);
    }
  }

  emit(topic, data) {
    const handlers = this._handlers.get(topic);
    if (!handlers) return;
    for (const handler of handlers) {
      try {
        handler(data);
      } catch (err) {
        console.error(`[signal-bus] Handler error on topic ${topic}:`, err.message);
      }
    }
  }
}

const bus = new SignalBus();

module.exports = bus;
