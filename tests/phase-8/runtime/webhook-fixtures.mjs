// Phase 8 — Webhook fixture client.
// Talks to webhook-simulator (9200) + control (9201).
// Targets resolved from env so it works inside docker
// (WEBHOOK_SIMULATOR_HOST) and outside (localhost fallback).

import http from 'node:http';

const DELIVERY_HOST = process.env.WEBHOOK_SIMULATOR_HOST || 'localhost';
const DELIVERY_PORT = parseInt(process.env.WEBHOOK_SIMULATOR_PORT || '9200', 10);
const CONTROL_PORT  = parseInt(process.env.WEBHOOK_CONTROL_PORT  || '9201', 10);

function request(host, port, method, path, body) {
  return new Promise((resolve, reject) => {
    const payload = body == null ? '' : JSON.stringify(body);
    const req = http.request({
      host, port, method, path,
      headers: payload
        ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) }
        : {},
    }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8');
        let parsed = null;
        try { parsed = text ? JSON.parse(text) : null; } catch (_) { parsed = text; }
        resolve({ status: res.statusCode, body: parsed });
      });
    });
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

export const WebhookFixtures = {
  host:  DELIVERY_HOST,
  port:  DELIVERY_PORT,
  ctrl:  CONTROL_PORT,

  async health()    { return request(DELIVERY_HOST, DELIVERY_PORT, 'GET',  '/health'); },
  async list()      { return request(DELIVERY_HOST, DELIVERY_PORT, 'GET',  '/v1/webhook/fixtures'); },
  async deliver(fixtureName) {
    return request(DELIVERY_HOST, DELIVERY_PORT, 'POST', `/v1/webhook/${fixtureName}`);
  },
  async replay()    { return request(DELIVERY_HOST, DELIVERY_PORT, 'POST', '/v1/webhook/replay'); },
  async inject(scenario, fixture, count = 1) {
    return request(DELIVERY_HOST, CONTROL_PORT, 'POST', '/control/inject', { scenario, fixture, count });
  },
  async clear()     { return request(DELIVERY_HOST, CONTROL_PORT, 'POST', '/control/clear'); },
  async deliveries(){ return request(DELIVERY_HOST, CONTROL_PORT, 'GET',  '/control/deliveries'); },
  async reset()     { return request(DELIVERY_HOST, CONTROL_PORT, 'POST', '/control/reset'); },
};

export default WebhookFixtures;
