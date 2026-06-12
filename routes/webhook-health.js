// routes/webhook-health.js
// Debug read surface for the webhook acquisition subsystem.
//
// Owns: read-only observability of in-memory state.
// Does NOT own: ingestion, persistence, dispatch, worker logic.
//
// Endpoints:
//   GET /webhook/staged-events   — staged event count + sample per account
//   GET /webhook/buffer-state     — pending batch buffer state (future batching layer)

const express = require('express');
const router = express.Router();

const acquisitionFsm = require('../acquisition-kernel/fsm');

router.get('/staged-events', (req, res) => {
  try {
    const accountId = req.query.accountId || null;
    if (accountId) {
      return res.status(200).json({
        accountId,
        events: acquisitionFsm.getStagedEvents(accountId),
      });
    }
    return res.status(200).json({
      state: acquisitionFsm.getState(),
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

module.exports = router;