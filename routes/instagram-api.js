// backend.api/routes/instagram-api.js
// Thin router — mounts domain sub-routers under /api/instagram
const express = require('express');
const router = express.Router();
const { instagramAPIRateLimiter, logAfterResponse } = require('../middleware/rate-limiter');

// Rate limiting + response logging for ALL routes
router.use(instagramAPIRateLimiter);
router.use(logAfterResponse);

// Domain sub-routers (mirrors routes/agents/ convention)
router.use(require('./frontend/tokens'));
router.use(require('./frontend/media'));
router.use(require('./frontend/ugc'));
router.use(require('./frontend/sync'));
router.use(require('./frontend/inbox'));

// Health check (inline — zero dependencies)
router.get('/health', (req, res) => {
  res.json({
    status: 'operational',
    service: 'Instagram API Proxy',
    rate_limiting: 'enabled',
    timestamp: new Date().toISOString()
  });
});

module.exports = router;
