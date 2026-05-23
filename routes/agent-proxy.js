// backend.api/routes/agent-proxy.js
// Primary router for all Python/LangChain agent → backend calls.
// Mounted at /api/instagram in server.js.
//
// Agent transport (from agent/config.py):
//   Base URL : BACKEND_API_URL  (http://instagram-backend:3001 in Docker)
//   Auth     : X-API-Key header (value = AGENT_API_KEY env var)
//   Tracking : X-User-ID header (value = "agent-service")
//   Timeout  : 8 s per request, 2x retry via tenacity
//
// Endpoint ownership (by domain module in routes/agents/):
//   ugc.js        → POST /search-hashtag, GET /tags, POST /repost-ugc, POST /sync-ugc
//   engagement.js → GET  /post-comments, GET /conversations, GET /conversation-messages
//                   POST /reply-comment, POST /reply-dm
//   publishing.js → POST /publish-post
//   analytics.js  → GET  /insights, GET /account-insights, GET /media-insights
//   oversight.js  → POST /oversight/chat  (SSE streaming proxy to Python agent)
//
// Shared utilities:
//   helpers/agent-helpers.js → resolveAccountCredentials, ensureMediaRecord,
//                               syncHashtagsFromCaptions, handleInsightsRequest

const express = require('express');
const router = express.Router();
const { validateAgentOrUserAuth } = require('../middleware/agent-auth');

// ── Authentication ─────────────────────────────────────────────────────────────
// validateAgentOrUserAuth accepts X-API-Key (agent) OR Authorization: Bearer JWT (frontend).
// JWT path verifies the requesting user owns the business_account_id in the request.
// Applied once here so every sub-router inherits it — no per-route middleware needed.
router.use(validateAgentOrUserAuth);

// ── Domain sub-routers ─────────────────────────────────────────────────────────
// Imported from routes/agents/ — each file owns one functional domain.
// Express matches routes in registration order; conflicts resolve to first match.
//
// NOTE: oversight.js is intentionally NOT here. The oversight/chat endpoint is a
// frontend → backend → agent flow. It is mounted directly in server.js at
// /api/instagram BEFORE this router, without agent key auth.
router.use(require('./agents/ugc'));
router.use(require('./agents/engagement'));
router.use(require('./agents/publishing'));
router.use(require('./agents/analytics'));
router.use(require('./agents/queue'));
router.use(require('./agents/heartbeat'));

module.exports = router;
