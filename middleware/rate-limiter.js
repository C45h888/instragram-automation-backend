// middleware/rate-limiter.js
// Instagram API rate limiting — decomposed into constitutional architecture.
//
// IP throttling: in-memory (no DB needed).
// User rate limiting: CK.dispatch(API_RATE_LIMIT_CHECK) → GCFSM (policy owner)
//   → governedRead('db.api-usage') → postgres-telemetry kernel → Supabase
//   → API_RATE_LIMIT_RESULT → back to middleware.
// API call logging: CK.dispatch(DB_WRITE_REQUESTED) → persist-telemetry FSM
//   → api-usage-writer → Supabase (fire-and-forget).
//
// No direct Supabase access in this file.

const constitutionalKernel = require('../control-plane/governance/constitutional-kernel');
const crypto = require('crypto');

// ==========================================
// IN-MEMORY IP THROTTLING
// ==========================================

const ipThrottleMap = new Map();

const IP_LIMIT_PER_MINUTE = 60;
const IP_WINDOW_MS = 60 * 1000;

setInterval(() => {
  const now = Date.now();
  let cleanedCount = 0;
  for (const [ip, data] of ipThrottleMap.entries()) {
    if (now > data.resetTime) {
      ipThrottleMap.delete(ip);
      cleanedCount++;
    }
  }
  if (cleanedCount > 0) {
    console.log(`🧹 Cleaned ${cleanedCount} expired IP throttle entries (Map size: ${ipThrottleMap.size})`);
  }
}, 5 * 60 * 1000);

function throttleByIP(req, res, next) {
  const clientIp = req.ip ||
                   req.headers['x-forwarded-for']?.split(',')[0] ||
                   req.socket?.remoteAddress ||
                   'unknown';
  const now = Date.now();

  if (ipThrottleMap.has(clientIp)) {
    const data = ipThrottleMap.get(clientIp);
    if (now > data.resetTime) {
      ipThrottleMap.set(clientIp, { count: 1, resetTime: now + IP_WINDOW_MS });
      return next();
    }
    if (data.count >= IP_LIMIT_PER_MINUTE) {
      const retryAfter = Math.ceil((data.resetTime - now) / 1000);
      console.warn(`⚠️  IP throttle limit exceeded: ${clientIp}`);
      return res.status(429).json({
        error: 'Too many requests',
        code: 'IP_THROTTLE_EXCEEDED',
        retry_after: retryAfter,
        limit: IP_LIMIT_PER_MINUTE,
        window: '60 seconds',
        message: `Rate limit: ${IP_LIMIT_PER_MINUTE} requests per minute per IP`
      });
    }
    data.count++;
  } else {
    ipThrottleMap.set(clientIp, { count: 1, resetTime: now + IP_WINDOW_MS });
  }
  next();
}

// ==========================================
// DATABASE-BACKED USER RATE LIMITING (constitutional)
// ==========================================

const INSTAGRAM_API_LIMIT = 200;

/**
 * Check Instagram API rate limit through constitutional flow:
 *   CK.dispatch(API_RATE_LIMIT_CHECK) → GCFSM → governedRead → worker → Supabase
 *   → API_RATE_LIMIT_RESULT action → Promise resolves
 */
async function checkInstagramRateLimit(req, res, next) {
  try {
    const userId = req.user?.id ||
                   req.user?.user_id ||
                   req.body?.user_id ||
                   req.query?.user_id;

    if (!userId) {
      return next();
    }

    // Dispatch through CK → GCFSM (policy owner) → postgres-telemetry
    const result = await new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        resolve({ allowed: true, current: 0, limit: INSTAGRAM_API_LIMIT, remaining: INSTAGRAM_API_LIMIT });
      }, 5000); // 5s timeout — fail open

      const handler = (action) => {
        if (action.userId !== userId) return;
        clearTimeout(timer);
        resolve(action);
      };

      constitutionalKernel.subscribeAction('API_RATE_LIMIT_RESULT', handler);

      const dispatchResult = constitutionalKernel.dispatch({
        type: 'API_RATE_LIMIT_CHECK',
        userId,
        limit: INSTAGRAM_API_LIMIT,
      });

      if (!dispatchResult.allowed) {
        clearTimeout(timer);
        resolve({ allowed: true, current: 0, limit: INSTAGRAM_API_LIMIT, remaining: INSTAGRAM_API_LIMIT });
      }
    });

    if (!result.allowed) {
      const now = new Date();
      const hourBucket = new Date(now);
      hourBucket.setMinutes(0, 0, 0);
      const resetTime = new Date(hourBucket);
      resetTime.setHours(resetTime.getHours() + 1);
      const retryAfter = Math.ceil((resetTime - now) / 1000);

      console.warn(`⚠️  Instagram API rate limit EXCEEDED for user: ${userId}`);
      return res.status(429).json({
        error: 'Instagram API rate limit exceeded',
        code: 'INSTAGRAM_API_LIMIT_EXCEEDED',
        retry_after: retryAfter,
        limit: result.limit,
        current: result.current,
        window: '1 hour',
        reset_time: resetTime.toISOString(),
        message: `Rate limit: ${INSTAGRAM_API_LIMIT} Instagram API calls per hour.`
      });
    }

    req.rateLimitRemaining = result.remaining;
    next();
  } catch (error) {
    console.error('❌ Rate limit check exception:', error.message);
    // Fail open
    next();
  }
}

// ==========================================
// API USAGE LOGGING (constitutional)
// ==========================================

/**
 * Log API call through constitutional flow:
 *   CK.dispatch(DB_WRITE_REQUESTED) → persist-telemetry FSM → api-usage-writer
 *   → Supabase upsert (fire-and-forget)
 */
async function logInstagramAPICall(req, res, _next) {
  const userId = req.user?.id ||
                 req.user?.user_id ||
                 req.body?.user_id;

  const rawBusinessAccountId = req.body?.business_account_id ||
                               req.query?.business_account_id ||
                               req.user?.business_account_id ||
                               req.query?.businessAccountId ||
                               req.body?.businessAccountId;

  const isValidUuid = (v) => v && typeof v === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(v);
  const businessAccountId = isValidUuid(rawBusinessAccountId) ? rawBusinessAccountId : null;

  if (!userId) return;

  try {
    const now = new Date();
    const hourBucket = new Date(now);
    hourBucket.setMinutes(0, 0, 0);

    constitutionalKernel.dispatch({
      type: 'DB_WRITE_REQUESTED',
      domain: 'middleware-rate-limiter',
      accountId: userId,
      table: 'api_usage',
      operation: 'log_api_request',
      rows: [{
        userId,
        businessAccountId,
        endpoint: req.path || req.url,
        method: req.method,
        hourBucket: hourBucket.toISOString(),
        statusCode: res.statusCode,
        success: res.statusCode >= 200 && res.statusCode < 400,
      }],
    });
  } catch (error) {
    console.error('❌ API call logging error:', error.message);
  }
}

// ==========================================
// COMBINED MIDDLEWARE & UTILITIES
// ==========================================

function instagramAPIRateLimiter(req, res, next) {
  throttleByIP(req, res, (err) => {
    if (err) return next(err);
    checkInstagramRateLimit(req, res, (err) => {
      if (err) return next(err);
      next();
    });
  });
}

function logAfterResponse(req, res, next) {
  const originalSend = res.send;
  res.send = function(data) {
    res.send = originalSend;
    setImmediate(() => {
      logInstagramAPICall(req, res, () => {});
    });
    return originalSend.call(this, data);
  };
  next();
}

module.exports = {
  throttleByIP,
  checkInstagramRateLimit,
  logInstagramAPICall,
  instagramAPIRateLimiter,
  logAfterResponse,
  IP_LIMIT_PER_MINUTE,
  INSTAGRAM_API_LIMIT
};
