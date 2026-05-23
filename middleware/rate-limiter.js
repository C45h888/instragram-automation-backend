// backend.api/middleware/rate-limiter.js
const { getSupabaseAdmin } = require('../config/supabase');

// ==========================================
// IN-MEMORY IP THROTTLING
// ==========================================
// Fast protection against abuse/DDoS before hitting database
// Uses Map for O(1) lookups and updates

const ipThrottleMap = new Map(); // Structure: { ip: { count, resetTime } }

// Configuration
const IP_LIMIT_PER_MINUTE = 60; // 60 requests per minute = 1 req/second avg
const IP_WINDOW_MS = 60 * 1000; // 1 minute window

/**
 * Automatic cleanup of expired IP entries
 * Runs every 5 minutes to prevent memory leaks
 *
 * Without this, the Map would grow indefinitely as IPs are added
 * With typical load, Map size stays under 1000 entries
 */
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
}, 5 * 60 * 1000); // Every 5 minutes

/**
 * IP-based throttling middleware (in-memory)
 *
 * Fast protection layer before database checks
 * Limits requests per IP to prevent abuse and DDoS
 *
 * Uses sliding window: counts requests in last 60 seconds
 * Resets automatically when window expires
 *
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 * @param {Function} next - Express next middleware function
 */
function throttleByIP(req, res, next) {
  // Extract client IP from various sources
  // req.ip is set by Express with trust proxy enabled
  const clientIp = req.ip ||
                   req.headers['x-forwarded-for']?.split(',')[0] ||
                   req.socket?.remoteAddress ||
                   'unknown';

  const now = Date.now();

  if (ipThrottleMap.has(clientIp)) {
    const data = ipThrottleMap.get(clientIp);

    // ===== Check if window expired (sliding window) =====
    if (now > data.resetTime) {
      // Reset counter for new window
      ipThrottleMap.set(clientIp, {
        count: 1,
        resetTime: now + IP_WINDOW_MS
      });
      return next();
    }

    // ===== Check if limit exceeded =====
    if (data.count >= IP_LIMIT_PER_MINUTE) {
      const retryAfter = Math.ceil((data.resetTime - now) / 1000); // seconds

      console.warn(`⚠️  IP throttle limit exceeded: ${clientIp}`);
      console.warn(`   Count: ${data.count}/${IP_LIMIT_PER_MINUTE} in current window`);

      return res.status(429).json({
        error: 'Too many requests',
        code: 'IP_THROTTLE_EXCEEDED',
        retry_after: retryAfter,
        limit: IP_LIMIT_PER_MINUTE,
        window: '60 seconds',
        message: `Rate limit: ${IP_LIMIT_PER_MINUTE} requests per minute per IP`
      });
    }

    // ===== Increment counter =====
    data.count++;
  } else {
    // First request from this IP in current window
    ipThrottleMap.set(clientIp, {
      count: 1,
      resetTime: now + IP_WINDOW_MS
    });
  }

  // IP throttle passed, continue to next middleware
  next();
}

// ==========================================
// DATABASE-BACKED USER RATE LIMITING
// ==========================================
// Accurate, persistent tracking for Instagram API limits
// Uses existing api_usage table with hour_bucket design

const INSTAGRAM_API_LIMIT = 200; // Calls per hour per user (Meta 2025 limit)

/**
 * Check if user has exceeded Instagram API rate limit
 *
 * Uses existing api_usage table with hour_bucket design for efficiency:
 * - hour_bucket rounds timestamps to hour start (14:23:45 → 14:00:00)
 * - Single row per user/endpoint/hour reduces query complexity
 * - Indexed queries are fast even with millions of rows
 *
 * Query logic:
 * 1. Calculate current hour_bucket (round to hour start)
 * 2. Sum request_count for all user's calls in current hour
 * 3. Compare against limit (200)
 * 4. Return 429 if exceeded, or continue with remaining count
 *
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 * @param {Function} next - Express next middleware function
 */
async function checkInstagramRateLimit(req, res, next) {
  try {
    // ===== STEP 1: Extract user ID from request =====
    // Assumes authentication middleware has set req.user
    // Fallback to query/body params for testing
    const userId = req.user?.id ||
                   req.user?.user_id ||
                   req.body?.user_id ||
                   req.query?.user_id;

    if (!userId) {
      // No user ID - skip rate limiting for public endpoints
      console.log('ℹ️  No user ID found - skipping rate limit check');
      return next();
    }

    // ===== STEP 2: Get Supabase client =====
    const supabase = getSupabaseAdmin();
    if (!supabase) {
      console.warn('⚠️  Supabase not available - skipping rate limit check (fail open)');
      return next();
    }

    // ===== STEP 3: Calculate current hour bucket =====
    // Hour bucket rounds to hour start for efficient grouping
    // Example: 2025-01-15 14:23:45 → 2025-01-15 14:00:00
    const now = new Date();
    const hourBucket = new Date(now);
    hourBucket.setMinutes(0, 0, 0); // Set to hour start

    console.log(`🔍 Rate limit check for user ${userId}`);
    console.log(`   Hour bucket: ${hourBucket.toISOString()}`);

    // ===== STEP 4: Query total API calls in current hour =====
    // Uses >= hour_bucket to get current hour's requests
    // May include multiple rows for different endpoints
    const { data, error } = await supabase
      .from('api_usage')
      .select('request_count')
      .eq('user_id', userId)
      .gte('hour_bucket', hourBucket.toISOString());

    if (error) {
      console.error('❌ Rate limit check database error:', error);
      console.error('   Failing open - allowing request');
      // Fail open: Don't block on database errors
      return next();
    }

    // ===== STEP 5: Sum request counts across all endpoints =====
    const totalRequests = data?.reduce((sum, row) => {
      return sum + (row.request_count || 0);
    }, 0) || 0;

    console.log(`   Total requests in current hour: ${totalRequests}/${INSTAGRAM_API_LIMIT}`);

    // ===== STEP 6: Check if limit exceeded =====
    if (totalRequests >= INSTAGRAM_API_LIMIT) {
      // Calculate when the limit resets (next hour)
      const resetTime = new Date(hourBucket);
      resetTime.setHours(resetTime.getHours() + 1);
      const retryAfter = Math.ceil((resetTime - now) / 1000); // seconds

      console.warn(`⚠️  Instagram API rate limit EXCEEDED for user: ${userId}`);
      console.warn(`   Current: ${totalRequests}/${INSTAGRAM_API_LIMIT}`);
      console.warn(`   Resets in: ${retryAfter} seconds`);

      return res.status(429).json({
        error: 'Instagram API rate limit exceeded',
        code: 'INSTAGRAM_API_LIMIT_EXCEEDED',
        retry_after: retryAfter,
        limit: INSTAGRAM_API_LIMIT,
        current: totalRequests,
        window: '1 hour',
        reset_time: resetTime.toISOString(),
        message: `Rate limit: ${INSTAGRAM_API_LIMIT} Instagram API calls per hour. Limit resets at ${resetTime.toISOString()}`
      });
    }

    // ===== STEP 7: Add remaining count to request object =====
    // Route handlers can include this in API responses
    req.rateLimitRemaining = INSTAGRAM_API_LIMIT - totalRequests;

    console.log(`✅ Rate limit check PASSED: ${req.rateLimitRemaining} requests remaining`);

    next();
  } catch (error) {
    console.error('❌ Rate limit check exception:', error);
    console.error('   Error type:', error.constructor.name);
    console.error('   Error message:', error.message);
    // Fail open: Don't block on unexpected errors
    console.warn('⚠️  Failing open - allowing request due to exception');
    next();
  }
}

// ==========================================
// API USAGE LOGGING
// ==========================================
// Logs API calls to database for analytics and rate limit tracking
// Uses upsert with unique constraint to handle concurrent requests

/**
 * Log Instagram API call to database
 *
 * Updates api_usage table for rate limiting and analytics
 * Uses upsert pattern with unique constraint to handle concurrent requests:
 *
 * Unique constraint on (user_id, business_account_id, endpoint, method, hour_bucket)
 * - First request: INSERT new row with request_count = 1
 * - Concurrent requests: UPDATE existing row, increment request_count
 * - Database handles race conditions automatically
 *
 * This function is called AFTER response is sent (non-blocking)
 * Errors in logging don't affect the API response
 *
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 * @param {Function} next - Express next middleware function
 */
async function logInstagramAPICall(req, res, next) {
  // ===== STEP 1: Extract tracking data from request =====
  const userId = req.user?.id ||
                 req.user?.user_id ||
                 req.body?.user_id;

  // Support both snake_case (backend convention) and camelCase (some frontend callers)
  // Also validate UUID type before inserting — numeric media IDs must not reach this column
  const rawBusinessAccountId = req.body?.business_account_id ||
                               req.query?.business_account_id ||
                               req.user?.business_account_id ||
                               req.query?.businessAccountId ||
                               req.body?.businessAccountId;

  // UUID validation: reject numeric Instagram IDs that would violate the column type
  const isValidUuid = (v) => v && typeof v === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(v);
  const businessAccountId = isValidUuid(rawBusinessAccountId) ? rawBusinessAccountId : null;

  if (!userId) {
    // No user to track - skip logging
    return next();
  }

  try {
    const supabase = getSupabaseAdmin();
    if (!supabase) {
      console.warn('⚠️  Supabase not available - skipping API call logging');
      return next();
    }

    // ===== STEP 2: Prepare logging data =====
    const now = new Date();
    const hourBucket = new Date(now);
    hourBucket.setMinutes(0, 0, 0); // Round to hour start

    const endpoint = req.path || req.url;
    const method = req.method;
    const statusCode = res.statusCode;
    const success = statusCode >= 200 && statusCode < 400;

    // ===== STEP 3: Upsert to api_usage table =====
    // Unique constraint handles concurrent requests automatically
    // If row exists: request_count is incremented by database trigger
    // If row doesn't exist: new row created with request_count = 1
    const { error } = await supabase
      .from('api_usage')
      .upsert({
        user_id: userId,
        business_account_id: businessAccountId,
        endpoint,
        method,
        hour_bucket: hourBucket.toISOString(),
        request_count: 1, // Will be incremented by trigger if row exists
        status_code: statusCode,
        success,
        created_at: now.toISOString(),
        updated_at: now.toISOString()
      }, {
        onConflict: 'user_id,business_account_id,endpoint,method,hour_bucket',
        // ignoreDuplicates: false ensures update happens
      });

    if (error) {
      // Log error but don't fail the request
      console.error('❌ API call logging error:', error);
      console.error('   User:', userId);
      console.error('   Endpoint:', endpoint);
    } else {
      console.log(`📊 API call logged: ${method} ${endpoint} (${statusCode}) - User: ${userId}`);
    }
  } catch (error) {
    // Catch any unexpected errors
    console.error('❌ API call logging exception:', error);
    console.error('   Error type:', error.constructor.name);
  }

  // Always continue - logging errors shouldn't break the API
  next();
}

// ==========================================
// COMBINED MIDDLEWARE & UTILITIES
// ==========================================

/**
 * Combined rate limiting middleware
 *
 * Chains all three layers in correct order:
 * 1. IP throttling (fast, in-memory)
 * 2. User rate limiting (database check)
 * 3. Continue to route handler
 * 4. (After response) Log API call
 *
 * Apply to Instagram API routes that need rate limiting
 *
 * Usage:
 *   router.use(instagramAPIRateLimiter);
 *
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 * @param {Function} next - Express next middleware function
 */
function instagramAPIRateLimiter(req, res, next) {
  // Layer 1: IP throttling
  throttleByIP(req, res, (err) => {
    if (err) return next(err);

    // Layer 2: User rate limiting
    checkInstagramRateLimit(req, res, (err) => {
      if (err) return next(err);

      // Both checks passed - continue to route handler
      next();
    });
  });
}

/**
 * Middleware to log API call after response is sent
 *
 * This wraps res.send() to log after the response is complete
 * Ensures logging doesn't delay the API response
 *
 * Usage:
 *   router.use(logAfterResponse);
 *
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 * @param {Function} next - Express next middleware function
 */
function logAfterResponse(req, res, next) {
  // Capture the original send function
  const originalSend = res.send;

  // Override res.send to log after sending
  res.send = function(data) {
    // Restore original send first
    res.send = originalSend;

    // Log asynchronously (fire and forget)
    // This won't delay the response
    setImmediate(() => {
      logInstagramAPICall(req, res, () => {
        // Logging complete (or failed silently)
      });
    });

    // Send the response
    return originalSend.call(this, data);
  };

  next();
}

// ==========================================
// EXPORTS
// ==========================================

module.exports = {
  // Individual middleware functions
  throttleByIP,
  checkInstagramRateLimit,
  logInstagramAPICall,

  // Combined middleware for easy use
  instagramAPIRateLimiter,
  logAfterResponse,

  // Configuration constants (for testing)
  IP_LIMIT_PER_MINUTE,
  INSTAGRAM_API_LIMIT
};
