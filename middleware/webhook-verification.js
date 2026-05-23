// backend.api/middleware/webhook-verification.js
const crypto = require('crypto');

/**
 * Middleware to verify Meta Instagram webhook signatures
 *
 * Instagram webhooks use HMAC-SHA1 with X-Hub-Signature header
 * Format: "sha1=<signature>"
 *
 * SECURITY: This middleware prevents unauthorized webhook events from being processed.
 * Instagram signs each webhook event with HMAC-SHA1 using your app secret.
 * We must verify this signature before processing any webhook data.
 *
 * Reference: https://developers.facebook.com/docs/graph-api/webhooks/getting-started
 *
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 * @param {Function} next - Express next middleware function
 */
function verifyInstagramWebhookSignature(req, res, next) {
  const signature = req.headers['x-hub-signature'];
  // ✅ v3: Use INSTAGRAM_APP_SECRET with fallback to META_APP_SECRET for compatibility
  const META_APP_SECRET = process.env.INSTAGRAM_APP_SECRET || process.env.META_APP_SECRET;

  // ===== STEP 1: Check if signature header exists =====
  if (!signature) {
    console.error('❌ Missing x-hub-signature header');
    console.error('   Request headers:', JSON.stringify(req.headers, null, 2));
    return res.status(401).json({
      error: 'Unauthorized',
      code: 'MISSING_SIGNATURE',
      message: 'X-Hub-Signature header required for webhook verification'
    });
  }

  // ===== STEP 2: Check if app secret is configured =====
  if (!META_APP_SECRET) {
    console.error('❌ META_APP_SECRET not configured');

    // In production, this is a critical configuration error
    if (process.env.NODE_ENV === 'production') {
      return res.status(500).json({
        error: 'Server configuration error',
        code: 'MISSING_APP_SECRET',
        message: 'META_APP_SECRET must be configured in production'
      });
    }

    // In development, allow requests through but log warning
    console.warn('⚠️  META_APP_SECRET not set - signature verification skipped (development only)');
    console.warn('   Set META_APP_SECRET in .env for full security testing');
    return next();
  }

  // ===== STEP 3: Get raw request body =====
  // CRITICAL: Express must be configured to preserve raw body
  // This is configured in server.js: app.use(express.json({ verify: (req, res, buf) => { req.rawBody = buf } }))
  const rawBody = req.rawBody;

  if (!rawBody) {
    console.error('❌ Raw body not available for signature verification');
    console.error('   Ensure express.json() middleware preserves rawBody in server.js');
    console.error('   Required configuration: app.use(express.json({ verify: (req, res, buf) => { req.rawBody = buf } }))');
    return res.status(500).json({
      error: 'Server configuration error',
      code: 'MISSING_RAW_BODY',
      message: 'Raw request body required for signature verification'
    });
  }

  try {
    // ===== STEP 4: Compute expected signature using SHA-1 =====
    // Instagram uses SHA-1 (Facebook uses SHA-256, different algorithms for different products)
    const expectedSignature = crypto
      .createHmac('sha1', META_APP_SECRET)  // ✅ SHA-1 for Instagram webhooks
      .update(rawBody)                       // Use raw Buffer, not parsed JSON
      .digest('hex');                        // Output as hexadecimal string

    // ===== STEP 5: Parse provided signature =====
    // Format: "sha1=<hex_signature>"
    // Some implementations might send just the signature without prefix
    const providedSignature = signature.startsWith('sha1=')
      ? signature.substring(5)  // Remove "sha1=" prefix
      : signature;              // Use as-is if no prefix

    // ===== STEP 6: Timing-safe comparison =====
    // Convert to Buffers for crypto.timingSafeEqual()
    const expectedBuffer = Buffer.from(expectedSignature, 'hex');
    const providedBuffer = Buffer.from(providedSignature, 'hex');

    // First check: Buffers must be same length
    if (expectedBuffer.length !== providedBuffer.length) {
      console.error('❌ Instagram webhook signature verification failed (length mismatch)');
      console.error('   Expected length:', expectedBuffer.length);
      console.error('   Provided length:', providedBuffer.length);
      return res.status(401).json({
        error: 'Unauthorized',
        code: 'INVALID_SIGNATURE',
        message: 'Webhook signature verification failed'
      });
    }

    // Timing-safe comparison prevents timing attacks
    // Regular === comparison could leak information about signature through response time
    const isValid = crypto.timingSafeEqual(expectedBuffer, providedBuffer);

    if (isValid) {
      console.log('✅ Instagram webhook signature verified (HMAC-SHA1)');
      console.log('   Signature:', providedSignature.substring(0, 16) + '...');
      next();  // Signature valid - proceed to webhook handler
    } else {
      console.error('❌ Instagram webhook signature verification failed');
      console.error('   Expected signature:', expectedSignature);
      console.error('   Provided signature:', providedSignature);
      console.error('   This could indicate:');
      console.error('   - Incorrect META_APP_SECRET configured');
      console.error('   - Webhook not sent from Instagram/Meta');
      console.error('   - Request body modified in transit');

      return res.status(401).json({
        error: 'Unauthorized',
        code: 'INVALID_SIGNATURE',
        message: 'Webhook signature verification failed'
      });
    }
  } catch (error) {
    // Catch any unexpected errors during verification
    console.error('❌ Error verifying webhook signature:', error);
    console.error('   Error type:', error.constructor.name);
    console.error('   Error message:', error.message);
    return res.status(500).json({
      error: 'Internal server error',
      code: 'VERIFICATION_ERROR',
      message: error.message
    });
  }
}

// Export middleware function for use in routes
module.exports = { verifyInstagramWebhookSignature };
