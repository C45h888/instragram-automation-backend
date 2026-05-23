// backend/routes/legal.js
const express = require('express');
const router = express.Router();
const crypto = require('crypto');

// Import legal content (you'll need to convert the TypeScript to JavaScript or use a build process)
// For now, we'll define it directly here for the backend
const LEGAL_CONTENT = {
  privacyPolicy: {
    version: "2.0",
    effectiveDate: "2025-01-01",
    lastUpdated: "2025-01-01",
    metaComplianceDate: "2025-02-03",
    title: "Privacy Policy - Instagram Automation Platform"
  },
  termsOfService: {
    version: "2.0",
    effectiveDate: "2025-01-01",
    lastUpdated: "2025-01-01",
    title: "Terms and Conditions - Instagram Automation Platform"
  },
  dataDeletion: {
    version: "2.0",
    effectiveDate: "2025-01-01",
    lastUpdated: "2025-01-01",
    title: "Data Deletion Policy - Instagram Automation Platform"
  }
};

// Helper function to generate HTML for Meta crawlers
const generateLegalHTML = (type, content) => {
  const structuredData = {
    "@context": "https://schema.org",
    "@type": type === 'privacy' ? "PrivacyPolicy" : "TermsOfService",
    "name": content.title,
    "publisher": {
      "@type": "Organization",
      "name": "888 Intelligence Automation",
      "url": "https://888intelligenceautomation.in"
    },
    "datePublished": content.effectiveDate,
    "dateModified": content.lastUpdated,
    "inLanguage": "en-US",
    "isAccessibleForFree": true
  };

  return `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>${content.title} | 888 Intelligence Automation</title>
    <meta name="description" content="Meta Platform Terms compliant privacy policy for Instagram automation services. Effective ${content.effectiveDate}.">
    
    <!-- Open Graph Meta Tags for Meta/Facebook -->
    <meta property="og:title" content="${content.title}">
    <meta property="og:type" content="article">
    <meta property="og:url" content="https://api.888intelligenceautomation.in/legal/${type}">
    <meta property="og:description" content="Comprehensive ${type} policy covering Instagram API usage, data processing, and user rights.">
    <meta property="og:site_name" content="888 Intelligence Automation">
    
    <!-- Twitter Card Meta Tags -->
    <meta name="twitter:card" content="summary">
    <meta name="twitter:title" content="${content.title}">
    <meta name="twitter:description" content="Meta compliant ${type} policy for Instagram automation platform.">
    
    <!-- Structured Data for Search Engines -->
    <script type="application/ld+json">
    ${JSON.stringify(structuredData, null, 2)}
    </script>
    
    <!-- Robots Meta Tag -->
    <meta name="robots" content="index, follow">
    
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background: #fff;
        }
        header {
            background: linear-gradient(to right, #1a202c, #2d3748);
            color: white;
            padding: 2rem;
            margin: -20px -20px 30px -20px;
        }
        h1 {
            margin: 0;
            font-size: 2rem;
        }
        .meta-info {
            margin-top: 1rem;
            opacity: 0.9;
            font-size: 0.9rem;
        }
        .content {
            background: white;
            padding: 2rem;
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }
        .section {
            margin-bottom: 2rem;
        }
        .contact-info {
            background: #f7fafc;
            padding: 1.5rem;
            border-radius: 8px;
            margin-top: 2rem;
        }
        a {
            color: #3b82f6;
            text-decoration: none;
        }
        a:hover {
            text-decoration: underline;
        }
    </style>
</head>
<body>
    <header>
        <h1>${content.title}</h1>
        <div class="meta-info">
            <p>Version: ${content.version} | Effective Date: ${content.effectiveDate}</p>
            <p>Meta Platform Compliance: ${content.metaComplianceDate || 'February 3, 2025'}</p>
        </div>
    </header>
    
    <main class="content">
        <section class="section">
            <h2>Overview</h2>
            <p>This ${type} policy is maintained by 888 Intelligence Automation for our Instagram Business Automation Platform. 
            We are committed to protecting your privacy and ensuring compliance with all applicable data protection laws including GDPR, CCPA, and Meta Platform Terms.</p>
        </section>
        
        <section class="section">
            <h2>Key Information</h2>
            <ul>
                <li><strong>Service Provider:</strong> 888 Intelligence Automation</li>
                <li><strong>Service:</strong> Instagram Business Automation Platform</li>
                <li><strong>Data Processing:</strong> Instagram Graph API Integration</li>
                <li><strong>Security:</strong> Cloudflare Tunnel + Supabase Encryption</li>
                <li><strong>Compliance:</strong> GDPR, CCPA, Meta Platform Terms</li>
            </ul>
        </section>
        
        <section class="section">
            <h2>Full Policy Document</h2>
            <p>For the complete ${type} policy, please visit our application at:</p>
            <p><a href="https://app.888intelligenceautomation.in/${type === 'privacy' ? 'privacy-policy' : type === 'terms' ? 'terms-of-service' : 'data-deletion'}">
                View Full ${content.title}
            </a></p>
        </section>
        
        <section class="section contact-info">
            <h2>Contact Information</h2>
            <p><strong>Data Protection Officer:</strong> <a href="mailto:privacy@888intelligenceautomation.in">privacy@888intelligenceautomation.in</a></p>
            <p><strong>Legal Department:</strong> <a href="mailto:legal@888intelligenceautomation.in">legal@888intelligenceautomation.in</a></p>
            <p><strong>General Support:</strong> <a href="mailto:support@888intelligenceautomation.in">support@888intelligenceautomation.in</a></p>
        </section>
    </main>
</body>
</html>
  `;
};

// Privacy Policy Route
router.get('/privacy-policy', (req, res) => {
  const userAgent = req.headers['user-agent'] || '';
  const isMetaCrawler = /facebookexternalhit|Facebot/i.test(userAgent);
  
  // Log Meta crawler access for monitoring
  if (isMetaCrawler) {
    console.log('📋 Meta crawler accessed privacy policy:', new Date().toISOString());
  }
  
  // Always serve HTML for legal pages (both for crawlers and browsers)
  res.set('Content-Type', 'text/html');
  res.send(generateLegalHTML('privacy', LEGAL_CONTENT.privacyPolicy));
});

// Terms of Service Route
router.get('/terms-of-service', (req, res) => {
  const userAgent = req.headers['user-agent'] || '';
  const isMetaCrawler = /facebookexternalhit|Facebot/i.test(userAgent);
  
  if (isMetaCrawler) {
    console.log('📋 Meta crawler accessed terms of service:', new Date().toISOString());
  }
  
  res.set('Content-Type', 'text/html');
  res.send(generateLegalHTML('terms', LEGAL_CONTENT.termsOfService));
});

// =====================================
// RATE LIMITING FOR DATA DELETION ENDPOINT
// =====================================
const deletionRequestTracker = new Map(); // IP -> { count, resetTime }
const RATE_LIMIT_WINDOW = 60 * 60 * 1000; // 1 hour in milliseconds
const MAX_REQUESTS_PER_HOUR = 10;

/**
 * Rate limiting middleware for deletion requests
 * Prevents abuse by limiting requests per IP address
 */
function rateLimitDeletionRequests(req, res, next) {
  const clientIp = req.ip || req.socket?.remoteAddress || 'unknown';
  const now = Date.now();

  if (deletionRequestTracker.has(clientIp)) {
    const tracker = deletionRequestTracker.get(clientIp);

    // Reset if window expired
    if (now > tracker.resetTime) {
      deletionRequestTracker.set(clientIp, {
        count: 1,
        resetTime: now + RATE_LIMIT_WINDOW
      });
      return next();
    }

    // Check if limit exceeded
    if (tracker.count >= MAX_REQUESTS_PER_HOUR) {
      const retryAfter = Math.ceil((tracker.resetTime - now) / 1000);
      console.warn(`⚠️ Rate limit exceeded for IP: ${clientIp}`);

      return res.status(429).json({
        error: 'Too many deletion requests',
        code: 'RATE_LIMIT_EXCEEDED',
        retry_after: retryAfter
      });
    }

    // Increment counter
    tracker.count++;
  } else {
    // First request from this IP
    deletionRequestTracker.set(clientIp, {
      count: 1,
      resetTime: now + RATE_LIMIT_WINDOW
    });
  }

  next();
}

// Clean up old entries every hour
setInterval(() => {
  const now = Date.now();
  for (const [ip, tracker] of deletionRequestTracker.entries()) {
    if (now > tracker.resetTime) {
      deletionRequestTracker.delete(ip);
    }
  }
}, RATE_LIMIT_WINDOW);

/**
 * Verifies Meta's HMAC-SHA256 signature using timing-safe comparison
 * @param {string} encodedSig - Base64-encoded signature from Meta
 * @param {string} encodedPayload - Base64-encoded payload
 * @param {string} appSecret - Meta app secret
 * @returns {boolean} - True if signature is valid
 */
function verifyMetaSignature(encodedSig, encodedPayload, appSecret) {
  try {
    // Compute expected signature
    const expectedSig = crypto
      .createHmac('sha256', appSecret)
      .update(encodedPayload)
      .digest();

    // Decode received signature
    const receivedSigBase64 = encodedSig
      .replace(/-/g, '+')
      .replace(/_/g, '/');
    const paddedSig = receivedSigBase64 + '='.repeat((4 - receivedSigBase64.length % 4) % 4);
    const receivedSig = Buffer.from(paddedSig, 'base64');

    // Use timing-safe comparison to prevent timing attacks
    if (receivedSig.length !== expectedSig.length) {
      return false;
    }

    return crypto.timingSafeEqual(receivedSig, expectedSig);
  } catch (error) {
    console.error('Signature verification error:', error);
    return false;
  }
}

// =====================================
// CRITICAL: META DATA DELETION CALLBACK (POST)
// =====================================
// This endpoint is REQUIRED by Meta Platform Terms (Feb 3, 2025)
// Meta sends POST requests here when users request data deletion via Instagram settings

router.post('/data-deletion', rateLimitDeletionRequests, async (req, res) => {
  const startTime = Date.now();
  const clientIp = req.ip || req.socket?.remoteAddress || 'unknown';

  console.log('📩 Data deletion callback received from Meta');
  console.log('Request IP:', clientIp);
  console.log('Request timestamp:', new Date().toISOString());

  const signedRequest = req.body.signed_request;

  // ===== STEP 1: Validate signed_request parameter =====
  if (!signedRequest) {
    console.error('❌ Missing signed_request parameter');
    return res.status(400).json({
      error: 'Missing signed_request parameter',
      code: 'MISSING_SIGNED_REQUEST'
    });
  }

  // ===== STEP 2: Parse signed request =====
  // Format: base64_encoded_signature.base64_encoded_payload
  const parts = signedRequest.split('.');
  if (parts.length !== 2) {
    console.error('❌ Invalid signed_request format');
    return res.status(400).json({
      error: 'Invalid signed_request format',
      code: 'INVALID_FORMAT'
    });
  }

  const [encodedSig, encodedPayload] = parts;

  // ===== STEP 3: Verify HMAC-SHA256 signature =====
  const META_APP_SECRET = process.env.META_APP_SECRET || process.env.VITE_META_APP_SECRET;

  if (!META_APP_SECRET) {
    console.error('❌ META_APP_SECRET not configured');

    if (process.env.NODE_ENV === 'production') {
      return res.status(500).json({
        error: 'Server configuration error',
        code: 'MISSING_APP_SECRET'
      });
    }

    console.warn('⚠️ META_APP_SECRET not set - signature verification skipped (development only)');
  } else {
    const isValidSignature = verifyMetaSignature(encodedSig, encodedPayload, META_APP_SECRET);

    if (!isValidSignature) {
      console.error('❌ Signature verification failed');
      return res.status(401).json({
        error: 'Invalid signature',
        code: 'INVALID_SIGNATURE'
      });
    }

    console.log('✅ Signature verified successfully (timing-safe)');
  }

  // ===== STEP 4: Decode and parse payload =====
  let payload;
  try {
    const paddedPayload = encodedPayload + '='.repeat((4 - encodedPayload.length % 4) % 4);
    const decodedPayload = Buffer.from(
      paddedPayload.replace(/-/g, '+').replace(/_/g, '/'),
      'base64'
    ).toString('utf-8');
    payload = JSON.parse(decodedPayload);

    console.log('📦 Decoded payload:', payload);
  } catch (error) {
    console.error('❌ Error decoding payload:', error);
    return res.status(400).json({
      error: 'Invalid payload encoding',
      code: 'DECODE_ERROR'
    });
  }

  // ===== STEP 5: Extract and validate user ID =====
  const metaUserId = payload.user_id;

  if (!metaUserId) {
    console.error('❌ Missing user_id in payload');
    return res.status(400).json({
      error: 'Missing user_id',
      code: 'MISSING_USER_ID'
    });
  }

  console.log(`👤 Processing deletion request for Meta user ID: ${metaUserId}`);

  // ===== STEP 6: Generate unique confirmation code =====
  // Format: DEL_TIMESTAMP_RANDOM for easy identification
  const timestamp = Date.now();
  const randomBytes = crypto.randomBytes(16).toString('hex');
  const confirmationCode = `DEL_${timestamp}_${randomBytes}`;

  console.log(`🔐 Generated confirmation code: ${confirmationCode}`);

  // ===== STEP 7: Store deletion request in database =====
  const baseUrl = process.env.VITE_API_BASE_URL || 'https://api.888intelligenceautomation.in';
  const statusUrl = `${baseUrl}/legal/deletion-status?code=${confirmationCode}`;

  try {
    const { getSupabaseAdmin } = require('../config/supabase');
    const supabaseAdmin = getSupabaseAdmin();

    if (!supabaseAdmin) {
      throw new Error('Database connection not available');
    }

    // Extract request metadata for audit trail
    const userAgent = req.headers['user-agent'] || 'unknown';
    const ipAddress = req.ip || req.socket?.remoteAddress || 'unknown';

    // Store deletion request with enhanced tracking fields
    const { data: deletionRequest, error: insertError } = await supabaseAdmin
      .from('data_deletion_requests')
      .insert({
        meta_user_id: metaUserId,
        confirmation_code: confirmationCode,
        status: 'pending',
        requested_at: new Date().toISOString(),
        payload: payload,
        status_url: statusUrl,
        // Enhanced tracking fields
        ip_address: ipAddress,
        user_agent: userAgent,
        created_by: 'meta_webhook',
        retry_count: 0,
        max_retries: parseInt(process.env.DELETION_MAX_RETRIES || '3', 10)
      })
      .select()
      .single();

    if (insertError) {
      console.error('Database insertion error:', insertError);
      throw insertError;
    }

    console.log('✅ Deletion request stored in database:', deletionRequest.id);
    console.log('📝 Request metadata:', { ip: ipAddress, userAgent: userAgent.substring(0, 50) });

    // ===== STEP 8: Initiate asynchronous data deletion =====
    // Process deletion in background to avoid timeout
    setImmediate(async () => {
      try {
        console.log(`🗑️ Starting data deletion for Meta user: ${metaUserId}`);

        // Update status to processing
        await supabaseAdmin
          .from('data_deletion_requests')
          .update({
            status: 'processing',
            processing_started_at: new Date().toISOString()
          })
          .eq('id', deletionRequest.id);

        // Find internal user_id from Meta user_id
        const { data: instagramAccount, error: findError } = await supabaseAdmin
          .from('instagram_business_accounts')
          .select('user_id, id')
          .eq('instagram_user_id', metaUserId)
          .single();

        if (findError || !instagramAccount) {
          console.log(`⚠️ No internal user found for Meta user ${metaUserId} - marking as no_account (Meta requirement)`);

          // Per Meta requirements: always return success even if user doesn't exist
          await supabaseAdmin
            .from('data_deletion_requests')
            .update({
              status: 'no_account',
              completed_at: new Date().toISOString(),
              processed_at: new Date().toISOString(),
              error_message: 'No account found in system for this Meta user ID',
              error_code: 'USER_NOT_FOUND'
            })
            .eq('id', deletionRequest.id);

          return;
        }

        const internalUserId = instagramAccount.user_id;
        console.log(`Found internal user ID: ${internalUserId}`);

        // Execute comprehensive data deletion using helper
        const { supabaseHelpers } = require('../config/supabase');
        const deletionResult = await supabaseHelpers.deleteUserData(internalUserId);

        if (deletionResult.success) {
          console.log('✅ User data deletion completed successfully');
          console.log('Deletion results:', deletionResult.results);

          // Extract successfully deleted tables
          const deletedTables = deletionResult.results
            .filter(r => r.success)
            .map(r => r.table);

          // Update request status using database function
          await supabaseAdmin.rpc('complete_deletion_request', {
            p_confirmation_code: confirmationCode,
            p_deleted_data_types: JSON.stringify(deletedTables)
          });

          console.log(`✅ Marked deletion as completed. Deleted data types: ${deletedTables.join(', ')}`);
        } else {
          console.error('❌ User data deletion failed:', deletionResult.error);

          // Update with error using database function (will handle retry logic)
          await supabaseAdmin.rpc('fail_deletion_request', {
            p_confirmation_code: confirmationCode,
            p_error_message: deletionResult.error || 'Unknown deletion error',
            p_error_code: 'DELETION_FAILED'
          });

          console.log('⏰ Deletion marked as failed - retry logic will be handled by database triggers');
        }
      } catch (deletionError) {
        console.error('❌ Error during data deletion:', deletionError);

        // Update with error using database function (will handle retry logic)
        try {
          await supabaseAdmin.rpc('fail_deletion_request', {
            p_confirmation_code: confirmationCode,
            p_error_message: deletionError.message || 'Unexpected error during deletion',
            p_error_code: 'SYSTEM_ERROR'
          });
        } catch (updateError) {
          console.error('❌ Failed to update deletion status:', updateError);
          // Log for manual follow-up
          console.error('⚠️ CRITICAL: Manual intervention required for deletion:', {
            confirmation_code: confirmationCode,
            meta_user_id: metaUserId,
            error: deletionError.message
          });
        }
      }
    });

  } catch (dbError) {
    console.error('❌ Database error:', dbError);

    // Even on database error, return success to Meta (per their requirements)
    // But log the issue for manual follow-up
    console.error('⚠️ Manual follow-up required for deletion request:', {
      meta_user_id: metaUserId,
      confirmation_code: confirmationCode,
      error: dbError.message
    });
  }

  // ===== STEP 9: Return Meta-compliant response =====
  const response = {
    url: statusUrl,
    confirmation_code: confirmationCode
  };

  const processingTime = Date.now() - startTime;

  console.log(`✅ Responding to Meta with:`, response);
  console.log(`⏱️ Processing time: ${processingTime}ms`);

  res.json(response);
});

// =====================================
// DELETION STATUS CHECK ENDPOINT
// =====================================
// Meta and users can check this URL to verify deletion was processed
// Publicly accessible - no authentication required per Meta requirements

router.get('/deletion-status', async (req, res) => {
  const confirmationCode = req.query.code;

  console.log(`📊 Deletion status check for code: ${confirmationCode}`);

  if (!confirmationCode) {
    return res.status(400).send(`
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Invalid Request | 888 Intelligence Automation</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 800px;
            margin: 40px auto;
            padding: 20px;
            background: #f5f5f5;
        }
        .container {
            background: white;
            border-radius: 8px;
            padding: 40px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .error {
            color: #dc3545;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1 class="error">Invalid Request</h1>
        <p>Missing confirmation code. Please check your URL and try again.</p>
    </div>
</body>
</html>
    `);
  }

  try {
    const { getSupabaseAdmin } = require('../config/supabase');
    const supabaseAdmin = getSupabaseAdmin();

    if (!supabaseAdmin) {
      throw new Error('Database connection not available');
    }

    // Query database for deletion request
    const { data: deletionRequest, error } = await supabaseAdmin
      .from('data_deletion_requests')
      .select('*')
      .eq('confirmation_code', confirmationCode)
      .single();

    if (error || !deletionRequest) {
      console.error('Deletion request not found:', confirmationCode);

      return res.status(404).send(`
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Request Not Found | 888 Intelligence Automation</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 800px;
            margin: 40px auto;
            padding: 20px;
            background: #f5f5f5;
        }
        .container {
            background: white;
            border-radius: 8px;
            padding: 40px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .warning {
            color: #ffc107;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1 class="warning">Request Not Found</h1>
        <p>No deletion request found with code: <code>${confirmationCode}</code></p>
        <p>Please check the confirmation code and try again.</p>
    </div>
</body>
</html>
      `);
    }

    // Format dates for display
    const requestedAt = new Date(deletionRequest.requested_at);
    const completedAt = deletionRequest.completed_at ? new Date(deletionRequest.completed_at) : null;
    const isComplete = deletionRequest.status === 'completed';
    const isFailed = deletionRequest.status === 'failed';
    const isPending = deletionRequest.status === 'pending';

    // Generate status HTML
    const statusHtml = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="robots" content="noindex, nofollow">
    <title>Data Deletion Status | 888 Intelligence Automation</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', sans-serif;
            line-height: 1.6;
            color: #333;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container {
            max-width: 800px;
            margin: 40px auto;
            background: white;
            border-radius: 12px;
            padding: 40px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.2);
        }
        h1 {
            color: #1a202c;
            margin-bottom: 10px;
            font-size: 28px;
        }
        .subtitle {
            color: #718096;
            margin-bottom: 30px;
            font-size: 14px;
        }
        .status-badge {
            display: inline-block;
            padding: 12px 24px;
            border-radius: 8px;
            margin: 20px 0;
            font-weight: 600;
            font-size: 18px;
        }
        .status-badge.complete {
            background: #d4edda;
            color: #155724;
            border: 2px solid #28a745;
        }
        .status-badge.pending {
            background: #fff3cd;
            color: #856404;
            border: 2px solid #ffc107;
        }
        .status-badge.failed {
            background: #f8d7da;
            color: #721c24;
            border: 2px solid #dc3545;
        }
        .info-grid {
            margin: 30px 0;
        }
        .info-row {
            display: grid;
            grid-template-columns: 200px 1fr;
            padding: 15px 0;
            border-bottom: 1px solid #e2e8f0;
        }
        .info-row:last-child {
            border-bottom: none;
        }
        .info-label {
            font-weight: 600;
            color: #4a5568;
        }
        .info-value {
            color: #1a202c;
        }
        .info-value code {
            background: #f7fafc;
            padding: 4px 8px;
            border-radius: 4px;
            font-family: 'Monaco', 'Courier New', monospace;
            font-size: 13px;
            color: #e53e3e;
        }
        .status-description {
            background: #f7fafc;
            padding: 20px;
            border-radius: 8px;
            margin: 20px 0;
            border-left: 4px solid #4299e1;
        }
        .status-description h3 {
            color: #2d3748;
            margin-bottom: 10px;
        }
        .error-message {
            background: #fff5f5;
            border: 1px solid #fc8181;
            padding: 15px;
            border-radius: 8px;
            color: #742a2a;
            margin: 20px 0;
        }
        .footer {
            margin-top: 40px;
            padding-top: 30px;
            border-top: 2px solid #e2e8f0;
        }
        .footer h3 {
            color: #2d3748;
            margin-bottom: 15px;
            font-size: 18px;
        }
        .footer ul {
            list-style: none;
            margin: 10px 0;
        }
        .footer li {
            padding: 8px 0;
            color: #4a5568;
        }
        .footer li::before {
            content: "✓ ";
            color: #48bb78;
            font-weight: bold;
            margin-right: 8px;
        }
        .contact-info {
            background: #edf2f7;
            padding: 20px;
            border-radius: 8px;
            margin-top: 20px;
        }
        .contact-info a {
            color: #3182ce;
            text-decoration: none;
        }
        .contact-info a:hover {
            text-decoration: underline;
        }
        @media (max-width: 768px) {
            .container {
                padding: 20px;
            }
            .info-row {
                grid-template-columns: 1fr;
                gap: 5px;
            }
            .info-label {
                font-size: 14px;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Data Deletion Request Status</h1>
        <p class="subtitle">888 Intelligence Automation - Instagram Automation Platform</p>

        <div class="status-badge ${isComplete ? 'complete' : isFailed ? 'failed' : 'pending'}">
            ${isComplete ? '✅ Deletion Complete' : isFailed ? '❌ Deletion Failed' : '⏳ Processing'}
        </div>

        <div class="status-description">
            <h3>Current Status</h3>
            <p>${
              isComplete
                ? 'Your data deletion request has been completed successfully. All personal data associated with this account has been permanently removed from our systems in accordance with GDPR, CCPA, and Meta Platform Terms.'
                : isFailed
                ? 'There was an issue processing your deletion request. Our team has been notified and will address this manually. You will receive an email confirmation once completed.'
                : 'Your deletion request is currently being processed. This typically completes within 24-48 hours, with a maximum of 30 days as required by law.'
            }</p>
        </div>

        ${isFailed && deletionRequest.error_message ? `
        <div class="error-message">
            <strong>Error Details:</strong> ${deletionRequest.error_message}
        </div>
        ` : ''}

        <div class="info-grid">
            <div class="info-row">
                <div class="info-label">Confirmation Code</div>
                <div class="info-value"><code>${confirmationCode}</code></div>
            </div>

            <div class="info-row">
                <div class="info-label">Meta User ID</div>
                <div class="info-value">${deletionRequest.meta_user_id}</div>
            </div>

            <div class="info-row">
                <div class="info-label">Request Date</div>
                <div class="info-value">${requestedAt.toLocaleString('en-US', {
                  dateStyle: 'full',
                  timeStyle: 'short'
                })}</div>
            </div>

            ${completedAt ? `
            <div class="info-row">
                <div class="info-label">Completion Date</div>
                <div class="info-value">${completedAt.toLocaleString('en-US', {
                  dateStyle: 'full',
                  timeStyle: 'short'
                })}</div>
            </div>
            ` : ''}

            <div class="info-row">
                <div class="info-label">Status</div>
                <div class="info-value">${deletionRequest.status.toUpperCase()}</div>
            </div>
        </div>

        <div class="footer">
            <h3>Legal Compliance</h3>
            <ul>
                <li>GDPR Article 17 - Right to Erasure ("Right to be Forgotten")</li>
                <li>CCPA Section 1798.105 - Consumer's Right to Delete Personal Information</li>
                <li>Meta Platform Terms - Data Deletion Callback Requirements (Feb 3, 2025)</li>
                <li>Instagram Graph API - Data Deletion Policy</li>
            </ul>

            <div class="contact-info">
                <h3>Contact Information</h3>
                <p><strong>Data Protection Officer:</strong> <a href="mailto:privacy@888intelligenceautomation.in">privacy@888intelligenceautomation.in</a></p>
                <p><strong>Legal Department:</strong> <a href="mailto:legal@888intelligenceautomation.in">legal@888intelligenceautomation.in</a></p>
                <p><strong>General Support:</strong> <a href="mailto:support@888intelligenceautomation.in">support@888intelligenceautomation.in</a></p>
            </div>
        </div>
    </div>
</body>
</html>
    `;

    res.set('Content-Type', 'text/html');
    res.send(statusHtml);

  } catch (error) {
    console.error('Error fetching deletion status:', error);

    res.status(500).send(`
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Server Error | 888 Intelligence Automation</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 800px;
            margin: 40px auto;
            padding: 20px;
            background: #f5f5f5;
        }
        .container {
            background: white;
            border-radius: 8px;
            padding: 40px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .error {
            color: #dc3545;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1 class="error">Server Error</h1>
        <p>Unable to retrieve deletion status at this time. Please try again later.</p>
        <p>If the problem persists, contact <a href="mailto:support@888intelligenceautomation.in">support@888intelligenceautomation.in</a></p>
    </div>
</body>
</html>
    `);
  }
});

// Legacy endpoint support (redirect to new query-based endpoint)
router.get('/deletion-status/:confirmationCode', (req, res) => {
  const confirmationCode = req.params.confirmationCode;
  res.redirect(301, `/legal/deletion-status?code=${confirmationCode}`);
});

// Data Deletion Policy Route (GET - for browsers and Meta crawlers)
router.get('/data-deletion', (req, res) => {
  const userAgent = req.headers['user-agent'] || '';
  const isMetaCrawler = /facebookexternalhit|Facebot/i.test(userAgent);
  
  if (isMetaCrawler) {
    console.log('📋 Meta crawler accessed data deletion policy:', new Date().toISOString());
  }
  
  res.set('Content-Type', 'text/html');
  res.send(generateLegalHTML('deletion', LEGAL_CONTENT.dataDeletion));
});

// Data Deletion Instructions (Meta requirement)
router.get('/data-deletion-instructions', (req, res) => {
  res.json({
    instructions: {
      method_1: {
        name: "Dashboard Self-Service",
        url: "https://app.888intelligenceautomation.in/dashboard/privacy-controls",
        steps: [
          "1. Login to your account",
          "2. Navigate to Privacy Controls",
          "3. Select data to delete",
          "4. Confirm deletion",
          "5. Receive confirmation email"
        ],
        processing_time: "Immediate"
      },
      method_2: {
        name: "Email Request",
        email: "privacy@888intelligenceautomation.in",
        processing_time: "30 days maximum",
        required_information: [
          "Account email address",
          "Full name",
          "Deletion scope (partial or complete)",
          "Identity verification"
        ]
      }
    },
    compliance: {
      gdpr: "Article 17 - Right to Erasure",
      ccpa: "Section 1798.105 - Right to Delete",
      meta: "Platform Terms February 3, 2025"
    },
    contact: {
      dpo: "privacy@888intelligenceautomation.in",
      support: "support@888intelligenceautomation.in"
    }
  });
});

// =====================================
// RETRY PROCESSING ENDPOINT
// =====================================
// Processes pending and failed deletion requests with retry logic
// Should be called periodically via cron job or monitoring system

router.post('/process-deletions', async (req, res) => {
  const authHeader = req.headers.authorization;

  // Simple API key authentication for cron jobs
  const expectedKey = process.env.CRON_API_KEY || process.env.SUPABASE_SERVICE_KEY;
  if (!authHeader || authHeader !== `Bearer ${expectedKey}`) {
    return res.status(401).json({
      error: 'Unauthorized',
      message: 'Valid API key required'
    });
  }

  try {
    const { getSupabaseAdmin } = require('../config/supabase');
    const supabaseAdmin = getSupabaseAdmin();

    if (!supabaseAdmin) {
      throw new Error('Database connection not available');
    }

    const limit = parseInt(req.query.limit || '10', 10);

    // Get pending deletion requests using database function
    const { data: pendingRequests, error: fetchError } = await supabaseAdmin
      .rpc('get_pending_deletion_requests', { p_limit: limit });

    if (fetchError) {
      throw fetchError;
    }

    if (!pendingRequests || pendingRequests.length === 0) {
      return res.json({
        status: 'success',
        message: 'No pending deletion requests',
        processed: 0
      });
    }

    console.log(`📋 Processing ${pendingRequests.length} deletion requests`);

    const results = [];

    // Process each deletion request
    for (const request of pendingRequests) {
      try {
        console.log(`🔄 Processing deletion: ${request.confirmation_code} (retry ${request.retry_count})`);

        // Find internal user_id from Meta user_id
        const { data: instagramAccount } = await supabaseAdmin
          .from('instagram_business_accounts')
          .select('user_id')
          .eq('instagram_user_id', request.meta_user_id)
          .single();

        if (!instagramAccount) {
          // Mark as no_account
          await supabaseAdmin
            .from('data_deletion_requests')
            .update({
              status: 'no_account',
              completed_at: new Date().toISOString(),
              processed_at: new Date().toISOString(),
              error_code: 'USER_NOT_FOUND'
            })
            .eq('confirmation_code', request.confirmation_code);

          results.push({
            confirmation_code: request.confirmation_code,
            status: 'no_account',
            message: 'User not found'
          });
          continue;
        }

        // Execute deletion
        const { supabaseHelpers } = require('../config/supabase');
        const deletionResult = await supabaseHelpers.deleteUserData(instagramAccount.user_id);

        if (deletionResult.success) {
          const deletedTables = deletionResult.results
            .filter(r => r.success)
            .map(r => r.table);

          await supabaseAdmin.rpc('complete_deletion_request', {
            p_confirmation_code: request.confirmation_code,
            p_deleted_data_types: JSON.stringify(deletedTables)
          });

          results.push({
            confirmation_code: request.confirmation_code,
            status: 'completed',
            deleted_tables: deletedTables.length
          });
        } else {
          await supabaseAdmin.rpc('fail_deletion_request', {
            p_confirmation_code: request.confirmation_code,
            p_error_message: deletionResult.error,
            p_error_code: 'DELETION_FAILED'
          });

          results.push({
            confirmation_code: request.confirmation_code,
            status: 'failed',
            error: deletionResult.error
          });
        }
      } catch (error) {
        console.error(`❌ Error processing ${request.confirmation_code}:`, error);
        results.push({
          confirmation_code: request.confirmation_code,
          status: 'error',
          error: error.message
        });
      }
    }

    console.log(`✅ Processed ${results.length} deletion requests`);

    res.json({
      status: 'success',
      processed: results.length,
      results: results
    });
  } catch (error) {
    console.error('❌ Error in process-deletions:', error);
    res.status(500).json({
      error: 'Internal server error',
      message: error.message
    });
  }
});

// =====================================
// ADMIN MONITORING ENDPOINTS
// =====================================

// Get all deletion requests (admin only)
router.get('/admin/deletion-requests', async (req, res) => {
  try {
    // TODO: Add proper admin authentication
    // For now, require API key
    const authHeader = req.headers.authorization;
    const expectedKey = process.env.ADMIN_API_KEY || process.env.SUPABASE_SERVICE_KEY;

    if (!authHeader || authHeader !== `Bearer ${expectedKey}`) {
      return res.status(401).json({
        error: 'Unauthorized',
        message: 'Admin access required'
      });
    }

    const { getSupabaseAdmin } = require('../config/supabase');
    const supabaseAdmin = getSupabaseAdmin();

    if (!supabaseAdmin) {
      throw new Error('Database connection not available');
    }

    const { status, limit = 100, offset = 0 } = req.query;

    let query = supabaseAdmin
      .from('data_deletion_requests')
      .select('*', { count: 'exact' })
      .order('requested_at', { ascending: false })
      .range(offset, offset + limit - 1);

    if (status) {
      query = query.eq('status', status);
    }

    const { data: requests, error, count } = await query;

    if (error) {
      throw error;
    }

    res.json({
      success: true,
      data: requests,
      pagination: {
        total: count,
        limit: parseInt(limit),
        offset: parseInt(offset),
        has_more: count > (parseInt(offset) + parseInt(limit))
      }
    });
  } catch (error) {
    console.error('❌ Error fetching deletion requests:', error);
    res.status(500).json({
      error: 'Internal server error',
      message: error.message
    });
  }
});

// Get deletion statistics (admin only)
router.get('/admin/deletion-stats', async (req, res) => {
  try {
    // TODO: Add proper admin authentication
    const authHeader = req.headers.authorization;
    const expectedKey = process.env.ADMIN_API_KEY || process.env.SUPABASE_SERVICE_KEY;

    if (!authHeader || authHeader !== `Bearer ${expectedKey}`) {
      return res.status(401).json({
        error: 'Unauthorized',
        message: 'Admin access required'
      });
    }

    const { getSupabaseAdmin } = require('../config/supabase');
    const supabaseAdmin = getSupabaseAdmin();

    if (!supabaseAdmin) {
      throw new Error('Database connection not available');
    }

    // Use database function to get statistics
    const { data: stats, error } = await supabaseAdmin
      .rpc('get_deletion_statistics');

    if (error) {
      throw error;
    }

    res.json({
      success: true,
      statistics: stats && stats.length > 0 ? stats[0] : {},
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('❌ Error fetching deletion statistics:', error);
    res.status(500).json({
      error: 'Internal server error',
      message: error.message
    });
  }
});

// Health check for legal routes
router.get('/health', (req, res) => {
  res.json({
    status: 'OK',
    routes: [
      '/legal/privacy-policy',
      '/legal/terms-of-service',
      '/legal/data-deletion',
      '/legal/data-deletion-instructions',
      '/legal/deletion-status',
      '/legal/process-deletions',
      '/legal/admin/deletion-requests',
      '/legal/admin/deletion-stats'
    ],
    meta_compliance: true,
    last_updated: LEGAL_CONTENT.privacyPolicy.lastUpdated
  });
});

module.exports = router;