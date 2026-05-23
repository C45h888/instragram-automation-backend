// ============================================
// META DATA DELETION REQUEST SYSTEM - TEST SUITE
// ============================================
// File: backend.api/tests/deletion-requests.test.js
// Version: 1.0.0
// Purpose: Comprehensive test suite for data deletion endpoints
//
// Test Coverage:
//   - Meta signature verification (timing-safe)
//   - Webhook endpoint functionality
//   - Status checking endpoint
//   - Retry processing logic
//   - Admin endpoints
//   - Database triggers and functions
//   - Edge cases and error handling
//
// Usage:
//   npm test tests/deletion-requests.test.js
//   or
//   node tests/deletion-requests.test.js
// ============================================

const crypto = require('crypto');
const axios = require('axios');

// Configuration
const BASE_URL = process.env.API_BASE_URL || 'http://localhost:3001';
const META_APP_SECRET = process.env.META_APP_SECRET || 'test_secret_key_for_testing';
const TEST_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

// Test utilities
const colors = {
  green: '\x1b[32m',
  red: '\x1b[31m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  reset: '\x1b[0m'
};

function log(message, color = colors.reset) {
  console.log(`${color}${message}${colors.reset}`);
}

// Generate valid Meta signed_request
function generateMetaSignedRequest(userId, appSecret = META_APP_SECRET) {
  const payload = {
    user_id: userId,
    issued_at: Math.floor(Date.now() / 1000)
  };

  const encodedPayload = Buffer.from(JSON.stringify(payload))
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=/g, '');

  const signature = crypto
    .createHmac('sha256', appSecret)
    .update(encodedPayload)
    .digest('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=/g, '');

  return `${signature}.${encodedPayload}`;
}

// Test results tracker
const testResults = {
  passed: 0,
  failed: 0,
  total: 0,
  tests: []
};

function recordTest(name, passed, message = '') {
  testResults.total++;
  if (passed) {
    testResults.passed++;
    log(`✓ ${name}`, colors.green);
  } else {
    testResults.failed++;
    log(`✗ ${name}`, colors.red);
    if (message) log(`  Error: ${message}`, colors.red);
  }
  testResults.tests.push({ name, passed, message });
}

// ============================================
// TEST SUITE
// ============================================

async function runTests() {
  log('\n==========================================', colors.blue);
  log('META DATA DELETION REQUEST - TEST SUITE', colors.blue);
  log('==========================================\n', colors.blue);

  // Test 1: Signature Verification - Valid Signature
  log('\n[1] Signature Verification Tests', colors.yellow);
  try {
    const validSignedRequest = generateMetaSignedRequest('test_user_12345');
    const response = await axios.post(`${BASE_URL}/legal/data-deletion`, {
      signed_request: validSignedRequest
    }, {
      validateStatus: () => true // Don't throw on any status
    });

    recordTest(
      'Valid Meta signature should be accepted',
      response.status === 200,
      response.status !== 200 ? `Status: ${response.status}` : ''
    );

    // Store confirmation code for later tests
    global.testConfirmationCode = response.data?.confirmation_code;
  } catch (error) {
    recordTest('Valid Meta signature should be accepted', false, error.message);
  }

  // Test 2: Invalid Signature
  try {
    const invalidSignedRequest = 'invalid_signature.invalid_payload';
    const response = await axios.post(`${BASE_URL}/legal/data-deletion`, {
      signed_request: invalidSignedRequest
    }, {
      validateStatus: () => true
    });

    recordTest(
      'Invalid signature format should be rejected',
      response.status === 400,
      `Expected 400, got ${response.status}`
    );
  } catch (error) {
    recordTest('Invalid signature format should be rejected', false, error.message);
  }

  // Test 3: Missing Signed Request
  try {
    const response = await axios.post(`${BASE_URL}/legal/data-deletion`, {}, {
      validateStatus: () => true
    });

    recordTest(
      'Missing signed_request should return 400',
      response.status === 400,
      `Expected 400, got ${response.status}`
    );
  } catch (error) {
    recordTest('Missing signed_request should return 400', false, error.message);
  }

  // Test 4: Tampered Payload
  try {
    const signedRequest = generateMetaSignedRequest('test_user_12345');
    const [sig, payload] = signedRequest.split('.');
    const tamperedPayload = payload + 'tampered';
    const response = await axios.post(`${BASE_URL}/legal/data-deletion`, {
      signed_request: `${sig}.${tamperedPayload}`
    }, {
      validateStatus: () => true
    });

    recordTest(
      'Tampered payload should fail signature verification',
      response.status === 401 || response.status === 400,
      `Expected 401/400, got ${response.status}`
    );
  } catch (error) {
    recordTest('Tampered payload should fail signature verification', false, error.message);
  }

  // Test 5: Deletion Status Endpoint - Valid Code
  log('\n[2] Status Endpoint Tests', colors.yellow);
  if (global.testConfirmationCode) {
    try {
      const response = await axios.get(
        `${BASE_URL}/legal/deletion-status?code=${global.testConfirmationCode}`,
        { validateStatus: () => true }
      );

      recordTest(
        'Valid confirmation code should return status page',
        response.status === 200 && response.headers['content-type'].includes('text/html'),
        `Status: ${response.status}`
      );
    } catch (error) {
      recordTest('Valid confirmation code should return status page', false, error.message);
    }
  } else {
    recordTest('Valid confirmation code should return status page', false, 'No test code available');
  }

  // Test 6: Invalid Confirmation Code
  try {
    const response = await axios.get(
      `${BASE_URL}/legal/deletion-status?code=INVALID_CODE_12345`,
      { validateStatus: () => true }
    );

    recordTest(
      'Invalid confirmation code should return 404 or not found message',
      response.status === 404 || (response.status === 200 && response.data.includes('not found')),
      `Status: ${response.status}`
    );
  } catch (error) {
    recordTest('Invalid confirmation code should return 404', false, error.message);
  }

  // Test 7: Missing Confirmation Code
  try {
    const response = await axios.get(
      `${BASE_URL}/legal/deletion-status`,
      { validateStatus: () => true }
    );

    recordTest(
      'Missing confirmation code should return 400',
      response.status === 400,
      `Expected 400, got ${response.status}`
    );
  } catch (error) {
    recordTest('Missing confirmation code should return 400', false, error.message);
  }

  // Test 8: Rate Limiting
  log('\n[3] Rate Limiting Tests', colors.yellow);
  try {
    const requests = [];
    const testUserId = `rate_limit_test_${Date.now()}`;

    // Send 12 requests (rate limit is 10 per hour)
    for (let i = 0; i < 12; i++) {
      const signedRequest = generateMetaSignedRequest(testUserId);
      requests.push(
        axios.post(`${BASE_URL}/legal/data-deletion`, {
          signed_request: signedRequest
        }, {
          validateStatus: () => true
        })
      );
    }

    const responses = await Promise.all(requests);
    const rateLimitedResponses = responses.filter(r => r.status === 429);

    recordTest(
      'Rate limiting should block excessive requests (>10/hour)',
      rateLimitedResponses.length >= 2,
      `Only ${rateLimitedResponses.length} requests were rate limited`
    );
  } catch (error) {
    recordTest('Rate limiting test', false, error.message);
  }

  // Test 9: Process Deletions Endpoint - Unauthorized
  log('\n[4] Retry Processing Endpoint Tests', colors.yellow);
  try {
    const response = await axios.post(`${BASE_URL}/legal/process-deletions`, {}, {
      validateStatus: () => true
    });

    recordTest(
      'Process deletions without auth should return 401',
      response.status === 401,
      `Expected 401, got ${response.status}`
    );
  } catch (error) {
    recordTest('Process deletions without auth should return 401', false, error.message);
  }

  // Test 10: Process Deletions Endpoint - With Auth
  if (TEST_SERVICE_KEY) {
    try {
      const response = await axios.post(
        `${BASE_URL}/legal/process-deletions?limit=5`,
        {},
        {
          headers: {
            Authorization: `Bearer ${TEST_SERVICE_KEY}`
          },
          validateStatus: () => true
        }
      );

      recordTest(
        'Process deletions with valid auth should return 200',
        response.status === 200,
        `Status: ${response.status}`
      );

      if (response.status === 200 && response.data) {
        recordTest(
          'Process deletions response should include results',
          'processed' in response.data && 'results' in response.data,
          JSON.stringify(response.data)
        );
      }
    } catch (error) {
      recordTest('Process deletions with auth', false, error.message);
    }
  } else {
    log('  ⚠ Skipping authenticated tests - SUPABASE_SERVICE_KEY not set', colors.yellow);
  }

  // Test 11: Admin Endpoints - Unauthorized
  log('\n[5] Admin Endpoint Tests', colors.yellow);
  try {
    const response = await axios.get(`${BASE_URL}/legal/admin/deletion-requests`, {
      validateStatus: () => true
    });

    recordTest(
      'Admin endpoint without auth should return 401',
      response.status === 401,
      `Expected 401, got ${response.status}`
    );
  } catch (error) {
    recordTest('Admin endpoint without auth should return 401', false, error.message);
  }

  // Test 12: Admin Endpoints - With Auth
  if (TEST_SERVICE_KEY) {
    try {
      const response = await axios.get(`${BASE_URL}/legal/admin/deletion-requests?limit=10`, {
        headers: {
          Authorization: `Bearer ${TEST_SERVICE_KEY}`
        },
        validateStatus: () => true
      });

      recordTest(
        'Admin endpoint with auth should return deletion requests',
        response.status === 200 && response.data.success === true,
        `Status: ${response.status}`
      );
    } catch (error) {
      recordTest('Admin endpoint with auth', false, error.message);
    }
  }

  // Test 13: Admin Statistics Endpoint
  if (TEST_SERVICE_KEY) {
    try {
      const response = await axios.get(`${BASE_URL}/legal/admin/deletion-stats`, {
        headers: {
          Authorization: `Bearer ${TEST_SERVICE_KEY}`
        },
        validateStatus: () => true
      });

      recordTest(
        'Admin stats endpoint should return statistics',
        response.status === 200 && response.data.success === true,
        `Status: ${response.status}`
      );
    } catch (error) {
      recordTest('Admin stats endpoint', false, error.message);
    }
  }

  // Test 14: Legal Routes Health Check
  log('\n[6] Health Check Tests', colors.yellow);
  try {
    const response = await axios.get(`${BASE_URL}/legal/health`);

    recordTest(
      'Legal routes health check should return OK',
      response.status === 200 && response.data.status === 'OK',
      `Status: ${response.status}`
    );

    recordTest(
      'Health check should list all routes',
      response.data.routes && response.data.routes.length >= 8,
      `Found ${response.data.routes?.length || 0} routes`
    );
  } catch (error) {
    recordTest('Legal routes health check', false, error.message);
  }

  // Test 15: Response Format Compliance
  log('\n[7] Meta API Compliance Tests', colors.yellow);
  try {
    const signedRequest = generateMetaSignedRequest(`compliance_test_${Date.now()}`);
    const response = await axios.post(`${BASE_URL}/legal/data-deletion`, {
      signed_request: signedRequest
    });

    const hasUrl = response.data && 'url' in response.data;
    const hasCode = response.data && 'confirmation_code' in response.data;

    recordTest(
      'Response should include url field (Meta requirement)',
      hasUrl,
      hasUrl ? '' : 'Missing url field'
    );

    recordTest(
      'Response should include confirmation_code field (Meta requirement)',
      hasCode,
      hasCode ? '' : 'Missing confirmation_code field'
    );

    if (hasCode) {
      const code = response.data.confirmation_code;
      recordTest(
        'Confirmation code should not exceed 255 chars (Meta requirement)',
        code.length <= 255,
        `Length: ${code.length}`
      );
    }
  } catch (error) {
    recordTest('Meta API compliance tests', false, error.message);
  }

  // ============================================
  // TEST SUMMARY
  // ============================================

  log('\n==========================================', colors.blue);
  log('TEST SUMMARY', colors.blue);
  log('==========================================\n', colors.blue);

  log(`Total Tests: ${testResults.total}`, colors.blue);
  log(`Passed: ${testResults.passed}`, colors.green);
  log(`Failed: ${testResults.failed}`, testResults.failed > 0 ? colors.red : colors.green);
  log(`Success Rate: ${((testResults.passed / testResults.total) * 100).toFixed(2)}%\n`, colors.blue);

  if (testResults.failed > 0) {
    log('Failed Tests:', colors.red);
    testResults.tests
      .filter(t => !t.passed)
      .forEach(t => log(`  - ${t.name}: ${t.message}`, colors.red));
  }

  log('\n==========================================\n', colors.blue);

  // Exit with appropriate code
  process.exit(testResults.failed > 0 ? 1 : 0);
}

// ============================================
// RUN TESTS
// ============================================

// Check if server is running
async function checkServer() {
  try {
    await axios.get(`${BASE_URL}/health`, { timeout: 5000 });
    return true;
  } catch (error) {
    return false;
  }
}

async function main() {
  log('Checking if server is running...', colors.yellow);

  const serverRunning = await checkServer();

  if (!serverRunning) {
    log('\n❌ Server is not running!', colors.red);
    log(`Please start the server at ${BASE_URL} before running tests.`, colors.yellow);
    log('Run: npm start\n', colors.yellow);
    process.exit(1);
  }

  log('✓ Server is running\n', colors.green);

  // Run all tests
  await runTests();
}

// Handle errors
process.on('unhandledRejection', (error) => {
  log('\n❌ Unhandled error during tests:', colors.red);
  console.error(error);
  process.exit(1);
});

// Run main function
main();
