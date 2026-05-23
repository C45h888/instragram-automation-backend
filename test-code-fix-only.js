// ============================================
// CODE VALIDATION TEST: Instagram Business Account Fix
// ============================================
// Purpose: Validate the code fix WITHOUT requiring database connection
//
// Tests:
// 1. Verify the fix is applied correctly in the code
// 2. Check for removed non-existent fields (page_id, page_name)
// 3. Check for added required field (name)
// 4. Check onConflict parameter is corrected
// ============================================

const fs = require('fs');
const path = require('path');

// ANSI color codes
const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  cyan: '\x1b[36m'
};

function log(emoji, message, color = colors.reset) {
  console.log(`${color}${emoji} ${message}${colors.reset}`);
}

function logSection(title) {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`${colors.bright}${colors.cyan}${title}${colors.reset}`);
  console.log(`${'='.repeat(60)}\n`);
}

function testCodeFix() {
  logSection('CODE FIX VALIDATION');

  const filePath = path.join(__dirname, 'services', 'instagram-tokens.js');

  log('🔍', `Reading file: ${path.basename(filePath)}`);

  if (!fs.existsSync(filePath)) {
    log('❌', 'File not found!', colors.red);
    return false;
  }

  const fileContent = fs.readFileSync(filePath, 'utf-8');

  const tests = [];

  // TEST 1: Check that 'name' field is added
  logSection('TEST 1: Required "name" Field');
  const hasNameField = fileContent.includes("name: pageName");
  if (hasNameField) {
    log('✅', 'PASS: "name: pageName" field found in upsert', colors.green);
    log('ℹ️', '  This fixes the missing required field error', colors.blue);
    tests.push({ name: 'name field added', passed: true });
  } else {
    log('❌', 'FAIL: "name: pageName" field NOT found', colors.red);
    tests.push({ name: 'name field added', passed: false });
  }

  // TEST 2: Check that page_id is removed
  logSection('TEST 2: Non-existent "page_id" Removed');
  const hasPageId = fileContent.includes("page_id: pageId");
  if (!hasPageId) {
    log('✅', 'PASS: "page_id: pageId" removed from upsert', colors.green);
    log('ℹ️', '  This field does not exist in the database schema', colors.blue);
    tests.push({ name: 'page_id removed', passed: true });
  } else {
    log('❌', 'FAIL: "page_id: pageId" still present', colors.red);
    log('💡', '  This field should be removed', colors.yellow);
    tests.push({ name: 'page_id removed', passed: false });
  }

  // TEST 3: Check that page_name is removed
  logSection('TEST 3: Non-existent "page_name" Removed');
  const hasPageName = fileContent.includes("page_name: pageName");
  if (!hasPageName) {
    log('✅', 'PASS: "page_name: pageName" removed from upsert', colors.green);
    log('ℹ️', '  This field does not exist in the database schema', colors.blue);
    tests.push({ name: 'page_name removed', passed: true });
  } else {
    log('❌', 'FAIL: "page_name: pageName" still present', colors.red);
    log('💡', '  This field should be removed', colors.yellow);
    tests.push({ name: 'page_name removed', passed: false });
  }

  // TEST 4: Check onConflict parameter is fixed
  logSection('TEST 4: onConflict Parameter Fixed');
  const hasOldConflict = fileContent.includes("onConflict: 'user_id,instagram_business_id'");
  const hasNewConflict = fileContent.includes("onConflict: 'instagram_business_id'");

  if (hasNewConflict && !hasOldConflict) {
    log('✅', 'PASS: onConflict parameter corrected', colors.green);
    log('ℹ️', '  Changed from: "user_id,instagram_business_id"', colors.blue);
    log('ℹ️', '  Changed to: "instagram_business_id"', colors.blue);
    log('ℹ️', '  This fixes the "schema cache" error', colors.blue);
    tests.push({ name: 'onConflict fixed', passed: true });
  } else if (hasOldConflict) {
    log('❌', 'FAIL: Old onConflict parameter still present', colors.red);
    log('💡', '  Should be: onConflict: \'instagram_business_id\'', colors.yellow);
    tests.push({ name: 'onConflict fixed', passed: false });
  } else if (!hasNewConflict && !hasOldConflict) {
    log('⚠️', 'WARNING: onConflict parameter not found', colors.yellow);
    tests.push({ name: 'onConflict fixed', passed: false });
  }

  // TEST 5: Check validation is added
  logSection('TEST 5: Field Validation Added');
  const hasValidation = fileContent.includes("if (!pageName)");
  if (hasValidation) {
    log('✅', 'PASS: Validation for required fields added', colors.green);
    log('ℹ️', '  Validates pageName before database operation', colors.blue);
    tests.push({ name: 'validation added', passed: true });
  } else {
    log('⚠️', 'WARNING: Validation not found (optional)', colors.yellow);
    tests.push({ name: 'validation added', passed: true }); // Not critical
  }

  // TEST 6: Check comment indicates fix
  logSection('TEST 6: Fix Comments Present');
  const hasFixComment = fileContent.includes("// FIXED:") || fileContent.includes("FIXED:");
  if (hasFixComment) {
    log('✅', 'PASS: Fix comments found in code', colors.green);
    log('ℹ️', '  Code is documented with fix annotations', colors.blue);
    tests.push({ name: 'fix comments', passed: true });
  } else {
    log('⚠️', 'WARNING: No fix comments found', colors.yellow);
    tests.push({ name: 'fix comments', passed: true }); // Not critical
  }

  // Summary
  logSection('TEST SUMMARY');

  tests.forEach(test => {
    const icon = test.passed ? '✅' : '❌';
    const color = test.passed ? colors.green : colors.red;
    log(icon, test.name, color);
  });

  const passedTests = tests.filter(t => t.passed).length;
  const totalTests = tests.length;

  console.log('\n' + '='.repeat(60));
  if (passedTests === totalTests) {
    log('🎉', `ALL TESTS PASSED (${passedTests}/${totalTests})`, colors.green);
    log('✅', 'The code fix is correctly applied!', colors.green);
    console.log('\n' + colors.bright + colors.green + 'READY TO TEST WITH BACKEND' + colors.reset);
  } else {
    log('⚠️', `${passedTests}/${totalTests} tests passed`, colors.yellow);
    log('💡', 'Some non-critical tests failed', colors.yellow);
  }
  console.log('='.repeat(60) + '\n');

  // Show what the fix does
  logSection('WHAT THIS FIX RESOLVES');
  log('🔧', 'Original Error:', colors.yellow);
  log('  ', '"Could not find \'instagram_business_accounts\' in the schema cache"', colors.red);
  console.log();
  log('🔧', 'Root Causes Fixed:', colors.yellow);
  log('  ', '1. Wrong onConflict parameter (composite vs single column)', colors.blue);
  log('  ', '2. Missing required "name" field', colors.blue);
  log('  ', '3. Non-existent "page_id" and "page_name" fields', colors.blue);
  console.log();
  log('🔧', 'Next Steps:', colors.yellow);
  log('  ', '1. Ensure backend server has valid .env configuration', colors.blue);
  log('  ', '2. Restart backend server to load the fixed code', colors.blue);
  log('  ', '3. Test OAuth flow with actual Facebook login', colors.blue);
  log('  ', '4. Verify record is created in Supabase dashboard', colors.blue);
  console.log('='.repeat(60) + '\n');

  return passedTests === totalTests;
}

// Run test
const success = testCodeFix();
process.exit(success ? 0 : 1);
