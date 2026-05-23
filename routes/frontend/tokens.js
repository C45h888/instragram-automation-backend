// backend.api/routes/frontend/tokens.js
// Token management routes: exchange, release, refresh, validate
const express = require('express');
const router = express.Router();
const axios = require('axios');
const {
  exchangeForPageToken,
  storePageToken,
  retrievePageToken,
  storeUserToken,
  refreshUserToken,
  detectTokenType,
  fetchDynamicScope,
  GRAPH_API_BASE,
} = require('../../services/tokens');
const { getSupabaseAdmin, logAudit: logAuditService, fireAndForgetInsert } = require('../../config/supabase');
const { clearCredentialCache } = require('../../helpers/credential-cache');

const logAudit = logAuditService;
const GRAPH_API_VERSION = 'v23.0';

// ==========================================
// HELPER FUNCTIONS
// ==========================================

/**
 * Validates a Meta access token by calling the Graph API
 * @param {string} token - Access token to validate
 * @param {string} instagramBusinessId - IG Business Account ID
 * @returns {Promise<Object>} - { success: boolean, data?: object, error?: string }
 */
async function validateMetaToken(token, instagramBusinessId) {
  try {
    const response = await axios.get(
      `https://graph.facebook.com/${GRAPH_API_VERSION}/${instagramBusinessId}`,
      {
        params: {
          fields: 'id,username,name,profile_picture_url',
          access_token: token
        },
        timeout: 10000
      }
    );

    return {
      success: true,
      data: response.data
    };
  } catch (error) {
    return {
      success: false,
      error: error.response?.data?.error?.message || error.message
    };
  }
}

// ==========================================
// ROUTES
// ==========================================

/**
 * POST /api/instagram/exchange-token
 * Exchange user access token for page access token with AUTO-DISCOVERY
 */
router.post('/exchange-token', async (req, res) => {
  try {
    const { userAccessToken, userId, selectedPage } = req.body;

    console.log('📥 Token exchange request received');
    console.log('   User ID (UUID):', userId || 'not provided');
    console.log('   Mode:', selectedPage ? 'page-selection' : 'full-exchange');

    // ===== STEP 1: Validate userId =====
    if (!userId) {
      return res.status(400).json({
        success: false,
        error: 'userId (UUID) is required',
        code: 'MISSING_USER_ID'
      });
    }

    // ===== STEP 2: Resolve page token info =====
    let pageAccessToken, pageId, pageName, discoveredBusinessAccountId;

    if (selectedPage) {
      ({ pageAccessToken, pageId, pageName, igBusinessAccountId: discoveredBusinessAccountId } = selectedPage);
      console.log('✅ Using selected page:', pageName, '/ IG:', discoveredBusinessAccountId);

    } else {
      if (!userAccessToken) {
        return res.status(400).json({
          success: false,
          error: 'userAccessToken is required',
          code: 'MISSING_USER_TOKEN'
        });
      }

      console.log('🔄 Starting token exchange and IG account discovery...');
      const exchangeResult = await exchangeForPageToken(userAccessToken);

      if (!exchangeResult.success) {
        console.error('❌ Token exchange failed:', exchangeResult.error);
        return res.status(400).json({
          success: false,
          error: 'Failed to exchange token',
          details: exchangeResult.error,
          code: 'TOKEN_EXCHANGE_FAILED'
        });
      }

      // Multi-page: return list to frontend for picker modal
      if (exchangeResult.requiresSelection) {
        console.log('ℹ️  Multiple IG-linked pages found, returning list for picker');
        return res.status(200).json({
          success: true,
          requiresSelection: true,
          pages: exchangeResult.pages
        });
      }

      pageAccessToken = exchangeResult.pageAccessToken;
      pageId = exchangeResult.pageId;
      pageName = exchangeResult.pageName;
      discoveredBusinessAccountId = exchangeResult.igBusinessAccountId;
    }

    if (!discoveredBusinessAccountId) {
      return res.status(400).json({
        success: false,
        error: 'No Instagram Business Account found',
        code: 'NO_IG_BUSINESS_ACCOUNT'
      });
    }

    // ===== STEP 3: Collision check =====
    const supabaseAdmin = getSupabaseAdmin();
    const { data: existingAccount } = await supabaseAdmin
      .from('instagram_business_accounts')
      .select('user_id, id')
      .eq('instagram_business_id', discoveredBusinessAccountId)
      .neq('user_id', userId)
      .maybeSingle();

    if (existingAccount) {
      console.warn(`⚠️  IG account ${discoveredBusinessAccountId} already owned by another user`);

      await supabaseAdmin.from('system_alerts').insert({
        alert_type: 'account_transfer_request',
        business_account_id: existingAccount.id,
        message: 'Another user is requesting to connect your Instagram Business Account. Tap "Release" in this notification to transfer ownership.',
        details: {
          requesting_user_id: userId,
          instagram_business_id: discoveredBusinessAccountId,
          requested_at: new Date().toISOString()
        },
        resolved: false
      });

      await supabaseAdmin.from('instagram_business_accounts')
        .update({
          transfer_requested_by: userId,
          transfer_requested_at: new Date().toISOString()
        })
        .eq('id', existingAccount.id);

      return res.status(409).json({
        success: false,
        error: 'This Instagram account is already connected to another user. The current owner has been notified and can release it.',
        errorCode: 'ACCOUNT_ALREADY_CONNECTED'
      });
    }

    // ===== STEP 4: Validate & potentially extend UAT =====
    // Per Meta docs: long-lived UAT → non-expiring PAT. Short-lived UAT → short-lived PAT.
    let finalUAT = userAccessToken || selectedPage?.originalUAT;
    let uatScopes = [];
    let uatExpiresAt = null;
    let uatDetected = false;

    if (finalUAT) {
      const uatInfo = await detectTokenType(finalUAT);
      if (uatInfo && uatInfo.isValid) {
        uatDetected = true;
        uatScopes = uatInfo.scopes || [];
        const expiresAt = uatInfo.expiresAt;
        // Short-lived: expires_at > 0 and less than 7 days from now
        const isShortLived = expiresAt > 0 && (expiresAt - Math.floor(Date.now() / 1000)) < 3600 * 24 * 7;

        if (isShortLived) {
          try {
            const extendRes = await axios.get(`${GRAPH_API_BASE}/oauth/access_token`, {
              params: {
                grant_type: 'fb_exchange_token',
                client_id: process.env.INSTAGRAM_APP_ID,
                client_secret: process.env.INSTAGRAM_APP_SECRET,
                fb_exchange_token: finalUAT
              },
              timeout: 10000
            });
            finalUAT = extendRes.data.access_token;
            uatExpiresAt = extendRes.data.expires_in
              ? new Date(Date.now() + extendRes.data.expires_in * 1000).toISOString()
              : null;
            console.log('✅ UAT extended to long-lived');
          } catch (extErr) {
            console.warn('⚠️ UAT extension failed, storing as short-lived:', extErr.message);
            uatExpiresAt = new Date(expiresAt * 1000).toISOString();
          }
        } else {
          uatExpiresAt = expiresAt > 0 ? new Date(expiresAt * 1000).toISOString() : null;
        }
      }
    }

    // ===== STEP 5: Detect PAT scopes via /debug_token =====
    const patScopes = await fetchDynamicScope(pageAccessToken, supabaseAdmin);

    // ===== STEP 6: Store PAT in vault =====
    console.log('💾 Storing PAT and creating business account record...');

    const storeResult = await storePageToken({
      userId,
      igBusinessAccountId: discoveredBusinessAccountId,
      pageAccessToken,
      pageId,
      pageName,
      scope: patScopes
    });

    if (!storeResult.success) {
      console.error('❌ Failed to store page token:', storeResult.error);
      return res.status(500).json({
        success: false,
        error: 'Failed to store credentials',
        details: storeResult.error,
        code: 'STORAGE_FAILED'
      });
    }

    console.log('✅ PAT stored successfully (token_type=page)');
    clearCredentialCache(storeResult.businessAccountId);

    // Resolve any outstanding auth_failure alerts — account is now reconnected
    await fireAndForgetInsert(
      supabaseAdmin
        .from('system_alerts')
        .update({ resolved: true, resolved_at: new Date().toISOString() })
        .eq('business_account_id', storeResult.businessAccountId)
        .eq('alert_type', 'auth_failure')
        .eq('resolved', false)
    );

    // ===== STEP 7: Store UAT in split vault =====
    if (uatDetected && finalUAT) {
      // Re-detect after potential extension to get data_access_expires_at
      const finalUatInfo = await detectTokenType(finalUAT);
      const dataAccessExpiresAt = finalUatInfo?.dataAccessExpiresAt
        ? new Date(finalUatInfo.dataAccessExpiresAt * 1000).toISOString()
        : null;

      const uatResult = await storeUserToken({
        userId,
        businessAccountId: storeResult.businessAccountId,
        userAccessToken: finalUAT,
        scope: uatScopes,
        expiresAt: uatExpiresAt,
        dataAccessExpiresAt
      });
      if (uatResult.success) {
        console.log('✅ UAT stored in split vault (token_type=user)');
      }
    }

    // ===== STEP 8: Return success =====
    return res.status(200).json({
      success: true,
      message: 'Token exchange and storage successful',
      data: {
        businessAccountId: storeResult.businessAccountId,
        instagramBusinessId: discoveredBusinessAccountId,
        pageId,
        pageName,
        tokensStored: uatDetected ? ['page', 'user'] : ['page']
      }
    });

  } catch (error) {
    console.error('❌ Token exchange error:', error);
    return res.status(500).json({
      success: false,
      error: 'Internal server error during token exchange',
      message: error.message,
      code: 'INTERNAL_ERROR'
    });
  }
});

/**
 * POST /api/instagram/release-account
 * Current owner releases their IG account so a requesting user can connect it.
 */
router.post('/release-account', async (req, res) => {
  try {
    const { userId, instagramBusinessId } = req.body;

    if (!userId || !instagramBusinessId) {
      return res.status(400).json({ error: 'userId and instagramBusinessId are required' });
    }

    const supabase = getSupabaseAdmin();

    await supabase
      .from('instagram_credentials')
      .update({ is_active: false, updated_at: new Date().toISOString() })
      .eq('user_id', userId)
      .eq('token_type', 'page');

    const { error: deleteError } = await supabase
      .from('instagram_business_accounts')
      .delete()
      .eq('user_id', userId)
      .eq('instagram_business_id', instagramBusinessId);

    if (deleteError) {
      console.error('❌ release-account delete failed:', deleteError.message);
      return res.status(500).json({ error: 'Failed to release account', details: deleteError.message });
    }

    await supabase
      .from('system_alerts')
      .update({ resolved: true, resolved_at: new Date().toISOString() })
      .eq('alert_type', 'account_transfer_request')
      .filter('details->>instagram_business_id', 'eq', instagramBusinessId);

    console.log(`✅ Account ${instagramBusinessId} released by user ${userId}`);
    return res.json({ success: true, message: 'Account released. The requesting user can now reconnect.' });

  } catch (error) {
    console.error('❌ release-account error:', error);
    return res.status(500).json({ error: 'Internal server error', message: error.message });
  }
});

/**
 * POST /api/instagram/refresh-token
 * Refresh token — branched by type.
 * PAT: non-expiring, returns early.
 * UAT: calls refreshUserToken() which uses fb_exchange_token (only works on UATs per Meta docs).
 */
router.post('/refresh-token', async (req, res) => {
  const requestStartTime = Date.now();

  try {
    const { userId, businessAccountId } = req.body;

    console.log('[Token] Refresh request received');
    console.log('   User ID:', userId);
    console.log('   Business Account ID:', businessAccountId);

    // ===== STEP 1: VALIDATION =====
    if (!userId || !businessAccountId) {
      return res.status(400).json({
        success: false,
        error: 'userId and businessAccountId are required',
        code: 'MISSING_PARAMETERS'
      });
    }

    const supabase = getSupabaseAdmin();

    // ===== STEP 2: Check what token types exist =====
    // Query without .single() since user may have both PAT and UAT rows
    const { data: credentials } = await supabase
      .from('instagram_credentials')
      .select('token_type')
      .eq('user_id', userId)
      .eq('business_account_id', businessAccountId)
      .eq('is_active', true);

    if (!credentials || credentials.length === 0) {
      await logAudit('token_refresh_failed', userId, {
        action: 'refresh_token',
        business_account_id: businessAccountId,
        error: 'credentials_not_found',
        response_time_ms: Date.now() - requestStartTime
      });

      return res.status(404).json({
        success: false,
        error: 'No credentials found',
        code: 'CREDENTIALS_NOT_FOUND'
      });
    }

    const tokenTypes = credentials.map(c => c.token_type);
    const hasUAT = tokenTypes.includes('user');
    const hasPAT = tokenTypes.includes('page');

    // ===== PAT GUARD: Page tokens are non-expiring =====
    if (hasPAT && !hasUAT) {
      console.log('[Token] PAT only — page tokens are non-expiring, skipping refresh');
      return res.status(200).json({
        success: true,
        message: 'Page Access Tokens are non-expiring and do not require refresh (per Meta docs)',
        token_type: 'page'
      });
    }

    // ===== UAT REFRESH: Call refreshUserToken from service layer =====
    if (hasUAT) {
      console.log('[Token] UAT detected — attempting fb_exchange_token refresh');
      try {
        const result = await refreshUserToken(userId, businessAccountId);

        // Success notification
        await supabase.from('system_alerts').insert({
          alert_type: 'uat_refresh_success',
          business_account_id: businessAccountId,
          message: 'Your access token has been refreshed successfully.',
          details: { new_expires_at: result.expiresAt, scopes: result.scopes },
          resolved: true
        });

        await logAudit('uat_refreshed', userId, {
          business_account_id: businessAccountId,
          new_expires_at: result.expiresAt,
          response_time_ms: Date.now() - requestStartTime
        });

        return res.json({
          success: true,
          message: 'User Access Token refreshed',
          token_type: 'user',
          expiresAt: result.expiresAt
        });
      } catch (refreshErr) {
        console.error('[Token] UAT refresh failed:', refreshErr.message);

        // Failure notification — user may need to reconnect
        await supabase.from('system_alerts').insert({
          alert_type: 'uat_refresh_failed',
          business_account_id: businessAccountId,
          message: 'Token refresh failed. You may need to reconnect your Instagram account.',
          details: { error: refreshErr.message },
          resolved: false
        });

        await logAudit('uat_refresh_failed', userId, {
          business_account_id: businessAccountId,
          error: refreshErr.message,
          response_time_ms: Date.now() - requestStartTime
        });

        return res.status(400).json({
          success: false,
          error: refreshErr.message,
          code: 'UAT_REFRESH_FAILED'
        });
      }
    }

    // Fallback — no refreshable token found
    return res.status(400).json({
      success: false,
      error: 'No refreshable token found',
      code: 'NO_REFRESHABLE_TOKEN'
    });

  } catch (error) {
    const responseTime = Date.now() - requestStartTime;

    console.error('[Token] Unexpected error:', error.message);

    await logAudit('token_refresh_error', req.body.userId, {
      action: 'refresh_token',
      error: error.message,
      response_time_ms: responseTime
    });

    res.status(500).json({
      success: false,
      error: 'Failed to refresh token',
      code: 'INTERNAL_ERROR',
      details: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

/**
 * POST /api/instagram/validate-token
 * Validates if the stored Instagram access token is still active
 */
router.post('/validate-token', async (req, res) => {
  const requestStartTime = Date.now();
  const supabase = getSupabaseAdmin();

  try {
    const {
      userId,
      businessAccountId,
      importMode = false,
      pageAccessToken,
      pageId,
      pageName
    } = req.body;

    // ===== IMPORT MODE: Direct token import with type detection =====
    if (importMode) {
      console.log('📥 Token import mode activated');

      const { instagramBusinessId } = req.body;

      if (!userId || !pageAccessToken || !instagramBusinessId) {
        return res.status(400).json({
          success: false,
          error: 'Missing required fields for import',
          required: ['userId', 'pageAccessToken', 'instagramBusinessId']
        });
      }

      // Detect token type via /debug_token before storing
      const tokenInfo = await detectTokenType(pageAccessToken);

      if (!tokenInfo || !tokenInfo.isValid) {
        try {
          await logAudit('token_import_failed', userId, {
            action: 'import_token',
            error: 'invalid_token',
            detected_type: tokenInfo?.type || 'unknown'
          });
        } catch (auditError) {
          console.warn('⚠️  Audit log failed (non-blocking):', auditError.message);
        }

        return res.status(401).json({
          success: false,
          error: 'Token is invalid or expired',
          detected_type: tokenInfo?.type || 'unknown'
        });
      }

      const detectedType = tokenInfo.type; // 'USER' or 'PAGE'
      console.log(`✅ Token validated — detected type: ${detectedType}`);

      let storeResult;
      let detectedScope = tokenInfo.scopes || [];
      let tokensStored = [];

      if (detectedType === 'USER') {
        // UAT imported — auto-exchange for PAT, store both
        console.log('🔄 UAT detected in import — auto-exchanging for PAT...');
        const exchangeResult = await exchangeForPageToken(pageAccessToken);

        if (!exchangeResult.success) {
          return res.status(400).json({
            success: false,
            error: 'Token is a User Access Token but PAT exchange failed',
            details: exchangeResult.error,
            code: 'UAT_EXCHANGE_FAILED'
          });
        }

        if (exchangeResult.requiresSelection) {
          return res.status(200).json({
            success: true,
            requiresSelection: true,
            pages: exchangeResult.pages,
            detectedType: 'USER'
          });
        }

        // Detect PAT scopes
        const patScopes = await fetchDynamicScope(exchangeResult.pageAccessToken, supabase);

        // Store PAT
        storeResult = await storePageToken({
          userId,
          igBusinessAccountId: exchangeResult.igBusinessAccountId,
          pageAccessToken: exchangeResult.pageAccessToken,
          pageId: exchangeResult.pageId,
          pageName: exchangeResult.pageName || pageName || 'Imported Account',
          scope: patScopes
        });

        tokensStored.push('page');
        detectedScope = patScopes;

        // Store UAT in split vault
        if (storeResult.success) {
          const uatExpiresAt = tokenInfo.expiresAt > 0
            ? new Date(tokenInfo.expiresAt * 1000).toISOString()
            : null;
          const uatDataAccessExpiresAt = tokenInfo.dataAccessExpiresAt
            ? new Date(tokenInfo.dataAccessExpiresAt * 1000).toISOString()
            : null;
          await storeUserToken({
            userId,
            businessAccountId: storeResult.businessAccountId,
            userAccessToken: pageAccessToken,
            scope: tokenInfo.scopes,
            expiresAt: uatExpiresAt,
            dataAccessExpiresAt: uatDataAccessExpiresAt
          });
          tokensStored.push('user');
        }

      } else {
        // PAGE token imported — store directly
        detectedScope = await fetchDynamicScope(pageAccessToken, supabase);

        storeResult = await storePageToken({
          userId,
          igBusinessAccountId: instagramBusinessId,
          pageAccessToken,
          pageId: pageId || instagramBusinessId,
          pageName: pageName || 'Imported Account',
          scope: detectedScope
        });

        tokensStored.push('page');
      }

      if (!storeResult || !storeResult.success) {
        try {
          await logAudit('token_storage_failed', userId, {
            action: 'import_token',
            error: storeResult?.error || 'unknown'
          });
        } catch (auditError) {
          console.warn('⚠️  Audit log failed (non-blocking):', auditError.message);
        }

        return res.status(500).json({
          success: false,
          error: 'Failed to store token',
          details: storeResult?.error
        });
      }

      try {
        await logAudit('token_imported', userId, {
          action: 'import_token',
          business_account_id: storeResult.businessAccountId,
          detected_type: detectedType,
          tokens_stored: tokensStored,
          scope: detectedScope,
          response_time_ms: Date.now() - requestStartTime,
          success: true
        });
      } catch (auditError) {
        console.warn('⚠️  Audit log failed (non-blocking):', auditError.message);
      }

      return res.json({
        success: true,
        status: 'imported',
        detectedType,
        data: {
          businessAccountId: storeResult.businessAccountId,
          expiresAt: storeResult.expiresAt,
          scope: detectedScope,
          tokensStored
        }
      });
    }

    // ===== VALIDATION MODE =====
    console.log('[Token Validation] Validating token for user:', userId);

    if (!userId || !businessAccountId) {
      return res.status(400).json({
        success: false,
        error: 'Missing required fields: userId and businessAccountId'
      });
    }

    // ===== STEP 1: Fetch credentials from database =====
    const { data: credentials, error: fetchError } = await supabase
      .from('instagram_credentials')
      .select('*')
      .eq('user_id', userId)
      .eq('business_account_id', businessAccountId)
      .eq('token_type', 'page')
      .eq('is_active', true)
      .single();

    if (fetchError || !credentials) {
      console.error('[Token Validation] ❌ Credentials not found:', {
        error: fetchError?.message,
        details: fetchError?.details,
        hint: fetchError?.hint,
        code: fetchError?.code,
        userId,
        businessAccountId
      });
      return res.status(404).json({
        success: false,
        status: 'not_found',
        error: 'Credentials not found for this user',
        code: 'CREDENTIALS_NOT_FOUND',
        details: process.env.NODE_ENV === 'development' ? {
          error: fetchError?.message,
          hint: fetchError?.hint
        } : undefined
      });
    }

    // ===== STEP 2: Fetch instagram_business_id =====
    const { data: businessAccount, error: businessError } = await supabase
      .from('instagram_business_accounts')
      .select('instagram_business_id')
      .eq('id', businessAccountId)
      .single();

    if (businessError || !businessAccount) {
      console.error('[Token Validation] ❌ Instagram business account not found:', {
        error: businessError?.message,
        details: businessError?.details,
        businessAccountId
      });
      return res.status(404).json({
        success: false,
        status: 'not_found',
        error: 'Instagram business account not linked to credentials',
        code: 'BUSINESS_ACCOUNT_NOT_LINKED',
        details: process.env.NODE_ENV === 'development' ? {
          error: businessError?.message
        } : undefined
      });
    }

    // ===== STEP 3: Decrypt the token =====
    const { data: decryptedToken, error: decryptError } = await supabase
      .rpc('decrypt_instagram_token', {
        encrypted_token: credentials.access_token_encrypted
      });

    if (decryptError || !decryptedToken) {
      console.error('[Token Validation] ❌ Token decryption failed:', {
        error: decryptError?.message,
        details: decryptError?.details,
        hint: decryptError?.hint
      });
      return res.status(500).json({
        success: false,
        status: 'error',
        error: 'Failed to decrypt access token',
        code: 'DECRYPTION_FAILED',
        details: process.env.NODE_ENV === 'development' ? {
          error: decryptError?.message,
          hint: decryptError?.hint
        } : undefined
      });
    }

    console.log('[Token Validation] ✅ Token decrypted successfully');

    // ===== STEP 4: Use instagram_business_id =====
    const instagramBusinessId = businessAccount.instagram_business_id;

    if (!instagramBusinessId) {
      console.error('[Token Validation] ❌ Missing instagram_business_id');
      return res.status(500).json({
        success: false,
        status: 'error',
        error: 'Instagram business ID not found',
        code: 'MISSING_BUSINESS_ID'
      });
    }

    // ===== STEP 5: Validate token by calling Meta API =====
    const graphUrl = `https://graph.facebook.com/${GRAPH_API_VERSION}/${instagramBusinessId}`;

    console.log('[Token Validation] 🔍 Calling Meta API:', {
      url: graphUrl,
      instagramBusinessId,
      tokenLength: decryptedToken?.length,
      hasToken: !!decryptedToken
    });

    try {
      const response = await axios.get(graphUrl, {
        params: {
          fields: 'id',
          access_token: decryptedToken
        },
        timeout: 5000
      });

      const responseTime = Date.now() - requestStartTime;

      console.log(`[Token Validation] ✅ Token is active (${responseTime}ms)`);

      return res.json({
        success: true,
        status: 'active',
        data: {
          instagram_business_id: response.data.id,
          validated_at: new Date().toISOString()
        },
        meta: {
          response_time_ms: responseTime
        }
      });

    } catch (validationError) {
      const responseTime = Date.now() - requestStartTime;

      if (validationError.response) {
        const { status, data } = validationError.response;
        const errorCode = data?.error?.code;
        const errorSubcode = data?.error?.error_subcode;
        const errorType = data?.error?.type;
        const errorMessage = data?.error?.message;

        console.error('[Token Validation] ❌ Meta API Error:', {
          status,
          code: errorCode,
          subcode: errorSubcode,
          type: errorType,
          message: errorMessage
        });

        if (errorCode === 190 || status === 401 || errorType === 'OAuthException') {
          let reason = 'Token expired or invalid';

          if (errorSubcode === 460) {
            reason = 'Password changed - user must reconnect';
          } else if (errorSubcode === 463) {
            reason = 'Token expired (60-day limit exceeded)';
          } else if (errorSubcode === 467) {
            reason = 'User deauthorized app or logged out';
          } else if (errorSubcode === 490) {
            reason = 'User account not confirmed';
          }

          console.log(`[Token Validation] ⚠️  Token expired (Code: ${errorCode}, Subcode: ${errorSubcode}) - ${reason}`);

          try {
            await logAudit('token_validation_expired', userId, {
              action: 'validate_token',
              business_account_id: businessAccountId,
              error_code: errorCode,
              error_subcode: errorSubcode,
              reason: reason,
              response_time_ms: responseTime
            });
          } catch (auditError) {
            console.warn('⚠️  Audit log failed (non-blocking):', auditError.message);
          }

          // Insert auth_failure system_alert so the notification bell lights up.
          // Only insert if no unresolved alert already exists (prevents duplicates).
          try {
            const { data: existingAlert } = await supabase
              .from('system_alerts')
              .select('id')
              .eq('business_account_id', businessAccountId)
              .eq('alert_type', 'auth_failure')
              .eq('resolved', false)
              .maybeSingle();

            if (!existingAlert) {
              await supabase.from('system_alerts').insert({
                alert_type: 'auth_failure',
                business_account_id: businessAccountId,
                message: 'Instagram access token is no longer valid. Please reconnect your account.',
                details: { user_id: userId, reason, source: 'validate_token' },
                resolved: false
              });
            }
          } catch (alertErr) {
            console.warn('[Token Validation] ⚠️  system_alert insert failed (non-blocking):', alertErr.message);
          }

          return res.json({
            success: true,
            status: 'expired',
            error: errorMessage || reason,
            details: {
              error_code: errorCode,
              error_subcode: errorSubcode,
              reason: reason,
              requires_reconnect: true
            },
            meta: {
              response_time_ms: responseTime
            }
          });
        }

        if (errorCode === 4) {
          console.warn('[Token Validation] ⚠️  Rate limit hit during validation');

          return res.status(429).json({
            success: false,
            status: 'rate_limited',
            error: 'Rate limit exceeded during token validation',
            code: 'RATE_LIMIT_EXCEEDED',
            details: {
              retry_after: validationError.response.headers['retry-after'] || 60
            }
          });
        }

        console.error('[Token Validation] ❌ Unexpected Meta API error:', errorMessage);

        return res.status(status || 500).json({
          success: false,
          status: 'error',
          error: errorMessage || 'Failed to validate token',
          code: 'VALIDATION_FAILED',
          details: {
            error_code: errorCode,
            error_type: errorType
          }
        });
      }

      console.error('[Token Validation] ❌ Network error:', validationError.message);

      return res.status(500).json({
        success: false,
        status: 'error',
        error: 'Network error during token validation',
        code: 'NETWORK_ERROR',
        details: validationError.message
      });
    }

  } catch (error) {
    const responseTime = Date.now() - requestStartTime;

    console.error('[Token Validation] ❌ Unexpected error:', {
      message: error.message,
      stack: error.stack,
      name: error.name,
      code: error.code,
      response: error.response?.data
    });

    try {
      await logAudit('token_validation_error', req.body.userId, {
        action: 'validate_token',
        error: error.message,
        error_name: error.name,
        error_code: error.code,
        stack: process.env.NODE_ENV === 'development' ? error.stack : undefined,
        response_time_ms: responseTime
      });
    } catch (auditError) {
      console.warn('⚠️  Audit log failed (non-blocking):', auditError.message);
    }

    res.status(500).json({
      success: false,
      status: 'error',
      error: 'Internal server error during token validation',
      code: 'INTERNAL_ERROR',
      details: process.env.NODE_ENV === 'development' ? {
        message: error.message,
        name: error.name,
        code: error.code
      } : undefined
    });
  }
});

/**
 * GET /api/instagram/token-status
 * Lightweight endpoint for frontend to poll token health.
 * Returns status of both PAT and UAT with warning thresholds.
 */
router.get('/token-status', async (req, res) => {
  try {
    const { userId } = req.query;
    const businessAccountId = req.query.businessAccountId || req.query.business_account_id;
    if (!userId || !businessAccountId) {
      return res.status(400).json({ error: 'userId and businessAccountId required' });
    }

    const supabase = getSupabaseAdmin();

    // Fetch both token types
    const { data: creds } = await supabase
      .from('instagram_credentials')
      .select('token_type, expires_at, data_access_expires_at, is_active, scope, last_refreshed_at')
      .eq('user_id', userId)
      .eq('business_account_id', businessAccountId)
      .eq('is_active', true);

    const pat = creds?.find(c => c.token_type === 'page');
    const uat = creds?.find(c => c.token_type === 'user');

    const now = new Date();
    const sevenDays  = 7  * 24 * 60 * 60 * 1000;
    const fourteenDays = 14 * 24 * 60 * 60 * 1000;
    const thirtyDays   = 30 * 24 * 60 * 60 * 1000;

    // ── UAT token expiry status ──
    let uatStatus = 'missing';
    let uatWarning = null;
    if (uat) {
      if (!uat.expires_at) {
        uatStatus = 'valid';
      } else {
        const remaining = new Date(uat.expires_at) - now;
        if (remaining <= 0) {
          uatStatus = 'expired';
          uatWarning = 'Your access token has expired. Please reconnect your Instagram account.';
        } else if (remaining < sevenDays) {
          uatStatus = 'critical';
          uatWarning = 'Your access token expires in less than 7 days. Please refresh it.';
        } else if (remaining < fourteenDays) {
          uatStatus = 'warning';
          uatWarning = 'Your access token expires in less than 14 days.';
        } else {
          uatStatus = 'valid';
        }
      }
    }

    // ── Data access expiry status (separate Meta-controlled window) ──
    // Cannot be refreshed via fb_exchange_token — only resets on fresh OAuth consent.
    let dataAccessStatus = uat ? 'valid' : 'missing';
    let dataAccessWarning = null;
    if (uat?.data_access_expires_at) {
      const daRemaining = new Date(uat.data_access_expires_at) - now;
      if (daRemaining <= 0) {
        dataAccessStatus = 'expired';
        dataAccessWarning = 'Instagram data access has expired. Please reconnect your account to restore access to messages and comments.';
      } else if (daRemaining < sevenDays) {
        dataAccessStatus = 'critical';
        dataAccessWarning = 'Instagram data access expires in less than 7 days. Reconnect your account to renew it.';
      } else if (daRemaining < thirtyDays) {
        dataAccessStatus = 'warning';
        dataAccessWarning = 'Instagram data access expires soon. Reconnect your account to renew access to messages and comments.';
      }
    }

    res.json({
      success: true,
      pat: pat ? { status: 'valid', scope: pat.scope } : { status: 'missing' },
      uat: {
        status: uatStatus,
        warning: uatWarning,
        expiresAt: uat?.expires_at || null,
        dataAccessExpiresAt: uat?.data_access_expires_at || null,
        dataAccessStatus,
        dataAccessWarning,
        lastRefreshedAt: uat?.last_refreshed_at || null
      }
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
