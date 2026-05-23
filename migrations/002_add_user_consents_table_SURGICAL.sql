-- ============================================
-- SURGICAL MIGRATION: ENHANCE USER CONSENTS TABLE
-- ============================================
-- File: backend.api/migrations/002_add_user_consents_table_SURGICAL.sql
-- Version: 1.0.0 (Surgical - Only adds what's missing)
-- Purpose: Safely add missing columns/indexes/functions to user_consents
-- Safety: 100% idempotent - safe to run multiple times
--
-- CURRENT SCHEMA (8 columns detected in database.types.ts):
--   - id, consent_type, consent_given, consented_at
--   - privacy_policy_version, terms_version, ip_address, user_agent
--
-- WILL ADD (10 missing columns):
--   - user_id (CRITICAL - foreign key to auth.users)
--   - consent_text, browser_language, consent_method
--   - revoked, revoked_at, revocation_reason, revoked_by
--   - created_at, updated_at
--
-- COMPLIANCE:
--   - GDPR Article 7: Conditions for consent (proof of consent)
--   - GDPR Article 7.3: Right to withdraw consent
--   - CCPA Section 1798.100: Consumer's right to know
-- ============================================

BEGIN;

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================
-- SECTION 1: SURGICALLY ADD MISSING COLUMNS
-- ============================================

DO $$
DECLARE
  v_column_exists BOOLEAN;
  v_constraint_exists BOOLEAN;
BEGIN
  RAISE NOTICE 'Starting surgical migration of user_consents table...';

  -- Check if table exists
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'user_consents'
  ) THEN
    RAISE EXCEPTION 'Table user_consents does not exist. Create base table first.';
  END IF;

  -- =====================================
  -- Add user_id column (CRITICAL)
  -- =====================================
  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'user_consents'
      AND column_name = 'user_id'
  ) INTO v_column_exists;

  IF NOT v_column_exists THEN
    -- Add column first without constraint
    ALTER TABLE public.user_consents
      ADD COLUMN user_id UUID;
    RAISE NOTICE '✓ Added column: user_id';

    -- Add foreign key constraint separately (safer for existing data)
    ALTER TABLE public.user_consents
      ADD CONSTRAINT user_consents_user_id_fkey
      FOREIGN KEY (user_id)
      REFERENCES auth.users(id)
      ON DELETE CASCADE;
    RAISE NOTICE '✓ Added foreign key constraint: user_id → auth.users(id)';
  ELSE
    RAISE NOTICE '⊘ Column already exists: user_id';
  END IF;

  -- =====================================
  -- Add consent_text column
  -- =====================================
  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'user_consents'
      AND column_name = 'consent_text'
  ) INTO v_column_exists;

  IF NOT v_column_exists THEN
    ALTER TABLE public.user_consents
      ADD COLUMN consent_text TEXT;
    RAISE NOTICE '✓ Added column: consent_text';
  ELSE
    RAISE NOTICE '⊘ Column already exists: consent_text';
  END IF;

  -- =====================================
  -- Add browser_language column
  -- =====================================
  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'user_consents'
      AND column_name = 'browser_language'
  ) INTO v_column_exists;

  IF NOT v_column_exists THEN
    ALTER TABLE public.user_consents
      ADD COLUMN browser_language VARCHAR(10);
    RAISE NOTICE '✓ Added column: browser_language';
  ELSE
    RAISE NOTICE '⊘ Column already exists: browser_language';
  END IF;

  -- =====================================
  -- Add consent_method column
  -- =====================================
  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'user_consents'
      AND column_name = 'consent_method'
  ) INTO v_column_exists;

  IF NOT v_column_exists THEN
    ALTER TABLE public.user_consents
      ADD COLUMN consent_method VARCHAR(50) DEFAULT 'web';
    RAISE NOTICE '✓ Added column: consent_method';

    -- Add CHECK constraint for consent_method
    ALTER TABLE public.user_consents
      ADD CONSTRAINT user_consents_consent_method_check
      CHECK (consent_method IN ('web', 'mobile_app', 'api', 'admin_portal'));
    RAISE NOTICE '✓ Added check constraint for consent_method';
  ELSE
    RAISE NOTICE '⊘ Column already exists: consent_method';
  END IF;

  -- =====================================
  -- Add revoked column (CRITICAL for GDPR)
  -- =====================================
  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'user_consents'
      AND column_name = 'revoked'
  ) INTO v_column_exists;

  IF NOT v_column_exists THEN
    ALTER TABLE public.user_consents
      ADD COLUMN revoked BOOLEAN DEFAULT FALSE NOT NULL;
    RAISE NOTICE '✓ Added column: revoked';
  ELSE
    RAISE NOTICE '⊘ Column already exists: revoked';
  END IF;

  -- =====================================
  -- Add revoked_at column
  -- =====================================
  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'user_consents'
      AND column_name = 'revoked_at'
  ) INTO v_column_exists;

  IF NOT v_column_exists THEN
    ALTER TABLE public.user_consents
      ADD COLUMN revoked_at TIMESTAMP WITH TIME ZONE;
    RAISE NOTICE '✓ Added column: revoked_at';
  ELSE
    RAISE NOTICE '⊘ Column already exists: revoked_at';
  END IF;

  -- =====================================
  -- Add revocation_reason column
  -- =====================================
  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'user_consents'
      AND column_name = 'revocation_reason'
  ) INTO v_column_exists;

  IF NOT v_column_exists THEN
    ALTER TABLE public.user_consents
      ADD COLUMN revocation_reason TEXT;
    RAISE NOTICE '✓ Added column: revocation_reason';
  ELSE
    RAISE NOTICE '⊘ Column already exists: revocation_reason';
  END IF;

  -- =====================================
  -- Add revoked_by column
  -- =====================================
  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'user_consents'
      AND column_name = 'revoked_by'
  ) INTO v_column_exists;

  IF NOT v_column_exists THEN
    ALTER TABLE public.user_consents
      ADD COLUMN revoked_by UUID REFERENCES auth.users(id);
    RAISE NOTICE '✓ Added column: revoked_by';
  ELSE
    RAISE NOTICE '⊘ Column already exists: revoked_by';
  END IF;

  -- =====================================
  -- Add created_at column
  -- =====================================
  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'user_consents'
      AND column_name = 'created_at'
  ) INTO v_column_exists;

  IF NOT v_column_exists THEN
    ALTER TABLE public.user_consents
      ADD COLUMN created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL;
    RAISE NOTICE '✓ Added column: created_at';
  ELSE
    RAISE NOTICE '⊘ Column already exists: created_at';
  END IF;

  -- =====================================
  -- Add updated_at column
  -- =====================================
  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'user_consents'
      AND column_name = 'updated_at'
  ) INTO v_column_exists;

  IF NOT v_column_exists THEN
    ALTER TABLE public.user_consents
      ADD COLUMN updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL;
    RAISE NOTICE '✓ Added column: updated_at';
  ELSE
    RAISE NOTICE '⊘ Column already exists: updated_at';
  END IF;

  RAISE NOTICE 'Column addition phase complete.';
END $$;

-- ============================================
-- SECTION 2: UPDATE/ADD CONSTRAINTS
-- ============================================

DO $$
BEGIN
  -- Ensure consent_type constraint exists with all values
  ALTER TABLE public.user_consents
    DROP CONSTRAINT IF EXISTS user_consents_consent_type_check;

  ALTER TABLE public.user_consents
    ADD CONSTRAINT user_consents_consent_type_check
    CHECK (consent_type IN (
      'instagram_oauth',
      'instagram_data_access',
      'marketing_emails',
      'analytics_tracking',
      'third_party_sharing',
      'terms_acceptance',
      'privacy_policy',
      'cookies',
      'automation_features'
    ));
  RAISE NOTICE '✓ Updated consent_type constraint with all 9 valid values';

  -- Add unique constraint for user_id + consent_type + consented_at
  -- This prevents duplicate consent records
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'user_consents_user_type_time_unique'
  ) THEN
    ALTER TABLE public.user_consents
      ADD CONSTRAINT user_consents_user_type_time_unique
      UNIQUE (user_id, consent_type, consented_at);
    RAISE NOTICE '✓ Added unique constraint: (user_id, consent_type, consented_at)';
  ELSE
    RAISE NOTICE '⊘ Unique constraint already exists';
  END IF;
END $$;

-- ============================================
-- SECTION 3: ADD TABLE COMMENTS
-- ============================================

COMMENT ON TABLE public.user_consents IS 'User consent tracking for GDPR/CCPA compliance with complete audit trail and revocation support';
COMMENT ON COLUMN public.user_consents.user_id IS 'User who gave/revoked consent (foreign key to auth.users)';
COMMENT ON COLUMN public.user_consents.consent_type IS 'Type of consent: instagram_oauth, terms_acceptance, privacy_policy, etc.';
COMMENT ON COLUMN public.user_consents.consent_given IS 'Whether consent was given (true) or denied (false)';
COMMENT ON COLUMN public.user_consents.consent_text IS 'Exact consent text shown to user (legal evidence)';
COMMENT ON COLUMN public.user_consents.ip_address IS 'IP address at time of consent (proof of authenticity)';
COMMENT ON COLUMN public.user_consents.revoked IS 'Whether this consent has been withdrawn (GDPR Article 7.3)';
COMMENT ON COLUMN public.user_consents.revoked_at IS 'Timestamp when consent was revoked';

-- ============================================
-- SECTION 4: CREATE PERFORMANCE INDEXES
-- ============================================

-- Primary user lookup
CREATE INDEX IF NOT EXISTS idx_user_consents_user
  ON public.user_consents(user_id);

-- Consent type lookup
CREATE INDEX IF NOT EXISTS idx_user_consents_type
  ON public.user_consents(consent_type);

-- Combined user + type lookup (most common query)
CREATE INDEX IF NOT EXISTS idx_user_consents_user_type
  ON public.user_consents(user_id, consent_type);

-- Time-based queries for audit trails
CREATE INDEX IF NOT EXISTS idx_user_consents_date
  ON public.user_consents(consented_at DESC);

-- Partial index for revoked consents (performance optimization)
CREATE INDEX IF NOT EXISTS idx_user_consents_revoked
  ON public.user_consents(user_id, consent_type)
  WHERE revoked = TRUE;

-- Partial index for active consents (most frequent queries)
CREATE INDEX IF NOT EXISTS idx_user_consents_active
  ON public.user_consents(user_id, consent_type, consent_given)
  WHERE revoked = FALSE;

-- Version tracking for compliance audits
CREATE INDEX IF NOT EXISTS idx_user_consents_version
  ON public.user_consents(privacy_policy_version, terms_version)
  WHERE revoked = FALSE;

-- ============================================
-- SECTION 5: CREATE TRIGGER FUNCTIONS
-- ============================================

-- Trigger Function 1: Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION public.update_user_consents_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Trigger Function 2: Auto-set revoked_at when revoked flag changes
CREATE OR REPLACE FUNCTION public.set_consent_revoked_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  -- Set revoked_at when revoked changes from FALSE to TRUE
  IF NEW.revoked = TRUE AND (OLD.revoked IS NULL OR OLD.revoked = FALSE) THEN
    NEW.revoked_at = NOW();
  END IF;

  -- Clear revoked_at if revoked is set back to FALSE
  IF NEW.revoked = FALSE AND OLD.revoked = TRUE THEN
    NEW.revoked_at = NULL;
    NEW.revocation_reason = NULL;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- ============================================
-- SECTION 6: CREATE TRIGGERS
-- ============================================

DROP TRIGGER IF EXISTS user_consents_updated_at ON public.user_consents;
CREATE TRIGGER user_consents_updated_at
  BEFORE UPDATE ON public.user_consents
  FOR EACH ROW
  EXECUTE FUNCTION public.update_user_consents_updated_at();

DROP TRIGGER IF EXISTS user_consents_revoke_timestamp ON public.user_consents;
CREATE TRIGGER user_consents_revoke_timestamp
  BEFORE UPDATE OF revoked ON public.user_consents
  FOR EACH ROW
  EXECUTE FUNCTION public.set_consent_revoked_timestamp();

-- ============================================
-- SECTION 7: CREATE HELPER FUNCTIONS
-- ============================================

-- Function 1: Get active consent status for user
CREATE OR REPLACE FUNCTION public.get_active_consent(
  p_user_id UUID,
  p_consent_type VARCHAR
)
RETURNS BOOLEAN AS $$
DECLARE
  v_has_consent BOOLEAN;
BEGIN
  SELECT consent_given INTO v_has_consent
  FROM public.user_consents
  WHERE user_id = p_user_id
    AND consent_type = p_consent_type
    AND revoked = FALSE
  ORDER BY consented_at DESC
  LIMIT 1;

  RETURN COALESCE(v_has_consent, FALSE);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

COMMENT ON FUNCTION public.get_active_consent IS 'Check if user has active (non-revoked) consent for a specific type';

-- Function 2: Record new consent
CREATE OR REPLACE FUNCTION public.record_consent(
  p_user_id UUID,
  p_consent_type VARCHAR,
  p_consent_given BOOLEAN,
  p_privacy_policy_version VARCHAR DEFAULT NULL,
  p_terms_version VARCHAR DEFAULT NULL,
  p_consent_text TEXT DEFAULT NULL,
  p_ip_address INET DEFAULT NULL,
  p_user_agent TEXT DEFAULT NULL,
  p_browser_language VARCHAR DEFAULT NULL,
  p_consent_method VARCHAR DEFAULT 'web'
)
RETURNS UUID AS $$
DECLARE
  v_consent_id UUID;
BEGIN
  INSERT INTO public.user_consents (
    user_id,
    consent_type,
    consent_given,
    privacy_policy_version,
    terms_version,
    consent_text,
    ip_address,
    user_agent,
    browser_language,
    consent_method,
    consented_at,
    revoked
  ) VALUES (
    p_user_id,
    p_consent_type,
    p_consent_given,
    p_privacy_policy_version,
    p_terms_version,
    p_consent_text,
    COALESCE(p_ip_address, '0.0.0.0'::INET),
    p_user_agent,
    p_browser_language,
    p_consent_method,
    NOW(),
    FALSE
  )
  RETURNING id INTO v_consent_id;

  RETURN v_consent_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

COMMENT ON FUNCTION public.record_consent IS 'Record a new consent with full audit trail';

-- Function 3: Revoke existing consent
CREATE OR REPLACE FUNCTION public.revoke_consent(
  p_user_id UUID,
  p_consent_type VARCHAR,
  p_reason TEXT DEFAULT NULL
)
RETURNS BOOLEAN AS $$
DECLARE
  v_affected_rows INTEGER;
BEGIN
  UPDATE public.user_consents
  SET
    revoked = TRUE,
    revoked_at = NOW(),
    revocation_reason = p_reason,
    revoked_by = p_user_id
  WHERE user_id = p_user_id
    AND consent_type = p_consent_type
    AND revoked = FALSE;

  GET DIAGNOSTICS v_affected_rows = ROW_COUNT;

  RETURN v_affected_rows > 0;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

COMMENT ON FUNCTION public.revoke_consent IS 'Revoke active consent for a user (GDPR Article 7.3)';

-- Function 4: Get consent history for user
CREATE OR REPLACE FUNCTION public.get_consent_history(
  p_user_id UUID,
  p_consent_type VARCHAR DEFAULT NULL
)
RETURNS TABLE(
  id UUID,
  consent_type VARCHAR,
  consent_given BOOLEAN,
  consented_at TIMESTAMP WITH TIME ZONE,
  revoked BOOLEAN,
  revoked_at TIMESTAMP WITH TIME ZONE,
  privacy_policy_version VARCHAR,
  terms_version VARCHAR,
  consent_text TEXT,
  ip_address INET,
  user_agent TEXT
) AS $$
BEGIN
  IF p_consent_type IS NULL THEN
    -- Return all consent history for user
    RETURN QUERY
    SELECT
      uc.id,
      uc.consent_type,
      uc.consent_given,
      uc.consented_at,
      uc.revoked,
      uc.revoked_at,
      uc.privacy_policy_version,
      uc.terms_version,
      uc.consent_text,
      uc.ip_address,
      uc.user_agent
    FROM public.user_consents uc
    WHERE uc.user_id = p_user_id
    ORDER BY uc.consented_at DESC;
  ELSE
    -- Return history for specific consent type
    RETURN QUERY
    SELECT
      uc.id,
      uc.consent_type,
      uc.consent_given,
      uc.consented_at,
      uc.revoked,
      uc.revoked_at,
      uc.privacy_policy_version,
      uc.terms_version,
      uc.consent_text,
      uc.ip_address,
      uc.user_agent
    FROM public.user_consents uc
    WHERE uc.user_id = p_user_id
      AND uc.consent_type = p_consent_type
    ORDER BY uc.consented_at DESC;
  END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

COMMENT ON FUNCTION public.get_consent_history IS 'Get complete consent history for a user (optionally filtered by type)';

-- Function 5: Check if user has all required consents
CREATE OR REPLACE FUNCTION public.has_required_consents(
  p_user_id UUID
)
RETURNS TABLE(
  has_all_required BOOLEAN,
  missing_consents TEXT[]
) AS $$
DECLARE
  v_required_consents TEXT[] := ARRAY['instagram_oauth', 'instagram_data_access', 'terms_acceptance', 'privacy_policy'];
  v_user_consents TEXT[];
  v_missing TEXT[];
BEGIN
  -- Get all active consents for user
  SELECT ARRAY_AGG(DISTINCT consent_type)
  INTO v_user_consents
  FROM public.user_consents
  WHERE user_id = p_user_id
    AND consent_given = TRUE
    AND revoked = FALSE;

  -- Find missing required consents
  SELECT ARRAY_AGG(required)
  INTO v_missing
  FROM UNNEST(v_required_consents) AS required
  WHERE required != ALL(COALESCE(v_user_consents, ARRAY[]::TEXT[]));

  -- Return results
  RETURN QUERY
  SELECT
    (v_missing IS NULL OR ARRAY_LENGTH(v_missing, 1) IS NULL),
    COALESCE(v_missing, ARRAY[]::TEXT[]);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

COMMENT ON FUNCTION public.has_required_consents IS 'Check if user has all required consents (instagram_oauth, instagram_data_access, terms_acceptance, privacy_policy)';

-- Function 6: Get consent audit report
CREATE OR REPLACE FUNCTION public.get_consent_audit_report(
  p_start_date TIMESTAMP WITH TIME ZONE,
  p_end_date TIMESTAMP WITH TIME ZONE
)
RETURNS TABLE(
  consent_type VARCHAR,
  consent_given_count BIGINT,
  consent_denied_count BIGINT,
  revoked_count BIGINT,
  unique_users BIGINT
) AS $$
BEGIN
  RETURN QUERY
  SELECT
    uc.consent_type,
    COUNT(*) FILTER (WHERE uc.consent_given = TRUE) AS consent_given_count,
    COUNT(*) FILTER (WHERE uc.consent_given = FALSE) AS consent_denied_count,
    COUNT(*) FILTER (WHERE uc.revoked = TRUE) AS revoked_count,
    COUNT(DISTINCT uc.user_id) AS unique_users
  FROM public.user_consents uc
  WHERE uc.consented_at BETWEEN p_start_date AND p_end_date
  GROUP BY uc.consent_type
  ORDER BY consent_given_count DESC;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

COMMENT ON FUNCTION public.get_consent_audit_report IS 'Generate consent statistics for a date range (for compliance audits)';

-- ============================================
-- SECTION 8: ENABLE RLS & CREATE POLICIES
-- ============================================

-- Enable RLS
ALTER TABLE public.user_consents ENABLE ROW LEVEL SECURITY;

-- Policy 1: Service role full access
DROP POLICY IF EXISTS "Service role full access" ON public.user_consents;
CREATE POLICY "Service role full access"
  ON public.user_consents
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

-- Policy 2: Users can view their own consents
DROP POLICY IF EXISTS "Users can view own consents" ON public.user_consents;
CREATE POLICY "Users can view own consents"
  ON public.user_consents
  FOR SELECT
  TO authenticated
  USING (user_id = auth.uid());

-- Policy 3: Users can insert their own consents
DROP POLICY IF EXISTS "Users can insert own consents" ON public.user_consents;
CREATE POLICY "Users can insert own consents"
  ON public.user_consents
  FOR INSERT
  TO authenticated
  WITH CHECK (user_id = auth.uid());

-- Policy 4: Users can update their own consents (for revocation)
DROP POLICY IF EXISTS "Users can update own consents" ON public.user_consents;
CREATE POLICY "Users can update own consents"
  ON public.user_consents
  FOR UPDATE
  TO authenticated
  USING (user_id = auth.uid())
  WITH CHECK (user_id = auth.uid());

-- Policy 5: Admins can view all consents
DROP POLICY IF EXISTS "Admins can view all consents" ON public.user_consents;
CREATE POLICY "Admins can view all consents"
  ON public.user_consents
  FOR SELECT
  TO authenticated
  USING (
    EXISTS (
      SELECT 1 FROM public.user_profiles
      WHERE user_profiles.user_id = auth.uid()
        AND user_profiles.user_role IN ('admin', 'super_admin')
    )
  );

-- ============================================
-- SECTION 9: CREATE MATERIALIZED VIEW
-- ============================================

CREATE MATERIALIZED VIEW IF NOT EXISTS public.active_consents_summary AS
SELECT
  consent_type,
  COUNT(*) as total_consents,
  SUM(CASE WHEN consent_given THEN 1 ELSE 0 END) as consents_given,
  SUM(CASE WHEN NOT consent_given THEN 1 ELSE 0 END) as consents_denied,
  COUNT(DISTINCT user_id) as unique_users,
  privacy_policy_version,
  terms_version
FROM public.user_consents
WHERE revoked = FALSE
GROUP BY consent_type, privacy_policy_version, terms_version
ORDER BY total_consents DESC;

-- Create unique index for CONCURRENT refresh
CREATE UNIQUE INDEX IF NOT EXISTS active_consents_summary_unique_idx
  ON public.active_consents_summary (consent_type, COALESCE(privacy_policy_version, ''), COALESCE(terms_version, ''));

COMMENT ON MATERIALIZED VIEW public.active_consents_summary IS 'Aggregated consent statistics for monitoring dashboards (refresh with: REFRESH MATERIALIZED VIEW CONCURRENTLY active_consents_summary)';

-- ============================================
-- SECTION 10: GRANT PERMISSIONS
-- ============================================

GRANT SELECT ON public.user_consents TO authenticated;
GRANT INSERT ON public.user_consents TO authenticated;
GRANT UPDATE ON public.user_consents TO authenticated;

GRANT ALL ON public.user_consents TO service_role;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO service_role;
GRANT SELECT ON public.active_consents_summary TO authenticated;

-- ============================================
-- SECTION 11: VALIDATION
-- ============================================

DO $$
DECLARE
  v_column_count INTEGER;
  v_index_count INTEGER;
  v_function_count INTEGER;
  v_trigger_count INTEGER;
  v_policy_count INTEGER;
BEGIN
  -- Count columns
  SELECT COUNT(*)
  INTO v_column_count
  FROM information_schema.columns
  WHERE table_schema = 'public'
    AND table_name = 'user_consents';

  -- Count indexes
  SELECT COUNT(*)
  INTO v_index_count
  FROM pg_indexes
  WHERE tablename = 'user_consents';

  -- Count functions
  SELECT COUNT(*)
  INTO v_function_count
  FROM information_schema.routines
  WHERE routine_schema = 'public'
    AND routine_name LIKE '%consent%';

  -- Count triggers
  SELECT COUNT(*)
  INTO v_trigger_count
  FROM information_schema.triggers
  WHERE event_object_table = 'user_consents';

  -- Count RLS policies
  SELECT COUNT(*)
  INTO v_policy_count
  FROM pg_policies
  WHERE tablename = 'user_consents';

  RAISE NOTICE '========================================';
  RAISE NOTICE 'SURGICAL MIGRATION COMPLETE';
  RAISE NOTICE '========================================';
  RAISE NOTICE 'Total Columns: % (expected: 18)', v_column_count;
  RAISE NOTICE 'Total Indexes: % (expected: 8+)', v_index_count;
  RAISE NOTICE 'Total Functions: % (expected: 6+)', v_function_count;
  RAISE NOTICE 'Total Triggers: % (expected: 2)', v_trigger_count;
  RAISE NOTICE 'Total RLS Policies: % (expected: 5)', v_policy_count;
  RAISE NOTICE '========================================';

  IF v_column_count < 18 THEN
    RAISE WARNING 'Column count is less than expected 18 columns';
  END IF;
END $$;

COMMIT;

-- ============================================
-- MIGRATION COMPLETE
-- ============================================
-- Next Steps:
-- 1. Verify this migration ran successfully (check NOTICE messages above)
-- 2. Run: npm run db:types
-- 3. Verify database.types.ts now has 18 columns for user_consents
-- 4. Test ConsentService functions work correctly
-- ============================================
