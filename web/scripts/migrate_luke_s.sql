-- Migration Script: Enable Passkey Authentication for Luke S
-- This migrates Luke S from password-only to hybrid authentication

-- Step 1: Verify Luke S's current account
SELECT 
    id, 
    username, 
    manager_key, 
    passkey_enabled, 
    account_status,
    'Luke S' as manager_name
FROM app.user u
LEFT JOIN edw.dim_manager dm ON u.manager_key = dm.manager_key
WHERE u.username = 'linkin22luke';

-- Step 2: Enable passkey authentication (hybrid mode)
UPDATE app.user 
SET 
    passkey_enabled = true,
    passkey_registered_at = NOW(),
    updated_at = NOW()
WHERE username = 'linkin22luke';

-- Step 3: Verify the migration
SELECT 
    id, 
    username, 
    manager_key, 
    passkey_enabled, 
    passkey_registered_at,
    account_status,
    'Luke S' as manager_name
FROM app.user u
LEFT JOIN edw.dim_manager dm ON u.manager_key = dm.manager_key
WHERE u.username = 'linkin22luke'; 