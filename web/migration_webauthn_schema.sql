-- Migration: Add WebAuthn Support for Biometric Passkeys
-- This adds the necessary tables for WebAuthn credential storage

-- Create WebAuthn credentials table
CREATE TABLE app.webauthn_credentials (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES app.user(id) ON DELETE CASCADE,
    credential_id TEXT NOT NULL UNIQUE,
    public_key TEXT NOT NULL,
    sign_count BIGINT NOT NULL DEFAULT 0,
    transports TEXT[], -- ['usb', 'nfc', 'ble', 'internal']
    backup_eligible BOOLEAN NOT NULL DEFAULT false,
    backup_state BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMP DEFAULT NOW(),
    last_used_at TIMESTAMP,
    device_type VARCHAR(50), -- 'phone', 'laptop', 'desktop', 'tablet'
    authenticator_type VARCHAR(50) -- 'platform', 'cross-platform'
);

-- Create indexes for performance
CREATE INDEX idx_webauthn_user_id ON app.webauthn_credentials(user_id);
CREATE INDEX idx_webauthn_credential_id ON app.webauthn_credentials(credential_id);
CREATE INDEX idx_webauthn_last_used ON app.webauthn_credentials(last_used_at);

-- Add WebAuthn challenge table for registration/authentication
CREATE TABLE app.webauthn_challenges (
    id TEXT PRIMARY KEY,
    challenge TEXT NOT NULL,
    user_id TEXT REFERENCES app.user(id),
    type VARCHAR(20) NOT NULL, -- 'registration', 'authentication'
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create index for challenge cleanup
CREATE INDEX idx_webauthn_challenge_expires ON app.webauthn_challenges(expires_at);

-- Add passkey-specific fields to user table
ALTER TABLE app.user ADD COLUMN passkey_enabled BOOLEAN DEFAULT false;
ALTER TABLE app.user ADD COLUMN passkey_registered_at TIMESTAMP;
ALTER TABLE app.user ADD COLUMN backup_codes TEXT[]; -- For emergency access

-- Create backup codes table for emergency access
CREATE TABLE app.backup_codes (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES app.user(id) ON DELETE CASCADE,
    code_hash TEXT NOT NULL,
    used BOOLEAN DEFAULT false,
    used_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_backup_codes_user_id ON app.backup_codes(user_id);
CREATE INDEX idx_backup_codes_used ON app.backup_codes(used);

-- Add comment explaining the migration
COMMENT ON TABLE app.webauthn_credentials IS 'Stores WebAuthn credentials for biometric passkey authentication';
COMMENT ON TABLE app.webauthn_challenges IS 'Temporary challenges for WebAuthn registration/authentication';
COMMENT ON TABLE app.backup_codes IS 'Emergency backup codes for passkey users'; 