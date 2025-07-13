-- Migration: Link Authentication System to Manager System
-- This connects the existing Lucia auth system with fantasy managers

-- Create app schema for application data
CREATE SCHEMA IF NOT EXISTS app;

-- Add manager_key to user table to link authenticated users to managers  
ALTER TABLE app."user" ADD COLUMN manager_key INTEGER;
ALTER TABLE app."user" ADD CONSTRAINT fk_user_manager 
    FOREIGN KEY (manager_key) REFERENCES edw.dim_manager(manager_key);

-- Create unique constraint to ensure one user per manager
ALTER TABLE app."user" ADD CONSTRAINT unique_user_manager_key UNIQUE (manager_key);

-- Add email to user table if needed (for password reset, notifications)
ALTER TABLE app."user" ADD COLUMN email VARCHAR(255);
ALTER TABLE app."user" ADD CONSTRAINT unique_user_email UNIQUE (email);

-- Add display preferences for authenticated managers
ALTER TABLE app."user" ADD COLUMN display_name VARCHAR(255);
ALTER TABLE app."user" ADD COLUMN profile_settings JSONB DEFAULT '{}';
ALTER TABLE app."user" ADD COLUMN notification_preferences JSONB DEFAULT '{
    "email_on_new_proposal": true,
    "email_on_vote_results": true,
    "email_on_trade_offers": false
}';

-- Update existing manager records to be ready for auth linking
UPDATE edw.dim_manager SET 
    manager_id = LOWER(REPLACE(REPLACE(manager_name, ' ', '_'), '.', ''))
WHERE manager_id IS NULL;

-- Sample data: Create user accounts for each current manager
-- (In production, managers would register themselves)
INSERT INTO app."user" (id, username, password_hash, email, display_name, manager_key) VALUES
-- Note: These are placeholder passwords - managers need to register with real passwords
('craig_user', 'craig', '$argon2id$v=19$m=19456,t=2,p=1$placeholder', 'craig@league.com', 'Craig', 
    (SELECT manager_key FROM edw.dim_manager WHERE manager_name = 'Craig')),
('erik_user', 'erik', '$argon2id$v=19$m=19456,t=2,p=1$placeholder', 'erik@league.com', 'Erik Snow', 
    (SELECT manager_key FROM edw.dim_manager WHERE manager_name = 'Erik Snow')),
('gabe_f_user', 'gabe_flores', '$argon2id$v=19$m=19456,t=2,p=1$placeholder', 'gabe.f@league.com', 'Gabe Flores', 
    (SELECT manager_key FROM edw.dim_manager WHERE manager_name = 'Gabe Flores')),
('gabe_y_user', 'gabe_younger', '$argon2id$v=19$m=19456,t=2,p=1$placeholder', 'gabe.y@league.com', 'Gabe the Younger', 
    (SELECT manager_key FROM edw.dim_manager WHERE manager_name = 'Gabe the Younger')),
('israel_user', 'israel', '$argon2id$v=19$m=19456,t=2,p=1$placeholder', 'israel@league.com', 'Israel', 
    (SELECT manager_key FROM edw.dim_manager WHERE manager_name = 'Israel')),
('luke_user', 'luke_s', '$argon2id$v=19$m=19456,t=2,p=1$placeholder', 'luke@league.com', 'Luke S', 
    (SELECT manager_key FROM edw.dim_manager WHERE manager_name = 'Luke S')),
('nick_user', 'nick', '$argon2id$v=19$m=19456,t=2,p=1$placeholder', 'nick@league.com', 'Nick', 
    (SELECT manager_key FROM edw.dim_manager WHERE manager_name = 'Nick')),
('trevor_user', 'trevor', '$argon2id$v=19$m=19456,t=2,p=1$placeholder', 'trevor@league.com', 'Trevor', 
    (SELECT manager_key FROM edw.dim_manager WHERE manager_name = 'Trevor')),
('troy_user', 'troy', '$argon2id$v=19$m=19456,t=2,p=1$placeholder', 'troy@league.com', 'Troy', 
    (SELECT manager_key FROM edw.dim_manager WHERE manager_name = 'Troy Colvin')),
('omar_user', 'omar', '$argon2id$v=19$m=19456,t=2,p=1$placeholder', 'omar@league.com', 'Omar', 
    (SELECT manager_key FROM edw.dim_manager WHERE manager_name = 'Omar'))
ON CONFLICT (username) DO NOTHING;

-- Index for performance
CREATE INDEX idx_user_manager_key ON app."user" (manager_key);
CREATE INDEX idx_user_email ON app."user" (email);

-- Comments
COMMENT ON COLUMN app."user".manager_key IS 'Links authenticated user to their fantasy manager profile';
COMMENT ON COLUMN app."user".notification_preferences IS 'JSON object storing user notification settings';
COMMENT ON COLUMN app."user".profile_settings IS 'JSON object for user interface preferences'; 