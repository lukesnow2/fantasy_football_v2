-- Drop the weekly_points >= 0 check on edw.fact_roster.
--
-- Negative fantasy scores are legitimate: missed field goals, interceptions,
-- and defenses giving up points all score below zero. The 2005-2025 history
-- has 365 such rows (low: -4), so the constraint made fact_roster unloadable
-- the moment roster publication actually ran.
--
-- Idempotent: safe to re-run.

BEGIN;

ALTER TABLE edw.fact_roster DROP CONSTRAINT IF EXISTS chk_weekly_points;

COMMIT;
