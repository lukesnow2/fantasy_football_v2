-- Make (league_id, season_year) unique on edw.dim_league so the incremental
-- upsert works.
--
-- Same defect as 002 on dim_team: load_dimension_table upserts
-- ON CONFLICT (league_id, season_year) but the table carried only indexes, so
-- every incremental dim_league load errored and new seasons never landed.
-- Full rebuilds masked it by truncating and inserting.
--
-- Verified unique in existing data (21 rows, 21 distinct pairs).
--
-- Idempotent: safe to re-run.

BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'dim_league_league_id_season_year_key'
          AND conrelid = 'edw.dim_league'::regclass
    ) THEN
        ALTER TABLE edw.dim_league
            ADD CONSTRAINT dim_league_league_id_season_year_key
            UNIQUE (league_id, season_year);
    END IF;
END $$;

COMMIT;
