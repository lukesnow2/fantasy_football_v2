-- Make edw.dim_team.team_id unique so the incremental upsert works.
--
-- load_dimension_table upserts ON CONFLICT (team_id), but the table only had
-- an index on that column. Postgres requires a unique constraint for ON
-- CONFLICT, so every incremental dim_team load raised
-- "no unique or exclusion constraint matching the ON CONFLICT specification"
-- and new teams never landed. Full rebuilds masked it by truncating and
-- inserting instead.
--
-- team_id is Yahoo's league-scoped team key (e.g. "153.l.76788.t.1"),
-- verified unique in existing data.
--
-- Idempotent: safe to re-run.

BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'dim_team_team_id_key'
          AND conrelid = 'edw.dim_team'::regclass
    ) THEN
        ALTER TABLE edw.dim_team
            ADD CONSTRAINT dim_team_team_id_key UNIQUE (team_id);
    END IF;
END $$;

COMMIT;
