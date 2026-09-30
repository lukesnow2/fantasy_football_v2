-- Restore the five foreign keys on the mart tables.
--
-- Migration 005 repaired the referential integrity that snapshot/restore
-- silently dropped, but its list covered only the dimension and fact
-- tables: 31 of the 36 foreign keys the schema declares. The three mart
-- tables were missed, so a warehouse repaired by 005 sits at 31 -
-- confirmed on the dev database, which carries exactly 31 and none on
-- mart_league_summary, mart_player_value or mart_weekly_power_rankings,
-- while production (never restored) carries all 36.
--
-- The gap does not self-heal: publish.py replays onto its snapshot only
-- the foreign keys edw currently holds, so once these are gone, every
-- later snapshot and restore reproduces their absence.
--
-- This is a separate file rather than an edit to 005 because 005 was
-- already recorded as applied on the databases that still need these, and
-- the runner tracks migrations by filename.
--
-- Each constraint is added only if absent, only if both columns exist, and
-- only if no orphaned rows would make it unaddable. Idempotent.

BEGIN;

DO $$
DECLARE
    fk      record;
    orphans bigint;
BEGIN
    FOR fk IN
        SELECT * FROM (VALUES
            ('mart_league_summary',        'league_key', 'dim_league', 'league_key'),
            ('mart_player_value',          'player_key', 'dim_player', 'player_key'),
            ('mart_weekly_power_rankings', 'league_key', 'dim_league', 'league_key'),
            ('mart_weekly_power_rankings', 'week_key',   'dim_week',   'week_key'),
            ('mart_weekly_power_rankings', 'team_key',   'dim_team',   'team_key')
        ) AS t(child, child_col, parent, parent_col)
    LOOP
        CONTINUE WHEN NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'edw' AND table_name = fk.child
              AND column_name = fk.child_col);
        CONTINUE WHEN NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'edw' AND table_name = fk.parent
              AND column_name = fk.parent_col);
        CONTINUE WHEN EXISTS (
            SELECT 1 FROM pg_constraint c
            JOIN pg_attribute a
              ON a.attrelid = c.conrelid AND a.attnum = c.conkey[1]
            WHERE c.contype = 'f'
              AND c.conrelid = format('edw.%I', fk.child)::regclass
              AND a.attname = fk.child_col);

        -- An orphan must not fail the migration. The runner applies this
        -- under the advisory lock before any pipeline work, so an unhandled
        -- ALTER TABLE failure would abort the run without recording the
        -- migration - and every run after it, identically.
        EXECUTE format(
            'SELECT count(*) FROM edw.%I c '
            'LEFT JOIN edw.%I p ON p.%I = c.%I '
            'WHERE c.%I IS NOT NULL AND p.%I IS NULL',
            fk.child, fk.parent, fk.parent_col, fk.child_col,
            fk.child_col, fk.parent_col) INTO orphans;
        IF orphans > 0 THEN
            RAISE WARNING
                'skipping FK edw.%.% -> edw.%.%: % orphaned row(s). Repair '
                'the dimension coverage, then re-run this migration.',
                fk.child, fk.child_col, fk.parent, fk.parent_col, orphans;
            CONTINUE;
        END IF;

        EXECUTE format(
            'ALTER TABLE edw.%I ADD CONSTRAINT %I '
            'FOREIGN KEY (%I) REFERENCES edw.%I(%I)',
            fk.child, fk.child || '_' || fk.child_col || '_fkey',
            fk.child_col, fk.parent, fk.parent_col);
        RAISE NOTICE 'restored FK edw.%.% -> edw.%.%',
            fk.child, fk.child_col, fk.parent, fk.parent_col;
    END LOOP;
END $$;

COMMIT;
