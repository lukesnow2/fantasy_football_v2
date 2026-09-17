-- Restore the edw foreign keys that snapshot/restore silently dropped.
--
-- clone_edw_snapshot built its snapshot with CREATE TABLE (LIKE ...
-- INCLUDING ALL), which does not copy FOREIGN KEY constraints. Every failed
-- publish therefore promoted a schema with no referential integrity and
-- dropped the original, permanently. Production (never restored) carried 36
-- foreign keys; a database that had been through restores carried none.
--
-- publish.py now replays foreign keys onto the snapshot, so this is a
-- one-time repair for databases that already lost them.
--
-- Each constraint is added only if absent, only if both tables exist, and
-- only if no orphaned rows would make it unaddable, so this is idempotent
-- and safe on a database that never lost them.
--
-- Covers 31 of the 36; the five on the mart tables are migration 007.

BEGIN;

DO $$
DECLARE
    fk      record;
    orphans bigint;
BEGIN
    FOR fk IN
        SELECT * FROM (VALUES
            ('dim_team',               'league_key',      'dim_league',   'league_key'),
            ('dim_team',               'manager_key',     'dim_manager',  'manager_key'),
            ('fact_matchup',           'league_key',      'dim_league',   'league_key'),
            ('fact_matchup',           'week_key',        'dim_week',     'week_key'),
            ('fact_matchup',           'team1_key',       'dim_team',     'team_key'),
            ('fact_matchup',           'team2_key',       'dim_team',     'team_key'),
            ('fact_matchup',           'manager1_key',    'dim_manager',  'manager_key'),
            ('fact_matchup',           'manager2_key',    'dim_manager',  'manager_key'),
            ('fact_matchup',           'winner_team_key', 'dim_team',     'team_key'),
            ('fact_matchup',           'winner_manager_key','dim_manager','manager_key'),
            ('fact_roster',            'team_key',        'dim_team',     'team_key'),
            ('fact_roster',            'manager_key',     'dim_manager',  'manager_key'),
            ('fact_roster',            'player_key',      'dim_player',   'player_key'),
            ('fact_roster',            'league_key',      'dim_league',   'league_key'),
            ('fact_roster',            'week_key',        'dim_week',     'week_key'),
            ('fact_transaction',       'league_key',      'dim_league',   'league_key'),
            ('fact_transaction',       'player_key',      'dim_player',   'player_key'),
            ('fact_transaction',       'from_team_key',   'dim_team',     'team_key'),
            ('fact_transaction',       'to_team_key',     'dim_team',     'team_key'),
            ('fact_transaction',       'from_manager_key','dim_manager',  'manager_key'),
            ('fact_transaction',       'to_manager_key',  'dim_manager',  'manager_key'),
            ('fact_draft',             'league_key',      'dim_league',   'league_key'),
            ('fact_draft',             'team_key',        'dim_team',     'team_key'),
            ('fact_draft',             'manager_key',     'dim_manager',  'manager_key'),
            ('fact_draft',             'player_key',      'dim_player',   'player_key'),
            ('fact_player_statistics', 'league_key',      'dim_league',   'league_key'),
            ('fact_player_statistics', 'player_key',      'dim_player',   'player_key'),
            ('fact_team_performance',  'team_key',        'dim_team',     'team_key'),
            ('fact_team_performance',  'manager_key',     'dim_manager',  'manager_key'),
            ('fact_team_performance',  'league_key',      'dim_league',   'league_key'),
            ('fact_team_performance',  'week_key',        'dim_week',     'week_key')
            -- NOTE: this list covers 31 of the schema's 36 foreign keys.
            -- The five on the mart tables are restored by migration 007,
            -- kept separate because this file had already been recorded as
            -- applied on databases that still needed them.
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
        -- Orphans would make the constraint unaddable; skip loudly rather
        -- than failing the whole migration. This check is the difference
        -- between one warning and a pipeline that can never run again: the
        -- migration runner applies this file under the advisory lock before
        -- any work, an unhandled ALTER TABLE failure aborts the run without
        -- recording the migration, and every subsequent run then fails
        -- identically. And the databases that need this repair are exactly
        -- the ones that published while their foreign keys were missing, so
        -- an orphan here is a live possibility, not a hypothetical.
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
