-- Normalize public.rosters.team_id to the bare team number.
--
-- public.rosters holds two different formats for team_id: the bare team
-- number ('1'), and the full Yahoo team key ('449.l.674707.t.1').
--
-- Only the bare number is usable. transform_fact_roster rebuilds the key
-- as f"{league_id}.t.{team_id}" and looks that up in dim_team, so a row
-- already holding the full key becomes
--     449.l.674707.t.449.l.674707.t.1
-- which matches nothing, and the row is counted into missing_keys and
-- dropped. Verified directly against dev: the full-key rows resolve to 0
-- rows in dim_team, the bare-number rows to 1.
--
-- 3,888 rows across 25 league-weeks carried the unusable form, and they
-- are not scattered - they are the final week of every season (2005 w17,
-- 2006 w16, ... 2024 w17), written by whichever extractor performed the
-- historical backfill. The current extractor writes only the bare number
-- (the one code path that wrote the full key could never execute and has
-- been removed), so this is a one-time repair of what is already stored,
-- picked up by the next publication.
--
-- roster_id embeds team_id, so it is rebuilt to match.
--
-- Safe: ux_rosters_key is (league_id, team_id, week, player_id), and no
-- league-week holds both formats for the same player, so normalizing
-- cannot collide (verified: 0 collisions).
--
-- Idempotent: safe to re-run - the WHERE clause matches nothing once done.

BEGIN;

UPDATE public.rosters
SET roster_id = league_id || '_' || split_part(team_id, '.t.', 2)
                || '_' || week::text || '_' || player_id,
    team_id   = split_part(team_id, '.t.', 2)
WHERE team_id LIKE '%.t.%';

DO $$
DECLARE
    remaining bigint;
BEGIN
    SELECT count(*) INTO remaining
    FROM public.rosters WHERE team_id LIKE '%.t.%';

    IF remaining > 0 THEN
        RAISE EXCEPTION
            'migration 008: % roster row(s) still hold a full team key.',
            remaining;
    END IF;
END $$;

COMMIT;
