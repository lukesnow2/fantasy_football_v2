-- Remove data for weeks before a league's own start week.
--
-- 2006 and 2007 began in week 2: there are no week-1 matchups and no week-1
-- dim_week row, but the original loads stored week-1 statistics anyway (237
-- rows for 2006, 192 for 2007), and those points were counted in season
-- totals for a week the league never played. Dev had them removed by hand on
-- 2026-09-28; this makes every database - including one rebuilt from the
-- tracked snapshot, and production at cutover - end up the same way.
-- incremental_load now refuses --weeks outside start..end, so nothing puts
-- them back.
--
-- Driven by public.leagues.start_week, not by league ids. Idempotent.

BEGIN;

DELETE FROM public.statistics s
USING public.leagues l
WHERE s.league_id = l.league_id
  AND NULLIF(l.start_week, '') IS NOT NULL
  AND s.week_number < NULLIF(l.start_week, '')::int;

DELETE FROM public.rosters r
USING public.leagues l
WHERE r.league_id = l.league_id
  AND NULLIF(l.start_week, '') IS NOT NULL
  AND r.week < NULLIF(l.start_week, '')::int;

DELETE FROM edw.fact_player_statistics f
USING edw.dim_league dl, public.leagues l
WHERE dl.league_key = f.league_key
  AND l.league_id = dl.league_id
  AND NULLIF(l.start_week, '') IS NOT NULL
  AND f.week_number < NULLIF(l.start_week, '')::int;

-- Migrations run before the pipeline creates its state tables on a fresh
-- database, so only touch pipeline_periods where it exists.
DO $$
BEGIN
    IF to_regclass('public.pipeline_periods') IS NOT NULL THEN
        DELETE FROM public.pipeline_periods p
        USING public.leagues l
        WHERE p.league_id = l.league_id
          AND NULLIF(l.start_week, '') IS NOT NULL
          AND p.week < NULLIF(l.start_week, '')::int;
    END IF;
END $$;

COMMIT;
