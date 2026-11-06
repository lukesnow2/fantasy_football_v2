-- Store each league-season's playoff format.
--
-- Yahoo's league settings say when the playoffs start and how many teams make
-- them, but the pipeline kept neither, so everything downstream guessed:
-- dim_season assumed a three-round bracket ending at end_week, and the web
-- app's playoff odds took the regular-season length from the last finished
-- season and hardcoded six playoff teams. A format change would have been
-- simulated wrongly for weeks before the guesses caught up.
--
--   end_week            last week of the fantasy season, playoffs included
--   playoff_start_week  first playoff week; the regular season is the weeks before it
--   num_playoff_teams   teams in the championship bracket
--
-- Going forward the incremental load captures these from Yahoo's settings.
-- Finished seasons are backfilled here from the games actually played, which
-- the brackets already record (is_playoffs). The backfill never overwrites a
-- value Yahoo supplied, and the ETL's upsert never overwrites a stored value
-- with a missing one, so re-publishing an old season keeps it.
--
-- Idempotent: safe to re-run.

BEGIN;

ALTER TABLE public.leagues  ADD COLUMN IF NOT EXISTS playoff_start_week INTEGER;
ALTER TABLE public.leagues  ADD COLUMN IF NOT EXISTS num_playoff_teams INTEGER;

ALTER TABLE edw.dim_league  ADD COLUMN IF NOT EXISTS end_week INTEGER;
ALTER TABLE edw.dim_league  ADD COLUMN IF NOT EXISTS playoff_start_week INTEGER;
ALTER TABLE edw.dim_league  ADD COLUMN IF NOT EXISTS num_playoff_teams INTEGER;

WITH games AS (
    SELECT fm.league_key, dw.week_number AS week, fm.is_playoffs,
           fm.team1_key, fm.team2_key
    FROM edw.fact_matchup fm
    JOIN edw.dim_week dw ON dw.week_key = fm.week_key
),
bracket AS (
    SELECT league_key,
           MAX(week)                           AS last_week,
           MIN(week) FILTER (WHERE is_playoffs) AS first_playoff_week
    FROM games
    GROUP BY league_key
),
field AS (
    -- Everyone who played a playoff game, byes included (they play in round two).
    SELECT league_key, COUNT(DISTINCT team) AS teams
    FROM (
        SELECT league_key, team1_key AS team FROM games WHERE is_playoffs
        UNION
        SELECT league_key, team2_key FROM games WHERE is_playoffs
    ) t
    GROUP BY league_key
)
UPDATE edw.dim_league dl
SET end_week           = COALESCE(dl.end_week, b.last_week),
    playoff_start_week = COALESCE(dl.playoff_start_week, b.first_playoff_week),
    num_playoff_teams  = COALESCE(dl.num_playoff_teams, f.teams)
FROM bracket b
JOIN field f ON f.league_key = b.league_key
-- Only seasons whose bracket has been played; a season in progress gets its
-- settings from Yahoo on the next load instead of a partial guess here.
WHERE b.league_key = dl.league_key
  AND b.first_playoff_week IS NOT NULL;

COMMIT;
