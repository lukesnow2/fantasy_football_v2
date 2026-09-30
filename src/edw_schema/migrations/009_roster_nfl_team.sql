-- Record each rostered player's NFL team, per week.
--
-- The draft board showed "Unknown" for every player: the ETL hardcoded
-- dim_player.nfl_team = 'Unknown'. Yahoo does return the team on the roster
-- call the pipeline already makes - but for a past season it returns the
-- player's CURRENT team (2017 Aaron Rodgers comes back as Pittsburgh, 2005
-- Peyton Manning as Denver). So the team is captured only for the season being
-- played, week by week, and kept with the roster row it came from: a 2026
-- draft board still shows 2026 teams in 2030. Past seasons stay NULL rather
-- than wrong.
--
-- The index serves the draft API's lookup of a drafted player's team in that
-- league-season (earliest rostered week).
--
-- Idempotent: safe to re-run.

BEGIN;

ALTER TABLE public.rosters   ADD COLUMN IF NOT EXISTS nfl_team VARCHAR(10);
ALTER TABLE edw.fact_roster  ADD COLUMN IF NOT EXISTS nfl_team VARCHAR(10);

CREATE INDEX IF NOT EXISTS idx_fact_roster_player_league_week
    ON edw.fact_roster (player_key, league_key, week_key);

COMMIT;
