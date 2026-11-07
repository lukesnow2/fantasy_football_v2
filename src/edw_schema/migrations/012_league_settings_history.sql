-- Keep each league-season's full Yahoo settings: scoring, bonuses, lineup.
--
-- Migration 011 kept three settings (playoff start, playoff field, trade
-- deadline). The rest of the settings response is the only record of how
-- each season was scored and played, and the league changed it repeatedly:
--
--   2005-07  1 pt per 30 passing yards, 3-pt passing TDs, no reception points,
--            2 QB / 3 RB / 3 WR / 2 TE / 2 DEF lineups (2005 also 2 IDP)
--   2008     a point per reception, 1 per 20 passing yards, 4-pt passing TDs,
--            one-flex lineup; yardage bonuses (first appear 2006)
--   2013     defensive points-allowed scale raised (shutout 10 -> 12)
--   2022     FAAB waivers
--   2025     6-pt passing TDs
--
-- Trends across 21 seasons mean little without this. Raw tables hold what
-- Yahoo sent; edw mirrors them keyed by league_id (not league_key: a full
-- rebuild renumbers league_key, and these must survive it untouched).
--
-- The rows themselves come from Yahoo, through the weekly load for the
-- current season and scripts/backfill_league_settings.py for the past. That
-- backfill also corrects 2007's playoff field: migration 011 counted every
-- team in a playoff-flagged game, consolation bracket included, and recorded
-- 8; Yahoo's settings say 4.
--
-- Idempotent: safe to re-run.

BEGIN;

-- Scalar settings on the raw league row.
ALTER TABLE public.leagues ADD COLUMN IF NOT EXISTS start_date DATE;
ALTER TABLE public.leagues ADD COLUMN IF NOT EXISTS end_date DATE;
ALTER TABLE public.leagues ADD COLUMN IF NOT EXISTS previous_league_id TEXT;
ALTER TABLE public.leagues ADD COLUMN IF NOT EXISTS scoring_type TEXT;
ALTER TABLE public.leagues ADD COLUMN IF NOT EXISTS draft_type TEXT;
ALTER TABLE public.leagues ADD COLUMN IF NOT EXISTS is_auction_draft BOOLEAN;
ALTER TABLE public.leagues ADD COLUMN IF NOT EXISTS draft_time TIMESTAMPTZ;
ALTER TABLE public.leagues ADD COLUMN IF NOT EXISTS uses_playoff BOOLEAN;
ALTER TABLE public.leagues ADD COLUMN IF NOT EXISTS num_playoff_consolation_teams INTEGER;
ALTER TABLE public.leagues ADD COLUMN IF NOT EXISTS uses_playoff_reseeding BOOLEAN;
ALTER TABLE public.leagues ADD COLUMN IF NOT EXISTS has_multiweek_championship BOOLEAN;
ALTER TABLE public.leagues ADD COLUMN IF NOT EXISTS uses_median_score BOOLEAN;
ALTER TABLE public.leagues ADD COLUMN IF NOT EXISTS waiver_type TEXT;
ALTER TABLE public.leagues ADD COLUMN IF NOT EXISTS waiver_rule TEXT;
ALTER TABLE public.leagues ADD COLUMN IF NOT EXISTS waiver_time INTEGER;
ALTER TABLE public.leagues ADD COLUMN IF NOT EXISTS uses_faab BOOLEAN;
ALTER TABLE public.leagues ADD COLUMN IF NOT EXISTS trade_ratify_type TEXT;
ALTER TABLE public.leagues ADD COLUMN IF NOT EXISTS trade_reject_time INTEGER;
ALTER TABLE public.leagues ADD COLUMN IF NOT EXISTS post_draft_players TEXT;
ALTER TABLE public.leagues ADD COLUMN IF NOT EXISTS uses_fractional_points BOOLEAN;
ALTER TABLE public.leagues ADD COLUMN IF NOT EXISTS uses_negative_points BOOLEAN;

-- Points per unit of each stat. points is NULL for a stat the league only
-- displays (targets, rushing attempts).
CREATE TABLE IF NOT EXISTS public.league_scoring (
    league_id       TEXT    NOT NULL,
    stat_id         INTEGER NOT NULL,
    stat_name       TEXT,
    display_name    TEXT,
    stat_group      TEXT,
    position_type   TEXT,
    points          NUMERIC,
    is_display_only BOOLEAN NOT NULL DEFAULT false,
    sort_order      INTEGER,
    extracted_at    TIMESTAMP,
    PRIMARY KEY (league_id, stat_id)
);

-- Threshold bonuses: `points` extra once a game's stat reaches `target`.
CREATE TABLE IF NOT EXISTS public.league_scoring_bonuses (
    league_id    TEXT    NOT NULL,
    stat_id      INTEGER NOT NULL,
    target       NUMERIC NOT NULL,
    points       NUMERIC NOT NULL,
    extracted_at TIMESTAMP,
    PRIMARY KEY (league_id, stat_id, target)
);

CREATE TABLE IF NOT EXISTS public.league_roster_slots (
    league_id     TEXT    NOT NULL,
    position      TEXT    NOT NULL,
    position_type TEXT,
    slot_count    INTEGER NOT NULL,
    is_starting   BOOLEAN,
    sort_order    INTEGER,
    extracted_at  TIMESTAMP,
    PRIMARY KEY (league_id, position)
);

-- The same scalar settings on the warehouse league row.
ALTER TABLE edw.dim_league ADD COLUMN IF NOT EXISTS start_date DATE;
ALTER TABLE edw.dim_league ADD COLUMN IF NOT EXISTS end_date DATE;
ALTER TABLE edw.dim_league ADD COLUMN IF NOT EXISTS previous_league_id VARCHAR(50);
ALTER TABLE edw.dim_league ADD COLUMN IF NOT EXISTS is_auction_draft BOOLEAN;
ALTER TABLE edw.dim_league ADD COLUMN IF NOT EXISTS draft_time TIMESTAMPTZ;
ALTER TABLE edw.dim_league ADD COLUMN IF NOT EXISTS uses_playoff BOOLEAN;
ALTER TABLE edw.dim_league ADD COLUMN IF NOT EXISTS num_playoff_consolation_teams INTEGER;
ALTER TABLE edw.dim_league ADD COLUMN IF NOT EXISTS uses_playoff_reseeding BOOLEAN;
ALTER TABLE edw.dim_league ADD COLUMN IF NOT EXISTS has_multiweek_championship BOOLEAN;
ALTER TABLE edw.dim_league ADD COLUMN IF NOT EXISTS uses_median_score BOOLEAN;
ALTER TABLE edw.dim_league ADD COLUMN IF NOT EXISTS waiver_type VARCHAR(10);
ALTER TABLE edw.dim_league ADD COLUMN IF NOT EXISTS waiver_rule VARCHAR(20);
ALTER TABLE edw.dim_league ADD COLUMN IF NOT EXISTS waiver_time INTEGER;
ALTER TABLE edw.dim_league ADD COLUMN IF NOT EXISTS uses_faab BOOLEAN;
ALTER TABLE edw.dim_league ADD COLUMN IF NOT EXISTS trade_ratify_type VARCHAR(20);
ALTER TABLE edw.dim_league ADD COLUMN IF NOT EXISTS trade_reject_time INTEGER;
ALTER TABLE edw.dim_league ADD COLUMN IF NOT EXISTS post_draft_players VARCHAR(10);
ALTER TABLE edw.dim_league ADD COLUMN IF NOT EXISTS uses_fractional_points BOOLEAN;
ALTER TABLE edw.dim_league ADD COLUMN IF NOT EXISTS uses_negative_points BOOLEAN;

-- scoring_type and draft_type held placeholders ('standard', 'snake') written
-- on every load; nothing read them. Yahoo's values replace them on backfill.

CREATE TABLE IF NOT EXISTS edw.dim_scoring_rule (
    league_id       VARCHAR(50) NOT NULL,
    season_year     INTEGER     NOT NULL,
    stat_id         INTEGER     NOT NULL,
    stat_name       VARCHAR(100),
    display_name    VARCHAR(50),
    stat_group      VARCHAR(50),
    position_type   VARCHAR(10),
    points          NUMERIC,
    is_display_only BOOLEAN     NOT NULL DEFAULT false,
    sort_order      INTEGER,
    PRIMARY KEY (league_id, stat_id)
);
CREATE INDEX IF NOT EXISTS dim_scoring_rule_season_idx ON edw.dim_scoring_rule (season_year);

CREATE TABLE IF NOT EXISTS edw.dim_scoring_bonus (
    league_id   VARCHAR(50) NOT NULL,
    season_year INTEGER     NOT NULL,
    stat_id     INTEGER     NOT NULL,
    target      NUMERIC     NOT NULL,
    points      NUMERIC     NOT NULL,
    PRIMARY KEY (league_id, stat_id, target)
);

CREATE TABLE IF NOT EXISTS edw.dim_roster_slot (
    league_id     VARCHAR(50) NOT NULL,
    season_year   INTEGER     NOT NULL,
    position      VARCHAR(10) NOT NULL,
    position_type VARCHAR(10),
    slot_count    INTEGER     NOT NULL,
    is_starting   BOOLEAN,
    sort_order    INTEGER,
    PRIMARY KEY (league_id, position)
);

COMMIT;
