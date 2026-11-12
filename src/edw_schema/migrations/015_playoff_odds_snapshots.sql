-- Keep what the playoff-odds model said each week, so it can be graded.
--
-- The site computes playoff odds on request (web/src/lib/playoff-odds.ts) and
-- stores nothing, so last week's forecast is gone once this week loads. These
-- tables keep each forecast as it was published: one snapshot per season,
-- week and model version, with every team's odds and the record they were
-- based on. web/scripts/snapshot-playoff-odds.ts writes them after each
-- weekly load; web/scripts/grade-playoff-odds.ts scores them once the
-- playoffs settle who got in and who won.
--
-- They live in their own schema, not edw, on purpose. A full EDW rebuild and
-- scripts/promote_to_neon.sh both drop and recreate edw, and drizzle-kit
-- manages app; a forecast cannot be re-derived after the fact (later stat
-- corrections and model changes would rewrite it), so nothing that rebuilds
-- those schemas may touch this one. For the same reason there are no foreign
-- keys into edw: team_key is renumbered by a rebuild, so each row also keeps
-- Yahoo's team key (team_id), which is stable.
--
-- Idempotent: safe to re-run.

BEGIN;

CREATE SCHEMA IF NOT EXISTS model;

CREATE TABLE IF NOT EXISTS model.playoff_odds_snapshots (
    snapshot_id          BIGSERIAL PRIMARY KEY,
    season_year          INTEGER NOT NULL,
    -- The last regular-season week whose games the forecast saw.
    through_week         INTEGER NOT NULL,
    regular_season_weeks INTEGER NOT NULL,
    playoff_teams        INTEGER NOT NULL,
    byes                 INTEGER NOT NULL,
    -- 'league-settings' or 'inferred' (league-format.ts).
    format_source        TEXT NOT NULL,
    sims                 INTEGER NOT NULL,
    league_average       NUMERIC(8, 3) NOT NULL,
    -- Hash of what the model returns on fixed sample seasons
    -- (web/src/lib/server/playoff-odds-version.ts): a change to the numbers
    -- starts a new version, graded separately from the old one.
    model_version        TEXT NOT NULL,
    model_params         JSONB NOT NULL,
    -- Every game and score the forecast saw (playoff-odds.ts
    -- gamesFingerprint). A stat correction changes it.
    games_fingerprint    TEXT NOT NULL,
    -- False when the week was already over at capture time: the forecast was
    -- rebuilt later (first run, or a new model version), not taken live.
    captured_live        BOOLEAN NOT NULL,
    git_sha              TEXT,
    captured_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT playoff_odds_snapshots_once
        UNIQUE (season_year, through_week, model_version, games_fingerprint)
);

CREATE INDEX IF NOT EXISTS playoff_odds_snapshots_season_week
    ON model.playoff_odds_snapshots (season_year, through_week);

CREATE TABLE IF NOT EXISTS model.playoff_odds_snapshot_teams (
    snapshot_id         BIGINT NOT NULL
        REFERENCES model.playoff_odds_snapshots (snapshot_id) ON DELETE CASCADE,
    team_id             TEXT NOT NULL,      -- Yahoo team key, stable
    team_key            INTEGER NOT NULL,   -- edw key at capture time
    team_name           TEXT NOT NULL,
    manager_name        TEXT,
    wins                INTEGER NOT NULL,
    losses              INTEGER NOT NULL,
    ties                INTEGER NOT NULL,
    points_for          NUMERIC(9, 2) NOT NULL,
    projected_points    NUMERIC(8, 3) NOT NULL,
    projected_wins      NUMERIC(6, 3) NOT NULL,
    remaining_games     INTEGER NOT NULL,
    playoff_odds        NUMERIC(6, 5) NOT NULL,
    bye_odds            NUMERIC(6, 5) NOT NULL,
    top_seed_odds       NUMERIC(6, 5) NOT NULL,
    title_odds          NUMERIC(6, 5) NOT NULL,
    clinched            BOOLEAN NOT NULL,
    eliminated          BOOLEAN NOT NULL,
    PRIMARY KEY (snapshot_id, team_id)
);

COMMIT;
