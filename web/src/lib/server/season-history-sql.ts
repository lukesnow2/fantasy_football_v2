import { sql } from 'drizzle-orm';

/**
 * SQL pieces shared by This Season's history queries (start-history.ts and
 * points-leaders.ts), so the two agree on what counts as a finished season, a
 * regular-season week and a playoff team.
 */

/**
 * The `finished` CTE: seasons with a championship game, before the newest
 * season. The newest is left out whether it's finished or not, since it's the
 * one This Season shows.
 */
export const finishedSeasonsCte = sql`
	finished AS (
		SELECT DISTINCT dl.season_year
		FROM edw.fact_matchup fm
		JOIN edw.dim_league dl ON dl.league_key = fm.league_key
		WHERE fm.is_championship
		  AND dl.season_year < (SELECT MAX(season_year) FROM edw.dim_league)
	)`;

/**
 * True when the `ftp` (fact_team_performance) row's week has no playoff or
 * consolation game, the way /api/standings drops bracket weeks.
 */
export const isRegularSeasonWeek = sql`
	NOT EXISTS (
		SELECT 1 FROM edw.fact_matchup fm
		WHERE fm.league_key = ftp.league_key
		  AND fm.week_key = ftp.week_key
		  AND (fm.is_playoffs OR fm.is_consolation)
	)`;

/** True when team-season `t` (t.season_year, t.team_key) played an is_playoffs game. */
export const madePlayoffs = sql`
	EXISTS (
		SELECT 1 FROM edw.fact_matchup fm
		WHERE fm.season_year = t.season_year AND fm.is_playoffs
		  AND t.team_key IN (fm.team1_key, fm.team2_key)
	)`;
