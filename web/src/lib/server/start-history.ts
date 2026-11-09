import { sql } from 'drizzle-orm';
import { db } from '$lib/server/db';
import type { StartHistoryRow } from '$lib/start-history';

/** Rows come back camel-cased with numerics as strings; read them loosely. */
type Row = Record<string, unknown>;

/**
 * Every team-season's record after `week`, in the finished seasons before the
 * newest one (WP4 S4.2), with whether it made the playoffs and won the title.
 *
 * Records sum fact_team_performance over weeks 1 to `week`, dropping bracket
 * weeks the way /api/standings does. A finished season is one with a
 * championship game; the newest season is left out whether it's finished or
 * not, since it's the one the page is showing. `madePlayoffs` means the team
 * played in an is_playoffs game that season, and `wonTitle` that it won the
 * season's is_championship game.
 */
export async function getStartHistoryRows(week: number): Promise<StartHistoryRow[]> {
	const rows = await db.execute(sql`
		WITH finished AS (
			SELECT DISTINCT dl.season_year
			FROM edw.fact_matchup fm
			JOIN edw.dim_league dl ON dl.league_key = fm.league_key
			WHERE fm.is_championship
			  AND dl.season_year < (SELECT MAX(season_year) FROM edw.dim_league)
		),
		records AS (
			SELECT ftp.season_year, ftp.team_key, dt.manager_name,
			       SUM(ftp.wins) AS wins, SUM(ftp.losses) AS losses, SUM(ftp.ties) AS ties
			FROM edw.fact_team_performance ftp
			JOIN edw.dim_week dw ON dw.week_key = ftp.week_key
			JOIN edw.dim_team dt ON dt.team_key = ftp.team_key
			WHERE ftp.season_year IN (SELECT season_year FROM finished)
			  AND dw.week_number BETWEEN 1 AND ${week}
			  AND NOT EXISTS (
				SELECT 1 FROM edw.fact_matchup fm
				WHERE fm.league_key = ftp.league_key
				  AND fm.week_key = ftp.week_key
				  AND (fm.is_playoffs OR fm.is_consolation)
			  )
			GROUP BY ftp.season_year, ftp.team_key, dt.manager_name
		)
		SELECT r.season_year AS season, r.manager_name, r.wins, r.losses, r.ties,
		       EXISTS (
				SELECT 1 FROM edw.fact_matchup fm
				WHERE fm.season_year = r.season_year AND fm.is_playoffs
				  AND r.team_key IN (fm.team1_key, fm.team2_key)
		       ) AS made_playoffs,
		       EXISTS (
				SELECT 1 FROM edw.fact_matchup fm
				WHERE fm.season_year = r.season_year AND fm.is_championship
				  AND fm.winner_team_key = r.team_key
		       ) AS won_title
		FROM records r
		ORDER BY r.season_year, r.team_key
	`);
	return (Array.from(rows) as Row[]).map((r) => ({
		season: Number(r.season),
		managerName: r.managerName == null ? null : String(r.managerName),
		wins: Number(r.wins) || 0,
		losses: Number(r.losses) || 0,
		ties: Number(r.ties) || 0,
		madePlayoffs: r.madePlayoffs === true,
		wonTitle: r.wonTitle === true
	}));
}
