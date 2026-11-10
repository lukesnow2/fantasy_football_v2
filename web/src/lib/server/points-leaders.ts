import { sql } from 'drizzle-orm';
import { db } from '$lib/server/db';
import type { PointsLeaderRow } from '$lib/points-leaders';

/** Rows come back camel-cased with numerics as strings; read them loosely. */
type Row = Record<string, unknown>;

/**
 * Every team-season's regular-season points for, in the finished seasons
 * before the newest one, with whether it made the playoffs and won the title.
 *
 * The same rules as start-history.ts: points sum fact_team_performance over
 * the weeks with no playoff or consolation game; a finished season has a
 * championship game; the newest season is left out, finished or not, since
 * it's the one This Season shows.
 */
export async function getPointsLeaderRows(): Promise<PointsLeaderRow[]> {
	const rows = await db.execute(sql`
		WITH finished AS (
			SELECT DISTINCT dl.season_year
			FROM edw.fact_matchup fm
			JOIN edw.dim_league dl ON dl.league_key = fm.league_key
			WHERE fm.is_championship
			  AND dl.season_year < (SELECT MAX(season_year) FROM edw.dim_league)
		),
		points AS (
			SELECT ftp.season_year, ftp.team_key, dt.manager_name,
			       SUM(ftp.points_for) AS points_for
			FROM edw.fact_team_performance ftp
			JOIN edw.dim_team dt ON dt.team_key = ftp.team_key
			WHERE ftp.season_year IN (SELECT season_year FROM finished)
			  AND NOT EXISTS (
				SELECT 1 FROM edw.fact_matchup fm
				WHERE fm.league_key = ftp.league_key
				  AND fm.week_key = ftp.week_key
				  AND (fm.is_playoffs OR fm.is_consolation)
			  )
			GROUP BY ftp.season_year, ftp.team_key, dt.manager_name
		)
		SELECT p.season_year AS season, p.manager_name, p.points_for,
		       EXISTS (
				SELECT 1 FROM edw.fact_matchup fm
				WHERE fm.season_year = p.season_year AND fm.is_playoffs
				  AND p.team_key IN (fm.team1_key, fm.team2_key)
		       ) AS made_playoffs,
		       EXISTS (
				SELECT 1 FROM edw.fact_matchup fm
				WHERE fm.season_year = p.season_year AND fm.is_championship
				  AND fm.winner_team_key = p.team_key
		       ) AS won_title
		FROM points p
		ORDER BY p.season_year, p.team_key
	`);
	return (Array.from(rows) as Row[]).map((r) => ({
		season: Number(r.season),
		managerName: r.managerName == null ? null : String(r.managerName),
		pointsFor: Number(r.pointsFor) || 0,
		madePlayoffs: r.madePlayoffs === true,
		wonTitle: r.wonTitle === true
	}));
}
