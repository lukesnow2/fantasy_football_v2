import { sql } from 'drizzle-orm';
import { db } from '$lib/server/db';
import type { PointsLeaderRow } from '$lib/points-leaders';
import { finishedSeasonsCte, isRegularSeasonWeek, madePlayoffs } from './season-history-sql';

/** Rows come back camel-cased with numerics as strings; read them loosely. */
type Row = Record<string, unknown>;

/**
 * Every team-season's regular-season points for, in the finished seasons
 * before the newest one, with whether it made the playoffs and won the title.
 *
 * The same rules as start-history.ts, through season-history-sql.ts: points
 * sum fact_team_performance over the weeks with no playoff or consolation
 * game; a finished season has a championship game; the newest season is left
 * out, finished or not, since it's the one This Season shows.
 */
export async function getPointsLeaderRows(): Promise<PointsLeaderRow[]> {
	const rows = await db.execute(sql`
		WITH ${finishedSeasonsCte},
		points AS (
			SELECT ftp.season_year, ftp.team_key, dt.manager_name,
			       SUM(ftp.points_for) AS points_for
			FROM edw.fact_team_performance ftp
			JOIN edw.dim_team dt ON dt.team_key = ftp.team_key
			WHERE ftp.season_year IN (SELECT season_year FROM finished)
			  AND ${isRegularSeasonWeek}
			GROUP BY ftp.season_year, ftp.team_key, dt.manager_name
		)
		SELECT t.season_year AS season, t.manager_name, t.points_for,
		       ${madePlayoffs} AS made_playoffs,
		       EXISTS (
				SELECT 1 FROM edw.fact_matchup fm
				WHERE fm.season_year = t.season_year AND fm.is_championship
				  AND fm.winner_team_key = t.team_key
		       ) AS won_title
		FROM points t
		ORDER BY t.season_year, t.team_key
	`);
	return (Array.from(rows) as Row[]).map((r) => ({
		season: Number(r.season),
		managerName: r.managerName == null ? null : String(r.managerName),
		pointsFor: Number(r.pointsFor) || 0,
		madePlayoffs: r.madePlayoffs === true,
		wonTitle: r.wonTitle === true
	}));
}
