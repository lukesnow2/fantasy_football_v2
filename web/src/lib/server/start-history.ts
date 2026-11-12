import { sql } from 'drizzle-orm';
import { db } from '$lib/server/db';
import type { StartHistoryRow } from '$lib/start-history';
import { finishedSeasonsCte, isRegularSeasonWeek, madePlayoffs } from './season-history-sql';

/** Rows come back camel-cased with numerics as strings; read them loosely. */
type Row = Record<string, unknown>;

/**
 * Every team-season's record after `week`, in the finished seasons before the
 * newest one (WP4 S4.6), with whether it made the playoffs.
 *
 * Records sum fact_team_performance over weeks 1 to `week`, dropping bracket
 * weeks the way /api/standings does. A finished season is one with a
 * championship game; the newest season is left out whether it's finished or
 * not, since it's the one the page is showing. `madePlayoffs` means the team
 * played in an is_playoffs game that season. The SQL pieces are shared with
 * points-leaders.ts in season-history-sql.ts.
 */
export async function getStartHistoryRows(week: number): Promise<StartHistoryRow[]> {
	const rows = await db.execute(sql`
		WITH ${finishedSeasonsCte},
		records AS (
			SELECT ftp.season_year, ftp.team_key, dt.manager_name,
			       SUM(ftp.wins) AS wins, SUM(ftp.losses) AS losses, SUM(ftp.ties) AS ties
			FROM edw.fact_team_performance ftp
			JOIN edw.dim_week dw ON dw.week_key = ftp.week_key
			JOIN edw.dim_team dt ON dt.team_key = ftp.team_key
			WHERE ftp.season_year IN (SELECT season_year FROM finished)
			  AND dw.week_number BETWEEN 1 AND ${week}
			  AND ${isRegularSeasonWeek}
			GROUP BY ftp.season_year, ftp.team_key, dt.manager_name
		)
		SELECT t.season_year AS season, t.manager_name, t.wins, t.losses, t.ties,
		       ${madePlayoffs} AS made_playoffs
		FROM records t
		ORDER BY t.season_year, t.team_key
	`);
	return (Array.from(rows) as Row[]).map((r) => ({
		season: Number(r.season),
		managerName: r.managerName == null ? null : String(r.managerName),
		wins: Number(r.wins) || 0,
		losses: Number(r.losses) || 0,
		ties: Number(r.ties) || 0,
		madePlayoffs: r.madePlayoffs === true
	}));
}
