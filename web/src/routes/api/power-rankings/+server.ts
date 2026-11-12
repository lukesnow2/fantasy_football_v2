import { json } from '@sveltejs/kit';
import { sql } from 'drizzle-orm';
import { db } from '$lib/server/db';
import { getSeasonOdds } from '$lib/server/playoff-odds-service';
import type { RequestHandler } from './$types';

/**
 * Weekly power rankings, straight off `edw.mart_weekly_power_rankings`.
 *
 * Two things about that table drive the shape of this endpoint:
 *
 *  1. Its `rank_change` column is zero for every row in every season — the ETL
 *     declares it and never fills it. Movement is therefore computed here with
 *     LAG over week_number rather than read from the mart.
 *  2. `dim_team.manager_key` is NULL for every team, so the team -> manager link
 *     has to come from the denormalised `dim_team.manager_name`. Joining
 *     dim_manager through manager_key silently returns nothing.
 *
 * The mart also only covers regular-season weeks (1-14 in recent seasons), which
 * the page surfaces explicitly rather than leaving as an unexplained gap.
 *
 * Playoff odds do NOT come from the mart. Its playoff_odds is a hand-set ladder
 * of rank and games back that backtested badly (teams given 40-50% made it 26%
 * of the time). They come from the same season simulation the This Season
 * widget and manager pages use, run as of the selected week, so a week's odds
 * read the same everywhere.
 */

type NumericLike = string | number | null | undefined;

function num(value: NumericLike): number | null {
	if (value === null || value === undefined || value === '') return null;
	const parsed = typeof value === 'number' ? value : Number(value);
	return Number.isFinite(parsed) ? parsed : null;
}

export interface PowerRankingRow {
	seasonYear: number;
	weekNumber: number;
	teamKey: number;
	teamName: string;
	managerName: string | null;
	/**
	 * Whether /managers/{managerName} exists, i.e. the manager has a row in
	 * mart_manager_performance (which that page reads). The rankings cover every
	 * team, but Yahoo's '--hidden--' placeholder and short-stint early managers
	 * have no profile.
	 */
	managerHasProfile: boolean;
	powerRank: number;
	/** Positive means the team climbed since last week. Null in week 1. */
	rankChange: number | null;
	recordRank: number | null;
	pointsRank: number | null;
	wins: number;
	losses: number;
	ties: number;
	winPercentage: number | null;
	powerScore: number | null;
	strengthOfSchedule: number | null;
	recentFormScore: number | null;
	pythagoreanWins: number | null;
	luckFactor: number | null;
	playoffOdds: number | null;
}

export const GET: RequestHandler = async ({ url }) => {
	try {
		// Which seasons actually have rankings. Driven off the mart, not dim_league:
		// the warehouse has 21 seasons but only the recent ones were marted.
		const seasonRows = Array.from(
			await db.execute(sql`
				SELECT DISTINCT w.season_year AS "seasonYear"
				FROM edw.mart_weekly_power_rankings p
				JOIN edw.dim_week w ON w.week_key = p.week_key
				ORDER BY "seasonYear" DESC
			`)
		) as { seasonYear: number }[];

		const seasons = seasonRows.map((r) => Number(r.seasonYear));

		if (seasons.length === 0) {
			return json({ season: null, week: null, seasons: [], weeks: [], rankings: [], trend: [] });
		}

		const seasonParam = url.searchParams.get('season');
		let season = seasons[0];
		if (seasonParam !== null) {
			const parsed = Number(seasonParam);
			if (!Number.isInteger(parsed) || !seasons.includes(parsed)) {
				return json(
					{ error: 'Invalid season parameter', availableSeasons: seasons },
					{ status: 400 }
				);
			}
			season = parsed;
		}

		// One query for the whole season. The page needs both the requested week
		// (the table) and every week (the trend chart), and LAG needs the full
		// series anyway to compute movement.
		const rows = Array.from(
			await db.execute(sql`
				WITH ranked AS (
					SELECT
						w.season_year          AS "seasonYear",
						w.week_number          AS "weekNumber",
						p.team_key             AS "teamKey",
						t.team_name            AS "teamName",
						t.manager_name         AS "managerName",
						(mp.manager_name IS NOT NULL) AS "managerHasProfile",
						p.power_rank           AS "powerRank",
						p.record_rank          AS "recordRank",
						p.points_rank          AS "pointsRank",
						p.wins                 AS "wins",
						p.losses               AS "losses",
						p.ties                 AS "ties",
						p.win_percentage       AS "winPercentage",
						p.power_score          AS "powerScore",
						p.strength_of_schedule AS "strengthOfSchedule",
						p.recent_form_score    AS "recentFormScore",
						p.pythagorean_wins     AS "pythagoreanWins",
						p.luck_factor          AS "luckFactor",
						p.playoff_odds         AS "playoffOdds",
						LAG(p.power_rank) OVER (
							PARTITION BY p.team_key ORDER BY w.week_number
						)                      AS "prevRank"
					FROM edw.mart_weekly_power_rankings p
					JOIN edw.dim_week w   ON w.week_key  = p.week_key
					JOIN edw.dim_team t   ON t.team_key  = p.team_key
					JOIN edw.dim_league l ON l.league_key = p.league_key
					LEFT JOIN edw.mart_manager_performance mp ON mp.manager_name = t.manager_name
					WHERE w.season_year = ${season}
					  AND l.season_year = ${season}
				)
				SELECT *, ("prevRank" - "powerRank") AS "rankChange"
				FROM ranked
				ORDER BY "weekNumber", "powerRank"
			`)
		) as (Record<string, NumericLike> & { managerHasProfile: boolean })[];

		const all: PowerRankingRow[] = rows.map((r) => ({
			seasonYear: Number(r.seasonYear),
			weekNumber: Number(r.weekNumber),
			teamKey: Number(r.teamKey),
			teamName: String(r.teamName ?? ''),
			managerName: r.managerName == null ? null : String(r.managerName),
			managerHasProfile: r.managerHasProfile === true,
			powerRank: Number(r.powerRank),
			// Null rather than 0 in week 1: there is no previous week to move from,
			// and rendering a 0 there would claim the team held its rank.
			rankChange: num(r.rankChange),
			recordRank: num(r.recordRank),
			pointsRank: num(r.pointsRank),
			wins: Number(r.wins ?? 0),
			losses: Number(r.losses ?? 0),
			ties: Number(r.ties ?? 0),
			winPercentage: num(r.winPercentage),
			powerScore: num(r.powerScore),
			strengthOfSchedule: num(r.strengthOfSchedule),
			recentFormScore: num(r.recentFormScore),
			pythagoreanWins: num(r.pythagoreanWins),
			luckFactor: num(r.luckFactor),
			playoffOdds: num(r.playoffOdds)
		}));

		const weeks = [...new Set(all.map((r) => r.weekNumber))].sort((a, b) => a - b);

		const weekParam = url.searchParams.get('week');
		let week = weeks[weeks.length - 1] ?? null;
		if (weekParam !== null) {
			const parsed = Number(weekParam);
			if (!Number.isInteger(parsed) || !weeks.includes(parsed)) {
				return json({ error: 'Invalid week parameter', availableWeeks: weeks }, { status: 400 });
			}
			week = parsed;
		}

		// Simulated as of the selected week. Unavailable (too few teams loaded)
		// or failed, the column shows a dash rather than the mart's old figures.
		let odds = new Map<number, number>();
		if (season != null && week != null) {
			try {
				const result = await getSeasonOdds(season, week);
				if (result.available) odds = new Map(result.teams.map((t) => [t.teamKey, t.playoffOdds]));
			} catch (oddsError) {
				console.error('[power-rankings] playoff odds simulation failed:', oddsError);
			}
		}

		return json({
			season,
			week,
			seasons,
			weeks,
			rankings: all
				.filter((r) => r.weekNumber === week)
				.map((r) => ({ ...r, playoffOdds: odds.get(r.teamKey) ?? null })),
			trend: all.map((r) => ({ ...r, playoffOdds: null }))
		});
	} catch (error) {
		console.error('Power rankings API error:', error);
		return json({ error: 'Failed to fetch power rankings' }, { status: 500 });
	}
};
