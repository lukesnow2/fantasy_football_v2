import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { sql } from 'drizzle-orm';
import type { RequestHandler } from './$types';
import { leagueState, loadLuckBySeason, num } from '$lib/server/manager-seasons';
import type {
	SeasonLuckRow,
	SeasonMovesRow,
	SeasonsPayload,
	TeamSeasonRow
} from '$lib/league-table';

/** A row as db.execute returns it: column names in camelCase, values untyped. */
type DbRow = Record<string, unknown>;

/**
 * Every manager's numbers one season at a time, so the League Table can add
 * up any span of seasons in the browser (`?from=2010&to=2020`) without a
 * request per span. Each row is one piece of a career row on
 * /api/managers/performance, read with the same rules, so adding up every
 * season gives the career numbers back exactly:
 *
 *   - teamSeasons: one row per team-season, as mart_manager_performance builds
 *     its career rows (a manager running two teams in a year gets two rows),
 *     plus the points against, playoff place and game results (W, L, T in
 *     week order, for streaks) the career extras read.
 *   - luck: one row per team-season from the power rankings (loadLuckBySeason).
 *   - moves: transactions, FAAB spent and the starter points those bids bought,
 *     per manager-season, as the mart counts them.
 */
export const GET: RequestHandler = async () => {
	try {
		const [teamResult, resultsResult, movesResult, luck] = await Promise.all([
			db.execute(sql`
				WITH league_state AS (${leagueState}),
				team_seasons AS (
					SELECT
						dt.manager_name AS manager_name,
						dl.season_year AS season_year,
						dt.team_key AS team_key,
						COALESCE(bool_or(ls.finished), false) AS finished,
						SUM(CASE WHEN fm.winner_team_key = dt.team_key THEN 1 ELSE 0 END) AS wins,
						SUM(CASE WHEN fm.winner_team_key != dt.team_key AND fm.is_tie = FALSE THEN 1 ELSE 0 END) AS losses,
						SUM(CASE WHEN fm.is_tie = TRUE THEN 1 ELSE 0 END) AS ties,
						SUM(CASE WHEN fm.team1_key = dt.team_key THEN fm.team1_points
								 WHEN fm.team2_key = dt.team_key THEN fm.team2_points
								 ELSE 0 END) AS points,
						COUNT(DISTINCT fm.matchup_key) AS games,
						SUM(CASE WHEN fm.matchup_type = 'championship' AND fm.winner_team_key = dt.team_key THEN 1 ELSE 0 END) AS titles,
						SUM(CASE WHEN fm.matchup_type = 'championship' THEN 1 ELSE 0 END) AS title_games,
						SUM(CASE WHEN fm.matchup_type IN ('playoffs', 'championship')
								  AND fm.winner_team_key = dt.team_key THEN 1 ELSE 0 END) AS playoff_wins,
						SUM(CASE WHEN fm.matchup_type IN ('playoffs', 'championship')
								  AND fm.winner_team_key <> dt.team_key AND NOT fm.is_tie THEN 1 ELSE 0 END) AS playoff_losses
					FROM edw.dim_team dt
					JOIN edw.dim_league dl ON dl.league_key = dt.league_key
					JOIN edw.dim_manager dm ON dm.manager_name = dt.manager_name
					LEFT JOIN edw.fact_matchup fm ON (fm.team1_key = dt.team_key OR fm.team2_key = dt.team_key)
						AND fm.season_year = dl.season_year
					LEFT JOIN league_state ls ON ls.league_key = dt.league_key
					WHERE dt.is_active AND dm.include_in_analysis
					GROUP BY dt.manager_name, dl.season_year, dt.team_key
				),
				performance AS (
					SELECT team_key,
						SUM(points_for) AS points_for,
						SUM(points_against) AS points_against,
						bool_or(is_playoff_team) AS made_playoffs
					FROM edw.fact_team_performance
					GROUP BY team_key
				)
				SELECT ts.*, p.points_for, p.points_against,
					COALESCE(p.made_playoffs, false) AS made_playoffs
				FROM team_seasons ts
				LEFT JOIN performance p ON p.team_key = ts.team_key
				WHERE ts.games > 0
				ORDER BY ts.manager_name, ts.season_year, ts.team_key
			`),
			// The career streak query's games, one string per team-season.
			db.execute(sql`
				SELECT
					dm.manager_name AS manager_name,
					side.team_key AS team_key,
					string_agg(
						CASE WHEN fm.winner_manager_key = side.manager_key THEN 'W'
							 WHEN fm.winner_manager_key IS NULL THEN 'T'
							 ELSE 'L' END,
						'' ORDER BY dw.week_number
					) AS results
				FROM edw.fact_matchup fm
				CROSS JOIN LATERAL (VALUES
					(fm.manager1_key, fm.team1_key),
					(fm.manager2_key, fm.team2_key)
				) AS side(manager_key, team_key)
				JOIN edw.dim_week dw ON fm.week_key = dw.week_key
				JOIN edw.dim_manager dm ON dm.manager_key = side.manager_key
				WHERE dm.manager_name IS NOT NULL
				GROUP BY dm.manager_name, side.team_key
			`),
			// Moves and FAAB as mart_manager_performance counts them, by season.
			db.execute(sql`
				WITH moves AS (
					SELECT dm.manager_name, ft.season_year, COUNT(ft.transaction_key) AS moves
					FROM edw.dim_manager dm
					JOIN edw.fact_transaction ft ON dm.manager_key = ft.to_manager_key
					WHERE dm.is_active AND dm.include_in_analysis
					GROUP BY dm.manager_name, ft.season_year
				),
				faab_claims AS (
					SELECT
						ft.to_manager_key, ft.to_team_key, ft.player_key, ft.season_year,
						ft.transaction_week AS claim_week, ft.faab_bid,
						LEAD(ft.transaction_week) OVER (
							PARTITION BY ft.to_team_key, ft.player_key
							ORDER BY ft.transaction_date, ft.transaction_key) AS next_claim_week
					FROM edw.fact_transaction ft
					WHERE ft.faab_bid > 0 AND ft.to_team_key IS NOT NULL
				),
				faab AS (
					SELECT
						dm.manager_name, fc.season_year,
						SUM(fc.faab_bid) AS faab_spent,
						SUM(COALESCE((
							SELECT SUM(fr.weekly_points)
							FROM edw.fact_roster fr
							JOIN edw.dim_week dw ON dw.week_key = fr.week_key
							WHERE fr.team_key = fc.to_team_key
							  AND fr.player_key = fc.player_key
							  AND fr.is_starter
							  AND dw.week_number >= fc.claim_week
							  AND (fc.next_claim_week IS NULL OR dw.week_number < fc.next_claim_week)
						), 0)) AS faab_points
					FROM faab_claims fc
					JOIN edw.dim_manager dm ON dm.manager_key = fc.to_manager_key
					WHERE dm.include_in_analysis
					GROUP BY dm.manager_name, fc.season_year
				)
				SELECT
					COALESCE(m.manager_name, f.manager_name) AS manager_name,
					COALESCE(m.season_year, f.season_year) AS season_year,
					COALESCE(m.moves, 0) AS moves,
					COALESCE(f.faab_spent, 0) AS faab_spent,
					COALESCE(f.faab_points, 0) AS faab_points
				FROM moves m
				FULL JOIN faab f ON f.manager_name = m.manager_name AND f.season_year = m.season_year
				ORDER BY 1, 2
			`),
			// The table reads no playoff odds, so no simulation run.
			loadLuckBySeason(null, { odds: false })
		]);

		const results = new Map(
			(Array.from(resultsResult) as DbRow[]).map((r) => [
				`${r.managerName}|${r.teamKey}`,
				String(r.results ?? '')
			])
		);

		const teamSeasons: TeamSeasonRow[] = (Array.from(teamResult) as DbRow[]).map((r) => ({
			managerName: String(r.managerName),
			seasonYear: Number(r.seasonYear),
			teamKey: Number(r.teamKey),
			finished: Boolean(r.finished),
			wins: Number(r.wins ?? 0),
			losses: Number(r.losses ?? 0),
			ties: Number(r.ties ?? 0),
			games: Number(r.games ?? 0),
			points: num(r.points) ?? 0,
			pointsFor: num(r.pointsFor),
			pointsAgainst: num(r.pointsAgainst),
			titles: Number(r.titles ?? 0),
			titleGames: Number(r.titleGames ?? 0),
			playoffWins: Number(r.playoffWins ?? 0),
			playoffLosses: Number(r.playoffLosses ?? 0),
			madePlayoffs: Boolean(r.madePlayoffs),
			results: results.get(`${r.managerName}|${r.teamKey}`) ?? ''
		}));

		const moves: SeasonMovesRow[] = (Array.from(movesResult) as DbRow[]).map((r) => ({
			managerName: String(r.managerName),
			seasonYear: Number(r.seasonYear),
			moves: Number(r.moves ?? 0),
			faabSpent: num(r.faabSpent) ?? 0,
			faabPoints: num(r.faabPoints) ?? 0
		}));

		const luckRows: SeasonLuckRow[] = [...luck.byManager].flatMap(([managerName, seasons]) =>
			seasons.map((s) => ({
				managerName,
				seasonYear: s.seasonYear,
				wins: s.wins,
				ties: s.ties,
				pythagoreanWins: s.pythagoreanWins,
				luckFactor: s.luckFactor
			}))
		);

		const data: SeasonsPayload = { teamSeasons, luck: luckRows, moves };
		return json({ data });
	} catch (error) {
		console.error('[managers/seasons] query failed:', error);
		return json({ error: 'Failed to fetch manager seasons' }, { status: 500 });
	}
};
