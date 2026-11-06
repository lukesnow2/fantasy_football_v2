import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { sql } from 'drizzle-orm';
import type { RequestHandler } from './$types';

/**
 * Postgres `numeric` arrives over the wire as a string, so every average, rate
 * and points total in this payload was a string. Consumers coerced by accident
 * (`'0.4317' * 100` happens to work) right up until one of them called
 * `.toFixed()` and crashed the whole manager profile page. Coerce once, here,
 * so the API's contract is "numbers are numbers".
 */
function num(value: unknown): number | null {
	if (value === null || value === undefined || value === '') return null;
	const parsed = typeof value === 'number' ? value : Number(value);
	return Number.isFinite(parsed) ? parsed : null;
}

type SeasonLuck = {
	seasonYear: number;
	weekNumber: number;
	wins: number;
	ties: number;
	pythagoreanWins: number | null;
	luckFactor: number | null;
	playoffOdds: number | null;
	inProgress: boolean;
};

/**
 * Each manager's expected (Pythagorean) wins and luck, one row per season,
 * taken from the power rankings' latest ranked week of that season: the final
 * regular-season standing once it is over, this week's while it is still
 * being played. Reused rather than recomputed so the manager pages and the
 * power rankings always show the same numbers.
 *
 * A season is in progress until its championship game is in fact_matchup.
 * Wins are regular-season only, like the expected wins they are compared with.
 */
async function loadLuckBySeason(): Promise<Map<string, SeasonLuck[]>> {
	const rows = Array.from(
		await db.execute(sql`
			WITH latest AS (
				SELECT
					t.manager_name AS manager_name,
					l.season_year  AS season_year,
					w.week_number  AS week_number,
					p.wins, p.ties, p.pythagorean_wins, p.luck_factor, p.playoff_odds,
					p.league_key,
					ROW_NUMBER() OVER (
						PARTITION BY p.league_key, p.team_key ORDER BY w.week_number DESC
					) AS rn
				FROM edw.mart_weekly_power_rankings p
				JOIN edw.dim_week w   ON w.week_key   = p.week_key
				JOIN edw.dim_team t   ON t.team_key   = p.team_key
				JOIN edw.dim_league l ON l.league_key = p.league_key
				WHERE t.manager_name IS NOT NULL
			)
			SELECT
				manager_name, season_year, week_number, wins, ties,
				pythagorean_wins, luck_factor, playoff_odds,
				NOT EXISTS (
					SELECT 1 FROM edw.fact_matchup fm
					WHERE fm.league_key = latest.league_key AND fm.is_championship
				) AS in_progress
			FROM latest
			WHERE rn = 1
			ORDER BY manager_name, season_year
		`)
	) as any[];

	const byManager = new Map<string, SeasonLuck[]>();
	for (const r of rows) {
		const name = String(r.managerName);
		const list = byManager.get(name) ?? [];
		list.push({
			seasonYear: Number(r.seasonYear),
			weekNumber: Number(r.weekNumber),
			wins: Number(r.wins ?? 0),
			ties: Number(r.ties ?? 0),
			pythagoreanWins: num(r.pythagoreanWins),
			luckFactor: num(r.luckFactor),
			playoffOdds: num(r.playoffOdds),
			inProgress: Boolean(r.inProgress)
		});
		byManager.set(name, list);
	}
	return byManager;
}

/** Career and current-season expected wins and luck for one manager. */
function summarizeLuck(seasons: SeasonLuck[]) {
	const scored = seasons.filter((s) => s.pythagoreanWins != null && s.luckFactor != null);
	const current = seasons.find((s) => s.inProgress) ?? null;
	const sum = (f: (s: SeasonLuck) => number) =>
		scored.length ? scored.reduce((acc, s) => acc + f(s), 0) : null;
	return {
		careerRegularSeasonWins: sum((s) => s.wins + 0.5 * s.ties),
		careerPythagoreanWins: sum((s) => s.pythagoreanWins as number),
		careerLuckFactor: sum((s) => s.luckFactor as number),
		luckSeasons: scored.length,
		currentSeason: current
			? {
					seasonYear: current.seasonYear,
					throughWeek: current.weekNumber,
					regularSeasonWins: current.wins + 0.5 * current.ties,
					pythagoreanWins: current.pythagoreanWins,
					luckFactor: current.luckFactor,
					playoffOdds: current.playoffOdds
				}
			: null
	};
}

export const GET: RequestHandler = async ({ url }) => {
	try {
		const manager = url.searchParams.get('manager');
		const analysis = url.searchParams.get('analysis') || 'overview';

		let data: any = {};

		// Every manager, always: the category ranks below compare a manager with
		// the rest of the league, and filtering to the requested manager first
		// left a field of one, so every profile read "#1" in every category.
		// The requested manager is picked out after ranking (exact match on the
		// whole dim_manager.manager_name, never ILIKE: `Gabe` must not match
		// `Gabe the Younger`). `manager` is still only ever a bound parameter.
		const statsResult = await db.execute(sql`
			SELECT
				manager_name,
				first_season,
				last_season,
				total_seasons,
				total_wins,
				total_losses,
				total_ties,
				career_win_percentage as win_percentage,
				total_points_scored,
				avg_points_per_game,
				avg_points_per_season as avg_points_for,
				championships_won as total_championships,
				championship_appearances,
				playoff_appearances,
				playoff_win_percentage,
				avg_draft_grade,
				best_draft_year,
				worst_draft_year,
				total_transactions,
				avg_transactions_per_season,
				faab_efficiency_rating,
				season_consistency_score,
				best_season_record,
				worst_season_record
			FROM edw.mart_manager_performance
			WHERE manager_name IS NOT NULL
			ORDER BY career_win_percentage DESC, championships_won DESC
		`);
		const managerStats = Array.from(statsResult);
		const luck = await loadLuckBySeason();

		// Add computed rankings and tier classification with camelCase field names
		const enrichedStats: any[] = managerStats.map((mgr: any, index: number) => ({
			// Handle both snake_case and camelCase field names from database
			managerName: mgr.manager_name || mgr.managerName,
			firstSeason: mgr.first_season || mgr.firstSeason,
			lastSeason: mgr.last_season || mgr.lastSeason,
			totalSeasons: num(mgr.total_seasons ?? mgr.totalSeasons),
			totalWins: num(mgr.total_wins ?? mgr.totalWins),
			totalLosses: num(mgr.total_losses ?? mgr.totalLosses),
			totalTies: num(mgr.total_ties ?? mgr.totalTies),
			winPercentage: num(mgr.win_percentage ?? mgr.winPercentage),
			totalPointsScored: num(mgr.total_points_scored ?? mgr.totalPointsScored),
			avgPointsPerGame: num(mgr.avg_points_per_game ?? mgr.avgPointsPerGame),
			// avg_points_per_season, despite the name. Kept for compatibility with
			// existing consumers; per-game display should use avgPointsPerGame.
			avgPointsFor: num(mgr.avg_points_for ?? mgr.avgPointsFor),
			totalChampionships: num(mgr.total_championships ?? mgr.totalChampionships),
			championshipAppearances: num(mgr.championship_appearances ?? mgr.championshipAppearances),
			playoffAppearances: num(mgr.playoff_appearances ?? mgr.playoffAppearances),
			playoffWinPercentage: num(mgr.playoff_win_percentage ?? mgr.playoffWinPercentage),
			avgDraftGrade: num(mgr.avg_draft_grade ?? mgr.avgDraftGrade),
			bestDraftYear: num(mgr.best_draft_year ?? mgr.bestDraftYear),
			worstDraftYear: num(mgr.worst_draft_year ?? mgr.worstDraftYear),
			totalTransactions: num(mgr.total_transactions ?? mgr.totalTransactions),
			avgTransactionsPerSeason: num(mgr.avg_transactions_per_season ?? mgr.avgTransactionsPerSeason),
			// Starter points per FAAB dollar; null for a manager who never paid for a claim.
			faabEfficiencyRating: num(mgr.faab_efficiency_rating ?? mgr.faabEfficiencyRating),
			...summarizeLuck(luck.get(mgr.manager_name || mgr.managerName) ?? []),
			seasonConsistencyScore: num(mgr.season_consistency_score ?? mgr.seasonConsistencyScore),
			bestSeasonRecord: mgr.best_season_record || mgr.bestSeasonRecord,
			worstSeasonRecord: mgr.worst_season_record || mgr.worstSeasonRecord,
			// Tier classification based on performance
			tierClassification: (mgr.manager_name || mgr.managerName) === 'Bobby' ? 'League Alum' : (() => {
				const championships = parseInt(mgr.total_championships || mgr.totalChampionships) || 0;
				const winPct = parseFloat(mgr.win_percentage || mgr.winPercentage) || 0;
				
				if (championships >= 3) return 'League Legend';
				if (championships >= 2) return 'Dynasty Builder';
				if (championships >= 1 && winPct >= 0.60) return 'Championship Elite';
				if (championships >= 1) return 'Champion';
				if (winPct >= 0.65) return 'Elite Competitor';
				if (winPct >= 0.55) return 'Playoff Contender';
				if (winPct >= 0.45) return 'League Regular';
				return 'Rebuilding';
			})()
		}));

		// Competition ranking: a manager's rank is one more than the number of
		// managers strictly ahead, so equal values share a rank (three managers
		// with 3 titles are all #2) instead of being split by sort order.
		const rankBy = (value: (m: any) => number) => (mgr: any) =>
			1 + enrichedStats.filter((m) => (value(m) || 0) > (value(mgr) || 0)).length;
		const championshipRank = rankBy((m) => Number(m.totalChampionships));
		const winPctRank = rankBy((m) => Number(m.winPercentage));
		// Scoring rank is a rate, not a volume: avgPointsFor is points *per season*,
		// so ranking on it rewarded whoever played the most weeks rather than
		// whoever scored the most per game.
		const scoringRank = rankBy((m) => Number(m.avgPointsPerGame));
		const playoffRank = rankBy((m) => Number(m.playoffWinPercentage));

		// Add rankings to enriched stats. Overall is the query's order: career
		// win percentage, then championships.
		enrichedStats.forEach((mgr, index) => {
			mgr.overallRank = index + 1;
			mgr.championshipRank = championshipRank(mgr);
			mgr.winPctRank = winPctRank(mgr);
			mgr.scoringRank = scoringRank(mgr);
			mgr.playoffRank = playoffRank(mgr);
		});

		// Ranked against the whole league above; now narrow to the one asked for.
		const selected = manager
			? enrichedStats.filter((m) => m.managerName === manager)
			: enrichedStats;
		data.performance = selected;
		data.rankings = selected;

		// Get achievements based on computed stats with camelCase field names
		const achievements = selected.map((mgr: any) => ({
			managerName: mgr.managerName,
			championshipAchievement: (mgr.totalChampionships || 0) >= 3 ? 'Dynasty Builder' : 
										(mgr.totalChampionships || 0) >= 2 ? 'Repeat Champion' : 
										(mgr.totalChampionships || 0) >= 1 ? 'Champion' : null,
			consistencyAchievement: (mgr.winPercentage || 0) >= 0.70 ? 'Dominance' : 
									(mgr.winPercentage || 0) >= 0.60 ? 'Consistent Winner' : 
									(mgr.winPercentage || 0) >= 0.50 ? 'Above Average' : null,
			// Thresholds of 130/120 are per-game numbers. They were being compared
			// against avgPointsFor (points per *season*, ~1800), so every manager
			// in league history qualified as an Offensive Powerhouse.
			scoringAchievement: (mgr.avgPointsPerGame || 0) >= 130 ? 'Offensive Powerhouse' :
								(mgr.avgPointsPerGame || 0) >= 120 ? 'High Scorer' : null,
			longevityAchievement: (mgr.totalSeasons || 0) >= 15 ? 'League Veteran' : 
								  (mgr.totalSeasons || 0) >= 10 ? 'Long Timer' : 
								  (mgr.totalSeasons || 0) >= 5 ? 'Established' : 'Newcomer',
			allAchievements: [
				(mgr.totalChampionships || 0) >= 1 ? 'Champion' : null,
				(mgr.winPercentage || 0) >= 0.60 ? 'Consistent Winner' : null,
				(mgr.avgPointsPerGame || 0) >= 120 ? 'High Scorer' : null,
				(mgr.totalSeasons || 0) >= 10 ? 'Long Timer' : 
				(mgr.totalSeasons || 0) >= 5 ? 'Established' : 'Newcomer'
			].filter(a => a !== null)
		}));

		data.achievements = achievements;

		// Get detailed season-by-season breakdown. fact_team_performance is per-week,
		// so aggregate to one row per season (the old query returned per-week rows).
		if (analysis === 'seasons' || analysis === 'all') {
			try {
				const seasonsResult = await db.execute(sql`
					SELECT
						tp.season_year                              AS season_year,
						SUM(tp.wins)                                AS wins,
						SUM(tp.losses)                              AS losses,
						SUM(tp.ties)                                AS ties,
						CASE WHEN SUM(tp.wins + tp.losses + tp.ties) > 0
							THEN (SUM(tp.wins) + 0.5 * SUM(tp.ties)) / SUM(tp.wins + tp.losses + tp.ties)
							ELSE 0 END                              AS win_percentage,
						SUM(tp.points_for)                          AS points_for,
						SUM(tp.points_against)                      AS points_against,
						bool_or(tp.is_playoff_team)                 AS made_playoffs
					FROM edw.fact_team_performance tp
					JOIN edw.dim_manager dm ON tp.manager_key = dm.manager_key
					WHERE dm.manager_name = ${manager}
					GROUP BY tp.season_year
					ORDER BY tp.season_year DESC
				`);
				// Expected wins, luck and (for a season still being played) playoff
				// odds, from the power rankings' last ranked week of each season, so
				// this page and the power rankings never disagree.
				const bySeason = new Map(
					(luck.get(manager ?? '') ?? []).map((l) => [l.seasonYear, l])
				);
				data.seasons = Array.from(seasonsResult).map((row: any) => {
					const l = bySeason.get(Number(row.seasonYear));
					return {
						...row,
						inProgress: l?.inProgress ?? false,
						regularSeasonWins: l ? l.wins + 0.5 * l.ties : null,
						pythagoreanWins: l?.pythagoreanWins ?? null,
						luckFactor: l?.luckFactor ?? null,
						playoffOdds: l?.inProgress ? l.playoffOdds : null,
						throughWeek: l?.weekNumber ?? null
					};
				});
			} catch (seasonsError) {
				console.error('[managers/performance] seasons query failed:', seasonsError);
				data.seasons = [];
			}

			// Seasons this manager won the championship (for per-season outcome + history).
			try {
				const champResult = await db.execute(sql`
					SELECT DISTINCT fm.season_year AS season_year
					FROM edw.fact_matchup fm
					JOIN edw.dim_manager dm ON fm.winner_manager_key = dm.manager_key
					WHERE fm.is_championship = true AND dm.manager_name = ${manager}
					ORDER BY fm.season_year DESC
				`);
				data.championshipSeasons = Array.from(champResult).map((r: any) => Number(r.seasonYear));
			} catch {
				data.championshipSeasons = [];
			}

			// Longest win/loss streaks from the manager's chronological game results
			// (one matchup per week; ordered by season then week_number).
			try {
				const gamesResult = await db.execute(sql`
					SELECT
						CASE WHEN fm.winner_manager_key = dm.manager_key THEN 'W'
							 WHEN fm.winner_manager_key IS NULL THEN 'T'
							 ELSE 'L' END AS result
					FROM edw.fact_matchup fm
					JOIN edw.dim_week dw ON fm.week_key = dw.week_key
					JOIN edw.dim_manager dm ON dm.manager_name = ${manager}
					WHERE fm.manager1_key = dm.manager_key OR fm.manager2_key = dm.manager_key
					ORDER BY fm.season_year, dw.week_number
				`);
				let longestWin = 0, longestLoss = 0, curWin = 0, curLoss = 0;
				for (const g of Array.from(gamesResult) as any[]) {
					if (g.result === 'W') { curWin++; curLoss = 0; if (curWin > longestWin) longestWin = curWin; }
					else if (g.result === 'L') { curLoss++; curWin = 0; if (curLoss > longestLoss) longestLoss = curLoss; }
					else { curWin = 0; curLoss = 0; }
				}
				data.streaks = {
					longestWinStreak: longestWin,
					longestLossStreak: longestLoss,
					totalGames: gamesResult.length
				};
			} catch {
				data.streaks = null;
			}
		}

		// Get manager head-to-head dominance (if table exists)
		if (analysis === 'h2h' || analysis === 'all') {
			try {
				// Parameterized for the same reason as the stats query above, and
				// exact-matched so `Gabe` stops pulling in `Gabe the Younger`'s rows.
				// `subject` is bound once and reused; the CASE arms orient each row so
				// the requested manager is always the `manager` side.
				const subject = manager ?? '';
				const h2hResult = await db.execute(sql`
					SELECT
						CASE
							WHEN manager_a_name = ${subject} THEN manager_a_name
							WHEN manager_b_name = ${subject} THEN manager_b_name
							ELSE manager_a_name
						END AS manager,
						CASE
							WHEN manager_a_name = ${subject} THEN manager_b_name
							WHEN manager_b_name = ${subject} THEN manager_a_name
							ELSE manager_b_name
						END AS opponent,
						total_matchups,
						CASE
							WHEN manager_a_name = ${subject} THEN manager_a_wins
							WHEN manager_b_name = ${subject} THEN manager_b_wins
							ELSE manager_a_wins
						END AS wins,
						CASE
							WHEN manager_a_name = ${subject} THEN manager_a_losses
							WHEN manager_b_name = ${subject} THEN manager_b_losses
							ELSE manager_a_losses
						END AS losses,
						CASE
							WHEN manager_a_name = ${subject} THEN manager_a_win_percentage
							WHEN manager_b_name = ${subject} THEN manager_b_win_percentage
							ELSE manager_a_win_percentage
						END AS win_percentage
					FROM edw.mart_manager_h2h
					WHERE total_matchups >= 5
					${manager ? sql`AND (manager_a_name = ${manager} OR manager_b_name = ${manager})` : sql``}
					ORDER BY win_percentage DESC, total_matchups DESC
				`);
				data.head_to_head = Array.from(h2hResult).map((row: any) => ({
					...row,
					totalMatchups: num(row.totalMatchups ?? row.total_matchups),
					wins: num(row.wins),
					losses: num(row.losses),
					winPercentage: num(row.winPercentage ?? row.win_percentage)
				}));
			} catch (h2hError) {
				console.error('[managers/performance] head-to-head query failed:', h2hError);
				data.head_to_head = [];
			}
		}

		// Available managers list - check both snake_case and camelCase field names
		data.availableManagers = managerStats
			.filter((mgr: any) => mgr.manager_name != null || mgr.managerName != null)
			.map((mgr: any) => mgr.manager_name || mgr.managerName);

		// Meta information
		data.meta = {
			totalManagers: data.availableManagers?.length || 0,
			requestedManager: manager,
			analysisLevel: analysis
		};

		return json({
			data,
			meta: data.meta
		});

	} catch (error) {
		console.error('Manager performance API error:', error);
		return json(
			{ error: 'Failed to fetch manager performance data', details: error instanceof Error ? error.message : 'Unknown error' },
			{ status: 500 }
		);
	}
}; 