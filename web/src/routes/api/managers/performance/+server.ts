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

export const GET: RequestHandler = async ({ url }) => {
	try {
		const manager = url.searchParams.get('manager');
		const analysis = url.searchParams.get('analysis') || 'overview';

		let data: any = {};

		// Use the actual mart_manager_performance table with correct column names.
		//
		// Parameterized, not `sql.raw` with the query string spliced together: this
		// endpoint is in PUBLIC_PREFIXES, so `manager` arrives unauthenticated from
		// the URL, and it used to be concatenated straight into the WHERE clause.
		// Exact `=` rather than `ILIKE '%…%'` too — the slug is a whole
		// dim_manager.manager_name, and the substring form quietly matched two
		// managers for `/managers/Gabe`.
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
			${manager ? sql`AND manager_name = ${manager}` : sql``}
			ORDER BY career_win_percentage DESC, championships_won DESC
		`);
		const managerStats = Array.from(statsResult);

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
			faabEfficiencyRating: num(mgr.faab_efficiency_rating ?? mgr.faabEfficiencyRating),
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

		// Calculate rankings after creating enriched stats
		const sortedByChampionships = [...enrichedStats].sort((a, b) => (Number(b.totalChampionships) || 0) - (Number(a.totalChampionships) || 0));
		const sortedByWinPct = [...enrichedStats].sort((a, b) => (Number(b.winPercentage) || 0) - (Number(a.winPercentage) || 0));
		// Scoring rank is a rate, not a volume: avgPointsFor is points *per season*,
		// so ranking on it rewarded whoever played the most weeks rather than
		// whoever scored the most per game.
		const sortedByScoring = [...enrichedStats].sort((a, b) => (Number(b.avgPointsPerGame) || 0) - (Number(a.avgPointsPerGame) || 0));
		const sortedByPlayoffs = [...enrichedStats].sort((a, b) => (Number(b.playoffWinPercentage) || 0) - (Number(a.playoffWinPercentage) || 0));

		// Add rankings to enriched stats
		enrichedStats.forEach((mgr, index) => {
			mgr.overallRank = index + 1;
			mgr.championshipRank = sortedByChampionships.findIndex(m => m.managerName === mgr.managerName) + 1;
			mgr.winPctRank = sortedByWinPct.findIndex(m => m.managerName === mgr.managerName) + 1;
			mgr.scoringRank = sortedByScoring.findIndex(m => m.managerName === mgr.managerName) + 1;
			mgr.playoffRank = sortedByPlayoffs.findIndex(m => m.managerName === mgr.managerName) + 1;
		});

		data.performance = enrichedStats;
		data.rankings = enrichedStats;

		// Get achievements based on computed stats with camelCase field names
		const achievements = enrichedStats.map((mgr: any) => ({
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
				data.seasons = Array.from(seasonsResult);
			} catch (seasonsError) {
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