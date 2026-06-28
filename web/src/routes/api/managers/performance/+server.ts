import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { sql } from 'drizzle-orm';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async ({ url }) => {
	try {
		const manager = url.searchParams.get('manager');
		const analysis = url.searchParams.get('analysis') || 'overview';

		let data: any = {};

		// Use the actual mart_manager_performance table with correct column names
		const managerStatsQuery = `
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
			${manager ? `AND manager_name ILIKE '%${manager}%'` : ''}
			ORDER BY career_win_percentage DESC, championships_won DESC
		`;

		const statsResult = await db.execute(sql.raw(managerStatsQuery));
		const managerStats = Array.from(statsResult);

		// Add computed rankings and tier classification with camelCase field names
		const enrichedStats: any[] = managerStats.map((mgr: any, index: number) => ({
			// Handle both snake_case and camelCase field names from database
			managerName: mgr.manager_name || mgr.managerName,
			firstSeason: mgr.first_season || mgr.firstSeason,
			lastSeason: mgr.last_season || mgr.lastSeason,
			totalSeasons: mgr.total_seasons || mgr.totalSeasons,
			totalWins: mgr.total_wins || mgr.totalWins,
			totalLosses: mgr.total_losses || mgr.totalLosses,
			totalTies: mgr.total_ties || mgr.totalTies,
			winPercentage: mgr.win_percentage || mgr.winPercentage,
			totalPointsScored: mgr.total_points_scored || mgr.totalPointsScored,
			avgPointsPerGame: mgr.avg_points_per_game || mgr.avgPointsPerGame,
			avgPointsFor: mgr.avg_points_for || mgr.avgPointsFor,
			totalChampionships: mgr.total_championships || mgr.totalChampionships,
			championshipAppearances: mgr.championship_appearances || mgr.championshipAppearances,
			playoffAppearances: mgr.playoff_appearances || mgr.playoffAppearances,
			playoffWinPercentage: mgr.playoff_win_percentage || mgr.playoffWinPercentage,
			avgDraftGrade: mgr.avg_draft_grade || mgr.avgDraftGrade,
			bestDraftYear: mgr.best_draft_year || mgr.bestDraftYear,
			worstDraftYear: mgr.worst_draft_year || mgr.worstDraftYear,
			totalTransactions: mgr.total_transactions || mgr.totalTransactions,
			avgTransactionsPerSeason: mgr.avg_transactions_per_season || mgr.avgTransactionsPerSeason,
			faabEfficiencyRating: mgr.faab_efficiency_rating || mgr.faabEfficiencyRating,
			seasonConsistencyScore: mgr.season_consistency_score || mgr.seasonConsistencyScore,
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
		const sortedByScoring = [...enrichedStats].sort((a, b) => (Number(b.avgPointsFor) || 0) - (Number(a.avgPointsFor) || 0));
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
			scoringAchievement: (mgr.avgPointsFor || 0) >= 130 ? 'Offensive Powerhouse' : 
								(mgr.avgPointsFor || 0) >= 120 ? 'High Scorer' : null,
			longevityAchievement: (mgr.totalSeasons || 0) >= 15 ? 'League Veteran' : 
								  (mgr.totalSeasons || 0) >= 10 ? 'Long Timer' : 
								  (mgr.totalSeasons || 0) >= 5 ? 'Established' : 'Newcomer',
			allAchievements: [
				(mgr.totalChampionships || 0) >= 1 ? 'Champion' : null,
				(mgr.winPercentage || 0) >= 0.60 ? 'Consistent Winner' : null,
				(mgr.avgPointsFor || 0) >= 120 ? 'High Scorer' : null,
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
		}

		// Get manager head-to-head dominance (if table exists)
		if (analysis === 'h2h' || analysis === 'all') {
			try {
				const h2hQuery = `
					SELECT 
						CASE 
							WHEN manager_a_name ILIKE '%${manager || ''}%' THEN manager_a_name
							WHEN manager_b_name ILIKE '%${manager || ''}%' THEN manager_b_name
							ELSE manager_a_name
						END as manager,
						CASE 
							WHEN manager_a_name ILIKE '%${manager || ''}%' THEN manager_b_name
							WHEN manager_b_name ILIKE '%${manager || ''}%' THEN manager_a_name
							ELSE manager_b_name
						END as opponent,
						total_matchups,
						CASE 
							WHEN manager_a_name ILIKE '%${manager || ''}%' THEN manager_a_wins
							WHEN manager_b_name ILIKE '%${manager || ''}%' THEN manager_b_wins
							ELSE manager_a_wins
						END as wins,
						CASE 
							WHEN manager_a_name ILIKE '%${manager || ''}%' THEN manager_a_losses
							WHEN manager_b_name ILIKE '%${manager || ''}%' THEN manager_b_losses
							ELSE manager_a_losses
						END as losses,
						CASE 
							WHEN manager_a_name ILIKE '%${manager || ''}%' THEN manager_a_win_percentage
							WHEN manager_b_name ILIKE '%${manager || ''}%' THEN manager_b_win_percentage
							ELSE manager_a_win_percentage
						END as win_percentage
					FROM edw.mart_manager_h2h
					WHERE total_matchups >= 5
					${manager ? `AND (manager_a_name ILIKE '%${manager}%' OR manager_b_name ILIKE '%${manager}%')` : ''}
					ORDER BY win_percentage DESC, total_matchups DESC
				`;

				const h2hResult = await db.execute(sql.raw(h2hQuery));
				data.head_to_head = Array.from(h2hResult);
			} catch (h2hError) {
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