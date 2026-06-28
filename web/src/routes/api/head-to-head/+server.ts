import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { sql } from 'drizzle-orm';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async ({ url }) => {
	try {
		const manager = url.searchParams.get('manager');
		const minGames = parseInt(url.searchParams.get('minGames') || '5');
		const analysis = url.searchParams.get('analysis') || 'overview';

		// Base head-to-head query
		let query = `
			SELECT 
				h2h_key,
				manager_a_name,
				manager_b_name,
				total_matchups,
				first_matchup_date,
				last_matchup_date,
				seasons_played_together,
				leagues_played_together,
				manager_a_wins,
				manager_a_losses,
				manager_a_ties,
				manager_a_win_percentage,
				manager_a_total_points,
				manager_a_avg_points,
				manager_a_highest_score,
				manager_a_lowest_score,
				manager_a_pythagorean_wins,
				manager_a_luck_factor,
				manager_a_biggest_win_margin,
				manager_a_current_streak,
				manager_b_wins,
				manager_b_losses,
				manager_b_ties,
				manager_b_win_percentage,
				manager_b_total_points,
				manager_b_avg_points,
				manager_b_highest_score,
				manager_b_lowest_score,
				manager_b_pythagorean_wins,
				manager_b_luck_factor,
				manager_b_biggest_win_margin,
				manager_b_current_streak,
				series_leader,
				series_record,
				point_differential,
				avg_point_differential,
				most_lopsided_game,
				closest_game,
				total_points_in_series,
				avg_total_points_per_game,
				high_scoring_games,
				low_scoring_games,
				playoff_matchups,
				championship_matchups,
				semifinal_matchups,
				manager_a_playoff_wins,
				manager_a_championship_wins,
				manager_a_semifinal_wins,
				manager_b_playoff_wins,
				manager_b_championship_wins,
				manager_b_semifinal_wins,
				most_important_game_type,
				most_important_game_date,
				most_important_game_winner,
				most_important_game_score,
				most_important_game_margin,
				most_important_game_season,
				most_important_game_week,
				most_important_game_league
			FROM edw.mart_manager_h2h
			WHERE total_matchups >= ${minGames}
		`;

		// Add manager filter if specified
		if (manager) {
			query += ` AND (manager_a_name ILIKE '%${manager}%' OR manager_b_name ILIKE '%${manager}%')`;
		}

		query += ` ORDER BY total_matchups DESC`;

		const h2hData = await db.execute(sql.raw(query));

		// Generate analytics based on request type
		let analytics: any = {};

		if (analysis === 'overview' || analysis === 'all') {
			// Overview analytics
			const overviewQuery = `
				SELECT 
					COUNT(*) as total_rivalries,
					AVG(total_matchups) as avg_matchups_per_rivalry,
					MAX(total_matchups) as most_matchups,
					MIN(total_matchups) as least_matchups,
					AVG(seasons_played_together) as avg_seasons_together,
					MAX(seasons_played_together) as longest_rivalry_seasons,
					COUNT(*) FILTER (WHERE playoff_matchups > 0) as playoff_rivalries,
					COUNT(*) FILTER (WHERE championship_matchups > 0) as championship_rivalries,
					AVG(avg_total_points_per_game) as league_avg_scoring,
					MAX(most_lopsided_game) as biggest_blowout,
					MIN(closest_game) as closest_game_ever
				FROM edw.mart_manager_h2h
				WHERE total_matchups >= ${minGames}
			`;

			const overviewResult = await db.execute(sql.raw(overviewQuery));
			analytics.overview = Array.from(overviewResult)[0];
		}

		if (analysis === 'rivalries' || analysis === 'all') {
			// Top rivalries analytics
			const rivalriesQuery = `
				SELECT 
					manager_a_name,
					manager_b_name,
					total_matchups,
					seasons_played_together,
					series_leader,
					series_record,
					avg_point_differential,
					playoff_matchups,
					championship_matchups,
					most_important_game_type,
					most_important_game_season,
					CASE 
						WHEN total_matchups >= 30 THEN 'Epic Rivalry'
						WHEN total_matchups >= 20 THEN 'Major Rivalry'
						WHEN total_matchups >= 15 THEN 'Strong Rivalry'
						WHEN total_matchups >= 10 THEN 'Developing Rivalry'
						ELSE 'Occasional Matchup'
					END as rivalry_tier
				FROM edw.mart_manager_h2h
				WHERE total_matchups >= ${minGames}
				ORDER BY total_matchups DESC
				LIMIT 10
			`;

			const rivalriesResult = await db.execute(sql.raw(rivalriesQuery));
			analytics.top_rivalries = Array.from(rivalriesResult);
		}

		if (analysis === 'records' || analysis === 'all') {
			// Record analytics
			const recordsQuery = `
				WITH records AS (
					SELECT 
						manager_a_name as manager,
						manager_a_highest_score as highest_score,
						manager_a_biggest_win_margin as biggest_win,
						manager_a_current_streak as current_streak
					FROM edw.mart_manager_h2h
					UNION ALL
					SELECT 
						manager_b_name as manager,
						manager_b_highest_score as highest_score,
						manager_b_biggest_win_margin as biggest_win,
						manager_b_current_streak as current_streak
					FROM edw.mart_manager_h2h
				)
				SELECT 
					(SELECT manager FROM records WHERE highest_score = (SELECT MAX(highest_score) FROM records) LIMIT 1) as highest_score_manager,
					(SELECT MAX(highest_score) FROM records) as highest_score_value,
					(SELECT manager FROM records WHERE biggest_win = (SELECT MAX(biggest_win) FROM records) LIMIT 1) as biggest_win_manager,
					(SELECT MAX(biggest_win) FROM records) as biggest_win_value,
					(SELECT manager FROM records WHERE current_streak = (SELECT MAX(current_streak) FROM records) LIMIT 1) as longest_streak_manager,
					(SELECT MAX(current_streak) FROM records) as longest_streak_value,
					(SELECT manager FROM records WHERE current_streak = (SELECT MIN(current_streak) FROM records) LIMIT 1) as worst_streak_manager,
					(SELECT MIN(current_streak) FROM records) as worst_streak_value
			`;

			const recordsResult = await db.execute(sql.raw(recordsQuery));
			analytics.records = Array.from(recordsResult)[0];
		}

		if (analysis === 'playoff_impact' || analysis === 'all') {
			// Playoff impact analytics
			const playoffQuery = `
				SELECT 
					manager_a_name,
					manager_b_name,
					playoff_matchups,
					championship_matchups,
					semifinal_matchups,
					manager_a_playoff_wins,
					manager_a_championship_wins,
					manager_b_playoff_wins,
					manager_b_championship_wins,
					most_important_game_type,
					most_important_game_winner,
					most_important_game_season
				FROM edw.mart_manager_h2h
				WHERE playoff_matchups > 0 OR championship_matchups > 0
				ORDER BY championship_matchups DESC, playoff_matchups DESC
			`;

			const playoffResult = await db.execute(sql.raw(playoffQuery));
			analytics.playoff_impact = Array.from(playoffResult);
		}

		// Convert snake_case to camelCase for frontend compatibility
		const h2hArray = Array.from(h2hData).map((h2h: any) => ({
			h2hKey: h2h.h2hKey || h2h.h2h_key,
			managerAName: h2h.managerAName || h2h.manager_a_name,
			managerBName: h2h.managerBName || h2h.manager_b_name,
			totalMatchups: h2h.totalMatchups || h2h.total_matchups,
			firstMatchupDate: h2h.firstMatchupDate || h2h.first_matchup_date,
			lastMatchupDate: h2h.lastMatchupDate || h2h.last_matchup_date,
			seasonsPlayedTogether: h2h.seasonsPlayedTogether || h2h.seasons_played_together,
			leaguesPlayedTogether: h2h.leaguesPlayedTogether || h2h.leagues_played_together,
			managerAWins: h2h.managerAWins || h2h.manager_a_wins,
			managerALosses: h2h.managerALosses || h2h.manager_a_losses,
			managerATies: h2h.managerATies || h2h.manager_a_ties,
			managerAWinPercentage: h2h.managerAWinPercentage || h2h.manager_a_win_percentage,
			managerATotalPoints: h2h.managerATotalPoints || h2h.manager_a_total_points,
			managerAAvgPoints: h2h.managerAAvgPoints || h2h.manager_a_avg_points,
			managerAHighestScore: h2h.managerAHighestScore || h2h.manager_a_highest_score,
			managerALowestScore: h2h.managerALowestScore || h2h.manager_a_lowest_score,
			managerAPythagoreanWins: h2h.managerAPythagoreanWins || h2h.manager_a_pythagorean_wins,
			managerALuckFactor: h2h.managerALuckFactor || h2h.manager_a_luck_factor,
			managerABiggestWinMargin: h2h.managerABiggestWinMargin || h2h.manager_a_biggest_win_margin,
			managerACurrentStreak: h2h.managerACurrentStreak || h2h.manager_a_current_streak,
			managerBWins: h2h.managerBWins || h2h.manager_b_wins,
			managerBLosses: h2h.managerBLosses || h2h.manager_b_losses,
			managerBTies: h2h.managerBTies || h2h.manager_b_ties,
			managerBWinPercentage: h2h.managerBWinPercentage || h2h.manager_b_win_percentage,
			managerBTotalPoints: h2h.managerBTotalPoints || h2h.manager_b_total_points,
			managerBAvgPoints: h2h.managerBAvgPoints || h2h.manager_b_avg_points,
			managerBHighestScore: h2h.managerBHighestScore || h2h.manager_b_highest_score,
			managerBLowestScore: h2h.managerBLowestScore || h2h.manager_b_lowest_score,
			managerBPythagoreanWins: h2h.managerBPythagoreanWins || h2h.manager_b_pythagorean_wins,
			managerBLuckFactor: h2h.managerBLuckFactor || h2h.manager_b_luck_factor,
			managerBBiggestWinMargin: h2h.managerBBiggestWinMargin || h2h.manager_b_biggest_win_margin,
			managerBCurrentStreak: h2h.managerBCurrentStreak || h2h.manager_b_current_streak,
			seriesLeader: h2h.seriesLeader || h2h.series_leader,
			seriesRecord: h2h.seriesRecord || h2h.series_record,
			pointDifferential: h2h.pointDifferential || h2h.point_differential,
			avgPointDifferential: h2h.avgPointDifferential || h2h.avg_point_differential,
			mostLopsidedGame: h2h.mostLopsidedGame || h2h.most_lopsided_game,
			closestGame: h2h.closestGame || h2h.closest_game,
			totalPointsInSeries: h2h.totalPointsInSeries || h2h.total_points_in_series,
			avgTotalPointsPerGame: h2h.avgTotalPointsPerGame || h2h.avg_total_points_per_game,
			highScoringGames: h2h.highScoringGames || h2h.high_scoring_games,
			lowScoringGames: h2h.lowScoringGames || h2h.low_scoring_games,
			playoffMatchups: h2h.playoffMatchups || h2h.playoff_matchups,
			championshipMatchups: h2h.championshipMatchups || h2h.championship_matchups,
			semifinalMatchups: h2h.semifinalMatchups || h2h.semifinal_matchups,
			managerAPlayoffWins: h2h.managerAPlayoffWins || h2h.manager_a_playoff_wins,
			managerAChampionshipWins: h2h.managerAChampionshipWins || h2h.manager_a_championship_wins,
			managerASemifinalWins: h2h.managerASemifinalWins || h2h.manager_a_semifinal_wins,
			managerBPlayoffWins: h2h.managerBPlayoffWins || h2h.manager_b_playoff_wins,
			managerBChampionshipWins: h2h.managerBChampionshipWins || h2h.manager_b_championship_wins,
			managerBSemifinalWins: h2h.managerBSemifinalWins || h2h.manager_b_semifinal_wins,
			mostImportantGameType: h2h.mostImportantGameType || h2h.most_important_game_type,
			mostImportantGameDate: h2h.mostImportantGameDate || h2h.most_important_game_date,
			mostImportantGameWinner: h2h.mostImportantGameWinner || h2h.most_important_game_winner,
			mostImportantGameScore: h2h.mostImportantGameScore || h2h.most_important_game_score,
			mostImportantGameMargin: h2h.mostImportantGameMargin || h2h.most_important_game_margin,
			mostImportantGameSeason: h2h.mostImportantGameSeason || h2h.most_important_game_season,
			mostImportantGameWeek: h2h.mostImportantGameWeek || h2h.most_important_game_week,
			mostImportantGameLeague: h2h.mostImportantGameLeague || h2h.most_important_game_league
		}));

		// Convert analytics to camelCase
		const camelCaseAnalytics: any = {};
		if (analytics.overview) {
			camelCaseAnalytics.overview = {
				totalRivalries: analytics.overview.totalRivalries || analytics.overview.total_rivalries,
				avgMatchupsPerRivalry: analytics.overview.avgMatchupsPerRivalry || analytics.overview.avg_matchups_per_rivalry,
				mostMatchups: analytics.overview.mostMatchups || analytics.overview.most_matchups,
				leastMatchups: analytics.overview.leastMatchups || analytics.overview.least_matchups,
				avgSeasonsTogether: analytics.overview.avgSeasonsTogether || analytics.overview.avg_seasons_together,
				longestRivalrySeasons: analytics.overview.longestRivalrySeasons || analytics.overview.longest_rivalry_seasons,
				playoffRivalries: analytics.overview.playoffRivalries || analytics.overview.playoff_rivalries,
				championshipRivalries: analytics.overview.championshipRivalries || analytics.overview.championship_rivalries,
				leagueAvgScoring: analytics.overview.leagueAvgScoring || analytics.overview.league_avg_scoring,
				biggestBlowout: analytics.overview.biggestBlowout || analytics.overview.biggest_blowout,
				closestGameEver: analytics.overview.closestGameEver || analytics.overview.closest_game_ever
			};
		}

		if (analytics.top_rivalries) {
			camelCaseAnalytics.topRivalries = analytics.top_rivalries.map((rivalry: any) => ({
				managerAName: rivalry.managerAName || rivalry.manager_a_name,
				managerBName: rivalry.managerBName || rivalry.manager_b_name,
				totalMatchups: rivalry.totalMatchups || rivalry.total_matchups,
				seasonsPlayedTogether: rivalry.seasonsPlayedTogether || rivalry.seasons_played_together,
				seriesLeader: rivalry.seriesLeader || rivalry.series_leader,
				seriesRecord: rivalry.seriesRecord || rivalry.series_record,
				avgPointDifferential: rivalry.avgPointDifferential || rivalry.avg_point_differential,
				playoffMatchups: rivalry.playoffMatchups || rivalry.playoff_matchups,
				championshipMatchups: rivalry.championshipMatchups || rivalry.championship_matchups,
				mostImportantGameType: rivalry.mostImportantGameType || rivalry.most_important_game_type,
				mostImportantGameSeason: rivalry.mostImportantGameSeason || rivalry.most_important_game_season,
				rivalryTier: rivalry.rivalryTier || rivalry.rivalry_tier
			}));
		}

		if (analytics.records) {
			camelCaseAnalytics.records = {
				highestScoreManager: analytics.records.highestScoreManager || analytics.records.highest_score_manager,
				highestScoreValue: analytics.records.highestScoreValue || analytics.records.highest_score_value,
				biggestWinManager: analytics.records.biggestWinManager || analytics.records.biggest_win_manager,
				biggestWinValue: analytics.records.biggestWinValue || analytics.records.biggest_win_value,
				longestStreakManager: analytics.records.longestStreakManager || analytics.records.longest_streak_manager,
				longestStreakValue: analytics.records.longestStreakValue || analytics.records.longest_streak_value,
				worstStreakManager: analytics.records.worstStreakManager || analytics.records.worst_streak_manager,
				worstStreakValue: analytics.records.worstStreakValue || analytics.records.worst_streak_value
			};
		}

		if (analytics.playoff_impact) {
			camelCaseAnalytics.playoffImpact = analytics.playoff_impact.map((impact: any) => ({
				managerAName: impact.managerAName || impact.manager_a_name,
				managerBName: impact.managerBName || impact.manager_b_name,
				playoffMatchups: impact.playoffMatchups || impact.playoff_matchups,
				championshipMatchups: impact.championshipMatchups || impact.championship_matchups,
				semifinalMatchups: impact.semifinalMatchups || impact.semifinal_matchups,
				managerAPlayoffWins: impact.managerAPlayoffWins || impact.manager_a_playoff_wins,
				managerAChampionshipWins: impact.managerAChampionshipWins || impact.manager_a_championship_wins,
				managerBPlayoffWins: impact.managerBPlayoffWins || impact.manager_b_playoff_wins,
				managerBChampionshipWins: impact.managerBChampionshipWins || impact.manager_b_championship_wins,
				mostImportantGameType: impact.mostImportantGameType || impact.most_important_game_type,
				mostImportantGameWinner: impact.mostImportantGameWinner || impact.most_important_game_winner,
				mostImportantGameSeason: impact.mostImportantGameSeason || impact.most_important_game_season
			}));
		}


		return json({
			headToHead: h2hArray,
			analytics: camelCaseAnalytics,
			meta: {
				manager,
				minGames,
				analysis,
				total_returned: h2hArray.length
			}
		});

	} catch (error) {
		console.error('Error fetching head-to-head analysis:', error);
		return json({ 
			error: 'Failed to fetch head-to-head analysis data',
			details: error instanceof Error ? error.message : String(error)
		}, { status: 500 });
	}
}; 