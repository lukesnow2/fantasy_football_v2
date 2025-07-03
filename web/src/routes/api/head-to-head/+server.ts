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

		console.log('Executing H2H query:', query);
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

		const h2hArray = Array.from(h2hData);
		console.log(`Returning ${h2hArray.length} head-to-head matchups and analytics:`, Object.keys(analytics));

		return json({
			head_to_head: h2hArray,
			analytics,
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