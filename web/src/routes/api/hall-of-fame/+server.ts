import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { sql } from 'drizzle-orm';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async ({ url }) => {
	try {
		const limit = parseInt(url.searchParams.get('limit') || '50');
		const manager = url.searchParams.get('manager');

		// Main Hall of Fame query - using ALL restored columns
		let query = `
			SELECT 
				manager_name,
				total_seasons,
				championships_won,
				career_wins,
				career_losses,
				career_ties,
				career_win_percentage,
				total_points_scored,
				avg_points_per_game,
				playoff_appearances,
				season_consistency_score,
				hall_of_fame_index,
				hall_of_fame_rank
			FROM vw_manager_hall_of_fame
			WHERE 1=1
		`;

		// Add manager filter if specified
		if (manager) {
			query += ` AND manager_name ILIKE '%${manager}%'`;
		}

		query += ` ORDER BY hall_of_fame_rank ASC LIMIT ${limit}`;

		console.log('Executing Hall of Fame query:', query);
		const hallOfFameResult = await db.execute(sql.raw(query));
		const hallOfFameData = Array.from(hallOfFameResult);

		// Get analytics for the Hall of Fame using restored columns
		const analyticsQuery = `
			SELECT 
				COUNT(*) as total_managers,
				AVG(championships_won) as avg_championships,
				AVG(career_win_percentage) as avg_win_percentage,
				AVG(total_seasons) as avg_seasons_played,
				AVG(total_points_scored) as avg_career_points,
				AVG(avg_points_per_game) as avg_points_per_game,
				AVG(hall_of_fame_index) as avg_hall_of_fame_index,
				SUM(career_wins) as total_career_wins,
				SUM(career_losses) as total_career_losses,
				SUM(career_ties) as total_career_ties,
				MAX(career_win_percentage) as highest_win_percentage,
				MIN(career_win_percentage) as lowest_win_percentage,
				MAX(hall_of_fame_index) as highest_hall_of_fame_index,
				MIN(hall_of_fame_index) as lowest_hall_of_fame_index
			FROM vw_manager_hall_of_fame
		`;

		const analyticsResult = await db.execute(sql.raw(analyticsQuery));
		const analytics = Array.from(analyticsResult)[0];

		// Get tier breakdowns with enhanced metrics
		const tiersQuery = `
			SELECT 
				CASE 
					WHEN hall_of_fame_rank <= 3 THEN 'Hall of Fame Elite'
					WHEN hall_of_fame_rank <= 6 THEN 'Hall of Fame'
					WHEN hall_of_fame_rank <= 10 THEN 'Hall of Very Good'
					WHEN hall_of_fame_rank <= 15 THEN 'Solid Contributor'
					ELSE 'Developing Legacy'
				END as tier,
				COUNT(*) as manager_count,
				AVG(career_win_percentage) as avg_win_pct,
				AVG(championships_won) as avg_championships,
				AVG(total_seasons) as avg_seasons,
				AVG(hall_of_fame_index) as avg_hall_of_fame_index,
				AVG(avg_points_per_game) as avg_points_per_game,
				SUM(career_wins) as total_wins,
				SUM(career_losses) as total_losses
			FROM vw_manager_hall_of_fame
			GROUP BY 
				CASE 
					WHEN hall_of_fame_rank <= 3 THEN 'Hall of Fame Elite'
					WHEN hall_of_fame_rank <= 6 THEN 'Hall of Fame'
					WHEN hall_of_fame_rank <= 10 THEN 'Hall of Very Good'
					WHEN hall_of_fame_rank <= 15 THEN 'Solid Contributor'
					ELSE 'Developing Legacy'
				END
			ORDER BY MIN(hall_of_fame_rank)
		`;

		const tiersResult = await db.execute(sql.raw(tiersQuery));
		const tiers = Array.from(tiersResult);

		// Convert snake_case to camelCase for frontend compatibility
		const hallOfFameArray = hallOfFameData.map((entry: any) => ({
			managerName: entry.managerName || entry.manager_name,
			totalSeasons: entry.totalSeasons || entry.total_seasons,
			championshipsWon: entry.championshipsWon || entry.championships_won,
			careerWins: entry.careerWins || entry.career_wins,
			careerLosses: entry.careerLosses || entry.career_losses,
			careerTies: entry.careerTies || entry.career_ties,
			careerWinPercentage: entry.careerWinPercentage || entry.career_win_percentage,
			totalPointsScored: entry.totalPointsScored || entry.total_points_scored,
			avgPointsPerGame: entry.avgPointsPerGame || entry.avg_points_per_game,
			playoffAppearances: entry.playoffAppearances || entry.playoff_appearances,
			seasonConsistencyScore: entry.seasonConsistencyScore || entry.season_consistency_score,
			hallOfFameIndex: entry.hallOfFameIndex || entry.hall_of_fame_index,
			hallOfFameRank: entry.hallOfFameRank || entry.hall_of_fame_rank
		}));

		// Convert analytics to camelCase
		const camelCaseAnalytics = {
			totalManagers: analytics.totalManagers || analytics.total_managers,
			avgChampionships: analytics.avgChampionships || analytics.avg_championships,
			avgWinPercentage: analytics.avgWinPercentage || analytics.avg_win_percentage,
			avgSeasonsPlayed: analytics.avgSeasonsPlayed || analytics.avg_seasons_played,
			avgCareerPoints: analytics.avgCareerPoints || analytics.avg_career_points,
			avgPointsPerGame: analytics.avgPointsPerGame || analytics.avg_points_per_game,
			avgHallOfFameIndex: analytics.avgHallOfFameIndex || analytics.avg_hall_of_fame_index,
			totalCareerWins: analytics.totalCareerWins || analytics.total_career_wins,
			totalCareerLosses: analytics.totalCareerLosses || analytics.total_career_losses,
			totalCareerTies: analytics.totalCareerTies || analytics.total_career_ties,
			highestWinPercentage: analytics.highestWinPercentage || analytics.highest_win_percentage,
			lowestWinPercentage: analytics.lowestWinPercentage || analytics.lowest_win_percentage,
			highestHallOfFameIndex: analytics.highestHallOfFameIndex || analytics.highest_hall_of_fame_index,
			lowestHallOfFameIndex: analytics.lowestHallOfFameIndex || analytics.lowest_hall_of_fame_index
		};

		// Convert tiers to camelCase
		const camelCaseTiers = tiers.map((tier: any) => ({
			tier: tier.tier,
			managerCount: tier.managerCount || tier.manager_count,
			avgWinPct: tier.avgWinPct || tier.avg_win_pct,
			avgChampionships: tier.avgChampionships || tier.avg_championships,
			avgSeasons: tier.avgSeasons || tier.avg_seasons,
			avgHallOfFameIndex: tier.avgHallOfFameIndex || tier.avg_hall_of_fame_index,
			avgPointsPerGame: tier.avgPointsPerGame || tier.avg_points_per_game,
			totalWins: tier.totalWins || tier.total_wins,
			totalLosses: tier.totalLosses || tier.total_losses
		}));

		console.log(`Returning ${hallOfFameArray.length} Hall of Fame entries with complete column set`);

		return json({
			hallOfFame: hallOfFameArray,
			analytics: camelCaseAnalytics,
			tiers: camelCaseTiers,
			meta: {
				manager,
				limit,
				total_returned: hallOfFameArray.length
			}
		});

	} catch (error) {
		console.error('Error fetching Hall of Fame data:', error);
		return json({ 
			error: 'Failed to fetch Hall of Fame data',
			details: error instanceof Error ? error.message : String(error)
		}, { status: 500 });
	}
}; 