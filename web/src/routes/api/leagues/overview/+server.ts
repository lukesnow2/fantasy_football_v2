import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { dimLeague, dimManager, factTransaction, factMatchup, factTeamPerformance } from '$lib/server/db/schema';
import { count, countDistinct, max, min, eq } from 'drizzle-orm';
import type { RequestHandler } from './$types';
import { sql } from 'drizzle-orm';

export const GET: RequestHandler = async () => {
	try {
		// Get basic league counts
		const leagueStats = await db
			.select({
				totalLeagues: countDistinct(dimLeague.leagueId),
				totalSeasons: countDistinct(dimLeague.seasonYear),
				firstSeason: min(dimLeague.seasonYear),
				lastSeason: max(dimLeague.seasonYear)
			})
			.from(dimLeague)
			.where(eq(dimLeague.isActive, true));

		// Get manager counts
		const managerStats = await db
			.select({
				totalManagers: count(dimManager.managerKey)
			})
			.from(dimManager)
			.where(eq(dimManager.isActive, true));

		// Get transaction counts
		const transactionStats = await db
			.select({
				totalTransactions: count(factTransaction.transactionKey)
			})
			.from(factTransaction);

		// Get matchup counts
		const matchupStats = await db
			.select({
				totalMatchups: count(factMatchup.matchupKey)
			})
			.from(factMatchup);

		const championshipStats = await db
			.select({
				championshipGames: count(factMatchup.matchupKey)
			})
			.from(factMatchup)
			.where(eq(factMatchup.isChampionship, true));

		// Get team performance counts (represents teams across all seasons)
		const teamStats = await db
			.select({
				totalTeamSeasons: count(factTeamPerformance.performanceKey)
			})
			.from(factTeamPerformance);

		// Query for all-time leader (Hall of Fame #1)
		const hallOfFameLeaderResult = await db.execute(sql.raw(`
			SELECT manager_name
			FROM vw_manager_hall_of_fame
			WHERE hall_of_fame_rank = 1
			ORDER BY hall_of_fame_rank ASC
			LIMIT 1
		`));
		const allTimeLeader = Array.from(hallOfFameLeaderResult)[0]?.manager_name || 'TBD';

		// Query for biggest rivalry (most games played between two managers)
		const biggestRivalryResult = await db.execute(sql.raw(`
			SELECT manager_a_name, manager_b_name, total_matchups
			FROM edw.mart_manager_h2h
			ORDER BY total_matchups DESC
			LIMIT 1
		`));
		const rivalryRow = Array.from(biggestRivalryResult)[0];
		const biggestRivalry = rivalryRow ? `${rivalryRow.manager_a_name} vs ${rivalryRow.manager_b_name}` : 'Analyzing...';

		const overview = {
			totalSeasons: leagueStats[0]?.totalSeasons || 0,
			totalLeagues: leagueStats[0]?.totalLeagues || 0,
			firstSeason: leagueStats[0]?.firstSeason?.toString() || 'Unknown',
			lastSeason: leagueStats[0]?.lastSeason?.toString() || 'Unknown',
			totalManagers: managerStats[0]?.totalManagers || 0,
			totalTeams: teamStats[0]?.totalTeamSeasons || 0,
			totalTransactions: transactionStats[0]?.totalTransactions || 0,
			totalDraftPicks: 0, // Would need fact_draft table
			totalMatchups: matchupStats[0]?.totalMatchups || 0,
			championshipGames: championshipStats[0]?.championshipGames || 0,
			dataPoints: (teamStats[0]?.totalTeamSeasons || 0) + 
						(transactionStats[0]?.totalTransactions || 0) + 
						(matchupStats[0]?.totalMatchups || 0),
			allTimeLeader,
			biggestRivalry
		};

		return json({
			overview,
			lastUpdated: new Date().toISOString()
		});

	} catch (error) {
		console.error('Error fetching league overview:', error);
		return json({ error: 'Failed to fetch league overview' }, { status: 500 });
	}
}; 