import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { dimLeague, dimManager, factTransaction, factMatchup, factTeamPerformance } from '$lib/server/db/schema';
import { count, countDistinct, max, min, eq } from 'drizzle-orm';
import type { RequestHandler } from './$types';

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
						(matchupStats[0]?.totalMatchups || 0)
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