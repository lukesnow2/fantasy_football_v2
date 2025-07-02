import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { martManagerPerformance } from '$lib/server/db/schema';
import { desc } from 'drizzle-orm';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async () => {
	try {
		// Get manager career statistics from the mart table
		const managerStats = await db
			.select()
			.from(martManagerPerformance)
			.orderBy(desc(martManagerPerformance.totalChampionships), desc(martManagerPerformance.winPercentage));

		// Format data for frontend
		const formattedStats = managerStats.map(manager => ({
			name: manager.managerName,
			totalSeasons: manager.totalSeasons || 0,
			totalWins: manager.totalWins || 0,
			totalLosses: manager.totalLosses || 0,
			totalTies: manager.totalTies || 0,
			winRate: manager.winPercentage ? parseFloat(manager.winPercentage.toString()) : 0,
			avgPointsFor: manager.avgPointsFor ? parseFloat(manager.avgPointsFor.toString()) : 0,
			avgPointsAgainst: manager.avgPointsAgainst ? parseFloat(manager.avgPointsAgainst.toString()) : 0,
			championships: manager.totalChampionships || 0,
			totalTransactions: manager.totalTransactions || 0,
			yearsActive: manager.totalSeasons || 0,
			firstSeason: manager.firstSeason,
			lastSeason: manager.lastSeason,
			playoffAppearances: manager.totalPlayoffAppearances || 0,
			longestWinStreak: manager.longestWinStreak || 0,
			longestLossStreak: manager.longestLossStreak || 0
		}));

		return json({
			managers: formattedStats,
			totalManagers: formattedStats.length,
			lastUpdated: new Date().toISOString()
		});

	} catch (error) {
		console.error('Error fetching manager historical stats:', error);
		return json({ error: 'Failed to fetch manager statistics' }, { status: 500 });
	}
}; 