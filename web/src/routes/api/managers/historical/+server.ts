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
			.orderBy(desc(martManagerPerformance.championshipsWon), desc(martManagerPerformance.careerWinPercentage));

		// Format data for frontend
		const formattedStats = managerStats.map(manager => ({
			name: manager.managerName,
			totalSeasons: manager.totalSeasons || 0,
			totalWins: manager.totalWins || 0,
			totalLosses: manager.totalLosses || 0,
			totalTies: manager.totalTies || 0,
			winRate: manager.careerWinPercentage ? parseFloat(manager.careerWinPercentage.toString()) : 0,
			avgPointsPerGame: manager.avgPointsPerGame ? parseFloat(manager.avgPointsPerGame.toString()) : 0,
			avgPointsPerSeason: manager.avgPointsPerSeason ? parseFloat(manager.avgPointsPerSeason.toString()) : 0,
			totalPointsScored: manager.totalPointsScored ? parseFloat(manager.totalPointsScored.toString()) : 0,
			championships: manager.championshipsWon || 0,
			championshipAppearances: manager.championshipAppearances || 0,
			totalTransactions: manager.totalTransactions || 0,
			yearsActive: manager.totalSeasons || 0,
			firstSeason: manager.firstSeason,
			lastSeason: manager.lastSeason,
			playoffAppearances: manager.playoffAppearances || 0,
			bestSeasonRecord: manager.bestSeasonRecord,
			worstSeasonRecord: manager.worstSeasonRecord
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