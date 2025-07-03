import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { dimLeague, dimSeason, factTeamPerformance, vwCurrentSeasonDashboard } from '$lib/server/db/schema';
import { count, countDistinct, max, min } from 'drizzle-orm';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async () => {
	try {
		console.log('=== EDW DEBUG INFO ===');

		// Check what seasons are available
		const seasonInfo = await db
			.select({
				totalSeasons: countDistinct(dimLeague.seasonYear),
				minSeason: min(dimLeague.seasonYear),
				maxSeason: max(dimLeague.seasonYear)
			})
			.from(dimLeague);

		console.log('Season Info:', seasonInfo);

		// Check view data count
		const viewCount = await db
			.select({ count: count() })
			.from(vwCurrentSeasonDashboard);

		console.log('View count:', viewCount);

		// Sample view data
		const viewSample = await db
			.select()
			.from(vwCurrentSeasonDashboard)
			.limit(3);

		console.log('View sample:', viewSample);

		// Check fact table data
		const factCount = await db
			.select({
				totalRecords: count(),
				seasons: countDistinct(factTeamPerformance.seasonYear),
				minSeason: min(factTeamPerformance.seasonYear),
				maxSeason: max(factTeamPerformance.seasonYear)
			})
			.from(factTeamPerformance);

		console.log('Fact table info:', factCount);

		return json({
			debug: {
				seasonInfo: seasonInfo[0],
				viewCount: viewCount[0]?.count || 0,
				viewSample,
				factTableInfo: factCount[0],
				currentYear: new Date().getFullYear(),
				timestamp: new Date().toISOString()
			}
		});

	} catch (error) {
		console.error('Debug error:', error);
		return json({ error: error instanceof Error ? error.message : 'Unknown error' }, { status: 500 });
	}
}; 