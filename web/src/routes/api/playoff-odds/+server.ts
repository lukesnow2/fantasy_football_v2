import { json } from '@sveltejs/kit';
import { getSeasonOdds, latestSeason } from '$lib/server/playoff-odds-service';
import type { RequestHandler } from './$types';

/** Simulated playoff odds for one season (default: the latest). */
export const GET: RequestHandler = async ({ url }) => {
	try {
		const latest = await latestSeason();
		const seasonParam = url.searchParams.get('season');
		let season = latest;
		if (seasonParam !== null) {
			const parsed = Number(seasonParam);
			if (!Number.isInteger(parsed) || parsed < 2008 || parsed > latest) {
				return json({ error: 'Invalid season parameter' }, { status: 400 });
			}
			season = parsed;
		}
		const odds = await getSeasonOdds(season);
		if (!odds.available) return json(odds);
		// The games fingerprint is for the saved snapshots, not the page.
		const { gamesFingerprint, ...body } = odds;
		void gamesFingerprint;
		return json(body);
	} catch (error) {
		console.error('Playoff odds API error:', error);
		return json({ error: 'Failed to compute playoff odds' }, { status: 500 });
	}
};
