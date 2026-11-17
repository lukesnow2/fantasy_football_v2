import { json } from '@sveltejs/kit';
import { getSeasonOdds, latestSeason } from '$lib/server/playoff-odds-service';
import type { RequestHandler } from './$types';

/**
 * Simulated playoff odds for one season (default: the latest), as of a week
 * when `week` is given (a playoff week counts the bracket games through it).
 */
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
		const weekParam = url.searchParams.get('week');
		let week: number | undefined;
		if (weekParam !== null) {
			const parsed = Number(weekParam);
			if (!Number.isInteger(parsed) || parsed < 1 || parsed > 30) {
				return json({ error: 'Invalid week parameter' }, { status: 400 });
			}
			week = parsed;
		}
		return json(await getSeasonOdds(season, week));
	} catch (error) {
		console.error('Playoff odds API error:', error);
		return json({ error: 'Failed to compute playoff odds' }, { status: 500 });
	}
};
