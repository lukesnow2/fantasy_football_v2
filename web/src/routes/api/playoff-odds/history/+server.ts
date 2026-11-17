import { json } from '@sveltejs/kit';
import { FIRST_ODDS_SEASON } from '$lib/odds-history';
import { latestSeason } from '$lib/server/playoff-odds-service';
import { getOddsHistory } from '$lib/server/playoff-odds-history';
import type { RequestHandler } from './$types';

/** Every team's playoff, bye and title odds week by week (default: the latest season). */
export const GET: RequestHandler = async ({ url }) => {
	try {
		const latest = await latestSeason();
		const seasonParam = url.searchParams.get('season');
		let season = latest;
		if (seasonParam !== null) {
			const parsed = Number(seasonParam);
			if (!Number.isInteger(parsed) || parsed < FIRST_ODDS_SEASON || parsed > latest) {
				return json({ error: 'Invalid season parameter' }, { status: 400 });
			}
			season = parsed;
		}
		return json(await getOddsHistory(season));
	} catch (error) {
		console.error('Playoff odds history API error:', error);
		return json({ error: 'Failed to load the odds history' }, { status: 500 });
	}
};
