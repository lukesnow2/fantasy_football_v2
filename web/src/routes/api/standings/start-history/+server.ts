import { json } from '@sveltejs/kit';
import { getStartHistoryRows } from '$lib/server/start-history';
import type { RequestHandler } from './$types';

/**
 * How every team-season in a finished season stood after week N, for This
 * Season's history tile and notes (WP4 S4.2). `week` is required, 1 to 20.
 */
export const GET: RequestHandler = async ({ url }) => {
	const week = Number(url.searchParams.get('week'));
	if (!Number.isInteger(week) || week < 1 || week > 20) {
		return json({ error: 'week must be a whole number from 1 to 20' }, { status: 400 });
	}
	try {
		const rows = await getStartHistoryRows(week);
		const seasons = rows.map((r) => r.season);
		return json({
			week,
			firstSeason: seasons.length ? Math.min(...seasons) : null,
			lastSeason: seasons.length ? Math.max(...seasons) : null,
			rows
		});
	} catch (error) {
		console.error('Start history API error:', error);
		return json({ error: 'Failed to load start history' }, { status: 500 });
	}
};
