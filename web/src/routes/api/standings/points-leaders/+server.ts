import { json } from '@sveltejs/kit';
import { getPointsLeaderRows } from '$lib/server/points-leaders';
import type { RequestHandler } from './$types';

/**
 * Every team-season's regular-season points for in the finished seasons, for
 * This Season's history tile (how the points leaders have done).
 */
export const GET: RequestHandler = async ({ setHeaders }) => {
	try {
		const rows = await getPointsLeaderRows();
		// The rows only change when a season ends, so a browser can keep them an hour.
		setHeaders({ 'cache-control': 'private, max-age=3600' });
		return json({ rows });
	} catch (error) {
		console.error('Points leaders API error:', error);
		return json({ error: 'Failed to load points leaders' }, { status: 500 });
	}
};
