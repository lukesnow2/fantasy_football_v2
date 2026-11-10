import { json } from '@sveltejs/kit';
import { getPointsLeaderRows } from '$lib/server/points-leaders';
import type { RequestHandler } from './$types';

/**
 * Every team-season's regular-season points for in the finished seasons, for
 * This Season's history tile (how the points leaders have done).
 */
export const GET: RequestHandler = async () => {
	try {
		return json({ rows: await getPointsLeaderRows() });
	} catch (error) {
		console.error('Points leaders API error:', error);
		return json({ error: 'Failed to load points leaders' }, { status: 500 });
	}
};
