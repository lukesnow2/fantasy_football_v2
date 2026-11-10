import { loadEnv } from 'vite';
import { describe, expect, it } from 'vitest';
import { pointsLeaderHistory } from '$lib/points-leaders';

/**
 * Against the dev database: 21 finished seasons since 2005, every points
 * leader made the playoffs and eight won the title. Skipped where no database
 * is configured, as on CI; the counting itself is covered in
 * points-leaders.test.ts.
 */
const url =
	process.env.DATABASE_URL || loadEnv('development', process.cwd(), '').DATABASE_URL || '';
const local = /@(localhost|127\.0\.0\.1)[:/]/.test(url);
// Only ever the local dev database: a remote URL in .env skips the test.
if (local) process.env.DATABASE_URL = url;

describe.skipIf(!local)('getPointsLeaderRows against the dev database', () => {
	it('gives 8 of 21, all 21 in the playoffs, since 2005', async () => {
		const { getPointsLeaderRows } = await import('./points-leaders');
		expect(pointsLeaderHistory(await getPointsLeaderRows())).toEqual({
			seasons: 21,
			madePlayoffs: 21,
			wonTitle: 8,
			firstSeason: 2005
		});
	});
});
