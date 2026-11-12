import { loadEnv } from 'vite';
import { describe, expect, it } from 'vitest';
import { startHistory } from '$lib/start-history';

/**
 * Against the dev database (WP4 S4.2): 3-0 after week 3 gives 32 teams, 30 of
 * them made the playoffs, since 2005. Skipped where no database is configured, as on
 * CI; the counting itself is covered in start-history.test.ts.
 */
const url =
	process.env.DATABASE_URL || loadEnv('development', process.cwd(), '').DATABASE_URL || '';
const local = /@(localhost|127\.0\.0\.1)[:/]/.test(url);
// Only ever the local dev database: a remote URL in .env skips the test.
if (local) process.env.DATABASE_URL = url;

describe.skipIf(!local)('getStartHistoryRows against the dev database', () => {
	it('gives the board’s 3-0 tile after week 3', async () => {
		const { getStartHistoryRows } = await import('./start-history');
		const rows = await getStartHistoryRows(3);
		expect(startHistory(rows, { wins: 3, losses: 0, ties: 0 })).toMatchObject({
			count: 32,
			madePlayoffs: 30,
			firstSeason: 2005
		});
	});
});
