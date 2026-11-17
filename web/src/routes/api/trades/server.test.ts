import { beforeEach, describe, expect, it, vi } from 'vitest';
import { CHAMPION_GAIN, SWUNG_A_TITLE } from '$lib/server/trade-sql';
import { paramsOf, textOf } from '$lib/server/testing/sql-text';

/**
 * "Swung a title" counts trades where that season's champion gained 50 or
 * more points. production_differential is side A minus side B, so a bare
 * `production_differential >= 50` missed every title side B won, and the
 * absolute value would count swings away from the champion. The database is
 * a stub that records the SQL each query sends, so the test runs without a
 * warehouse.
 */

/** What each query sent: its SQL text and its bound parameters. */
const sent: string[] = [];
const params: unknown[][] = [];

vi.mock('$lib/server/db', () => ({
	db: {
		execute: vi.fn(async (query: unknown) => {
			sent.push(textOf(query));
			params.push(paramsOf(query));
			return [];
		})
	}
}));

const squeeze = (q: string) => q.replace(/\s+/g, ' ');

/** How many times the sent SQL tests for a title trade, and any other 50-point test left over. */
function titleTests(): { shared: number; strays: string[] } {
	let shared = 0;
	const strays: string[] = [];
	for (const q of sent.map(squeeze)) {
		const parts = q.split(SWUNG_A_TITLE);
		shared += parts.length - 1;
		for (const part of parts)
			strays.push(...(part.match(/production_differential\)? ?[<>]=? ?-?50/g) ?? []));
	}
	return { shared, strays };
}

beforeEach(() => {
	sent.length = 0;
	params.length = 0;
});

describe('Swung a title', () => {
	it("is a 50-point gain for the champion's side, A or B", () => {
		expect(CHAMPION_GAIN).toContain('WHEN team_a_champion = 1 THEN production_differential');
		expect(CHAMPION_GAIN).toContain('WHEN team_b_champion = 1 THEN -production_differential');
		expect(SWUNG_A_TITLE).toBe(`(${CHAMPION_GAIN}) >= 50`);
	});

	it('counts and lists title trades by it on the Trades API', async () => {
		const { GET } = await import('./+server');
		await GET({ url: new URL('http://x/api/trades?analysis=all') } as never);
		// The overview count, the trends count and the title-trades list.
		expect(titleTests()).toEqual({ shared: 3, strays: [] });
	});

	it("lists the champion's biggest gains first", async () => {
		const { GET } = await import('./+server');
		await GET({ url: new URL('http://x/api/trades?analysis=best_worst') } as never);
		const list = squeeze(sent.find((q) => q.includes("'championship' as category")) ?? '');
		expect(list).toContain(`ORDER BY ${CHAMPION_GAIN} DESC`);
	});

	it('counts the same way on the Overview API', async () => {
		const { GET } = await import('../overview/+server');
		await GET({ url: new URL('http://x/api/overview?metric=trades') } as never);
		expect(titleTests()).toEqual({ shared: 1, strays: [] });
	});
});

describe('Trades API filters', () => {
	it('binds the manager filter as a parameter, never as SQL text', async () => {
		const { GET } = await import('./+server');
		const evil = "x') OR 1=1 --";
		await GET({
			url: new URL(`http://x/api/trades?analysis=none&manager=${encodeURIComponent(evil)}`)
		} as never);
		const list = sent.findIndex((q) => q.includes('ILIKE'));
		expect(list).toBeGreaterThanOrEqual(0);
		expect(sent[list]).not.toContain(evil);
		expect(squeeze(sent[list])).toContain('team_a_manager ILIKE ? OR team_b_manager ILIKE ?');
		expect(params[list]).toEqual([`%${evil}%`, `%${evil}%`, 1000]);
	});

	it('reads a season that is not a year as all seasons', async () => {
		const { GET } = await import('./+server');
		await GET({ url: new URL('http://x/api/trades?analysis=all&season=abc') } as never);
		for (const q of sent) expect(q).not.toMatch(/season_year = (NaN|abc)/);
	});
});
