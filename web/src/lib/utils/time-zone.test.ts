import { describe, expect, it } from 'vitest';
import {
	LEAGUE_TIME_ZONE,
	TIME_ZONE_COOKIE,
	formatShortDate,
	isTimeZone,
	requestTimeZone,
	resolveTimeZone
} from './time-zone';

describe('isTimeZone', () => {
	it('knows real zones', () => {
		expect(isTimeZone('America/Los_Angeles')).toBe(true);
		expect(isTimeZone('America/Denver')).toBe(true);
	});

	it('refuses empty, made-up and overlong values', () => {
		expect(isTimeZone(null)).toBe(false);
		expect(isTimeZone('')).toBe(false);
		expect(isTimeZone('Mars/Olympus_Mons')).toBe(false);
		expect(isTimeZone(`America/${'x'.repeat(80)}`)).toBe(false);
	});
});

describe('resolveTimeZone', () => {
	it("prefers the browser's saved zone", () => {
		expect(resolveTimeZone('America/Los_Angeles', 'America/Denver')).toBe('America/Los_Angeles');
	});

	it("falls back to Vercel's guess on a first visit", () => {
		expect(resolveTimeZone(undefined, 'America/Los_Angeles')).toBe('America/Los_Angeles');
	});

	it('skips a bad cookie for the header', () => {
		expect(resolveTimeZone('not/a zone', 'America/Los_Angeles')).toBe('America/Los_Angeles');
	});

	it('ends at Mountain time with nothing to go on', () => {
		expect(resolveTimeZone(undefined, null)).toBe(LEAGUE_TIME_ZONE);
		expect(resolveTimeZone('nope', 'also nope')).toBe(LEAGUE_TIME_ZONE);
	});
});

describe('requestTimeZone', () => {
	const event = (cookie: string | undefined, header: string | null) => ({
		cookies: { get: (name: string) => (name === TIME_ZONE_COOKIE ? cookie : undefined) },
		request: new Request('http://x', header ? { headers: { 'x-vercel-ip-timezone': header } } : {})
	});

	it("reads the tz cookie, then Vercel's header", () => {
		expect(requestTimeZone(event('America/Los_Angeles', 'America/Denver'))).toBe(
			'America/Los_Angeles'
		);
		expect(requestTimeZone(event(undefined, 'America/Los_Angeles'))).toBe('America/Los_Angeles');
		expect(requestTimeZone(event(undefined, null))).toBe(LEAGUE_TIME_ZONE);
	});
});

describe('formatShortDate', () => {
	it('writes a date as the boards do', () => {
		expect(formatShortDate(new Date('2026-11-09T18:00:00Z'))).toBe('Nov 9, 2026');
		expect(formatShortDate('2026-11-09T18:00:00Z')).toBe('Nov 9, 2026');
	});

	it('writes the Mountain time day by default, whatever zone it runs in', () => {
		// 7pm Mountain on Nov 9 is already Nov 10 in UTC.
		expect(formatShortDate('2026-11-10T02:00:00Z')).toBe('Nov 9, 2026');
		expect(formatShortDate('2026-11-10T07:30:00Z')).toBe('Nov 10, 2026');
	});

	it("writes the reader's day when given their zone", () => {
		// 11:30pm Pacific on Nov 9 is already Nov 10 in Mountain time.
		expect(formatShortDate('2026-11-10T07:30:00Z', 'America/Los_Angeles')).toBe('Nov 9, 2026');
		expect(formatShortDate('2026-11-10T07:30:00Z', 'America/Denver')).toBe('Nov 10, 2026');
	});

	it('has nothing to show for no date or one that does not parse, and never a dash', () => {
		expect(formatShortDate(null)).toBeNull();
		expect(formatShortDate(undefined)).toBeNull();
		expect(formatShortDate('')).toBeNull();
		expect(formatShortDate('not a date')).toBeNull();
		expect(formatShortDate(new Date(Number.NaN))).toBeNull();
	});
});
