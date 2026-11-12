import { describe, expect, it } from 'vitest';
import { LEAGUE_TIME_ZONE, isTimeZone, resolveTimeZone } from './time-zone';

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
