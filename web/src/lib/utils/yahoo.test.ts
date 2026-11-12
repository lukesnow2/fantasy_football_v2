import { describe, expect, it } from 'vitest';
import { yahooArchiveUrl, yahooLeagueNumber } from './yahoo';

// The fixtures are real rows from edw.dim_league, not invented shapes.
describe('yahooLeagueNumber', () => {
	it('extracts the league number from a Yahoo league key', () => {
		expect(yahooLeagueNumber('461.l.654923')).toBe('654923');
		expect(yahooLeagueNumber('124.l.109785')).toBe('109785');
		expect(yahooLeagueNumber('414.l.1194955')).toBe('1194955');
	});

	it('returns null rather than a partial match for malformed input', () => {
		expect(yahooLeagueNumber('654923')).toBeNull();
		expect(yahooLeagueNumber('461.l.')).toBeNull();
		expect(yahooLeagueNumber('461.t.654923')).toBeNull();
		expect(yahooLeagueNumber('nfl.l.654923')).toBeNull();
		expect(yahooLeagueNumber('')).toBeNull();
		expect(yahooLeagueNumber(null)).toBeNull();
		expect(yahooLeagueNumber(undefined)).toBeNull();
	});
});

describe('yahooArchiveUrl', () => {
	it('builds the archive URL for a finished season', () => {
		expect(yahooArchiveUrl('390.l.777720', 2019)).toBe(
			'https://football.fantasysports.yahoo.com/archive/nfl/2019/777720'
		);
		expect(yahooArchiveUrl('273.l.107980', 2012)).toBe(
			'https://football.fantasysports.yahoo.com/archive/nfl/2012/107980'
		);
	});

	it('returns null when there is nothing safe to link to', () => {
		expect(yahooArchiveUrl(null, 2019)).toBeNull();
		expect(yahooArchiveUrl('not-a-key', 2019)).toBeNull();
		expect(yahooArchiveUrl('390.l.777720', Number.NaN)).toBeNull();
	});
});
