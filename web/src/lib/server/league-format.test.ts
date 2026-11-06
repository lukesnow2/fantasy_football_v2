import { describe, expect, it } from 'vitest';
import { resolveLeagueFormat, tradeDeadlinePassed } from './league-format';

describe('resolveLeagueFormat', () => {
	it('uses the stored Yahoo settings when the season has them', () => {
		const format = resolveLeagueFormat(
			2026,
			{ playoffStartWeek: '16', numPlayoffTeams: '4', tradeEndDate: '2026-11-22' },
			14
		);
		expect(format).toEqual({
			season: 2026,
			regularSeasonWeeks: 15,
			playoffStartWeek: 16,
			playoffTeams: 4,
			tradeEndDate: '2026-11-22',
			source: 'league-settings'
		});
	});

	it('infers until the settings are stored', () => {
		// No row (or a warehouse the pipeline hasn't migrated): every column null.
		const format = resolveLeagueFormat(
			2026,
			{ playoffStartWeek: null, numPlayoffTeams: null, tradeEndDate: null },
			14
		);
		expect(format).toMatchObject({
			regularSeasonWeeks: 14,
			playoffStartWeek: 15,
			playoffTeams: 6,
			tradeEndDate: null,
			source: 'inferred'
		});
		expect(resolveLeagueFormat(2026, undefined, null).regularSeasonWeeks).toBe(14);
	});

	it('only calls the format stored when both playoff fields are', () => {
		const format = resolveLeagueFormat(2026, { playoffStartWeek: '15' }, 13);
		expect(format.regularSeasonWeeks).toBe(14);
		expect(format.playoffTeams).toBe(6);
		expect(format.source).toBe('inferred');
	});

	it('ignores malformed values rather than trusting them', () => {
		const format = resolveLeagueFormat(
			2026,
			{ playoffStartWeek: '0', numPlayoffTeams: 'six', tradeEndDate: 'Nov 22' },
			14
		);
		expect(format).toMatchObject({ regularSeasonWeeks: 14, playoffTeams: 6, tradeEndDate: null });
	});
});

describe('tradeDeadlinePassed', () => {
	const format = resolveLeagueFormat(2026, { tradeEndDate: '2026-11-22' }, 14);

	it('keeps the deadline day itself open', () => {
		expect(tradeDeadlinePassed(format, new Date('2026-11-21T12:00:00Z'))).toBe(false);
		expect(tradeDeadlinePassed(format, new Date('2026-11-22T12:00:00Z'))).toBe(false);
		expect(tradeDeadlinePassed(format, new Date('2026-11-23T12:00:00Z'))).toBe(true);
	});

	it('is still the deadline day on the evening of it in the US', () => {
		// 8pm Pacific on the 22nd is already the 23rd in UTC.
		expect(tradeDeadlinePassed(format, new Date('2026-11-23T04:00:00Z'))).toBe(false);
		// And it has passed by early morning Pacific on the 23rd.
		expect(tradeDeadlinePassed(format, new Date('2026-11-23T09:00:00Z'))).toBe(true);
	});

	it("doesn't claim either way when the deadline isn't known", () => {
		expect(tradeDeadlinePassed(resolveLeagueFormat(2026, undefined, 14))).toBeNull();
	});
});
