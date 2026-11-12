import { describe, expect, it } from 'vitest';
import { formatMemberDate } from './member-dates';

describe('formatMemberDate', () => {
	it('writes a date as the board does', () => {
		expect(formatMemberDate(new Date('2026-11-09T18:00:00Z'))).toBe('Nov 9, 2026');
		expect(formatMemberDate('2026-11-09T18:00:00Z')).toBe('Nov 9, 2026');
	});

	it('writes the Mountain time day, whatever zone it runs in', () => {
		// 7pm Mountain on Nov 9 is already Nov 10 in UTC.
		expect(formatMemberDate('2026-11-10T02:00:00Z')).toBe('Nov 9, 2026');
		// Just after midnight Mountain is the new day.
		expect(formatMemberDate('2026-11-10T07:30:00Z')).toBe('Nov 10, 2026');
	});

	it('has nothing to show for no date', () => {
		expect(formatMemberDate(null)).toBeNull();
		expect(formatMemberDate(undefined)).toBeNull();
		expect(formatMemberDate('')).toBeNull();
	});

	it('has nothing to show for a value that does not parse, and never a dash', () => {
		expect(formatMemberDate('not a date')).toBeNull();
		expect(formatMemberDate(new Date(Number.NaN))).toBeNull();
	});
});
