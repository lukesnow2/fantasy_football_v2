import { describe, expect, it } from 'vitest';
import { formatMemberDate } from './member-dates';

describe('formatMemberDate', () => {
	it('writes a date as the board does', () => {
		// Midday, so the viewer's time zone can't move it to another day.
		expect(formatMemberDate(new Date(2026, 10, 9, 12))).toBe('Nov 9, 2026');
		expect(formatMemberDate('2026-11-09T12:00:00')).toBe('Nov 9, 2026');
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
