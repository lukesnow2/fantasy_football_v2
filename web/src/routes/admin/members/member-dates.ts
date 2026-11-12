/**
 * An allowlist date as the Commissioner board writes it, "Nov 9, 2026", or
 * null when there's no date to show (never invited, never signed in, or a
 * value that doesn't parse). The page shows null as "Not yet", never a dash
 * glyph (build spec G6).
 *
 * Always in the league's time zone, Mountain time, so the server (UTC) and
 * the browser write the same day for one timestamp.
 */
export const LEAGUE_TIME_ZONE = 'America/Denver';

export function formatMemberDate(value: string | Date | null | undefined): string | null {
	if (value == null || value === '') return null;
	const date = value instanceof Date ? value : new Date(value);
	if (Number.isNaN(date.getTime())) return null;
	return date.toLocaleDateString('en-US', {
		month: 'short',
		day: 'numeric',
		year: 'numeric',
		timeZone: LEAGUE_TIME_ZONE
	});
}
