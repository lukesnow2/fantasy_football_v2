/**
 * An allowlist date as the Commissioner board writes it, "Nov 9, 2026", or
 * null when there's no date to show (never invited, never signed in, or a
 * value that doesn't parse). The page shows null as "Not yet", never a dash
 * glyph (build spec G6).
 */
export function formatMemberDate(value: string | Date | null | undefined): string | null {
	if (value == null || value === '') return null;
	const date = value instanceof Date ? value : new Date(value);
	if (Number.isNaN(date.getTime())) return null;
	return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}
