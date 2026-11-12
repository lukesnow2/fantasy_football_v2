/**
 * Pure rules behind the WP1 parts, kept out of the components so they can be
 * tested on their own (build spec G9).
 */

export type MovementDirection = 'up' | 'down' | 'same';

/** "▲ 4", "▼ 2" or "Same", from a signed rank change. */
export function movement(change: number): { direction: MovementDirection; text: string } {
	if (change > 0) return { direction: 'up', text: String(change) };
	if (change < 0) return { direction: 'down', text: String(-change) };
	return { direction: 'same', text: 'Same' };
}

/** A percentage held to 0 to 100, so a bar never overflows its track. */
export function clampPercent(value: number): number {
	if (!Number.isFinite(value)) return 0;
	return Math.min(100, Math.max(0, value));
}

/** The odds bar's number: a whole percentage, as the boards print it. */
export function formatPercent(value: number): string {
	return `${Math.round(clampPercent(value))}%`;
}

/**
 * The letter on a photo-less avatar: the first character of the name,
 * capitalized. Taken by code point, so an emoji isn't split in half.
 */
export function avatarInitial(name: string | null | undefined): string {
	const [first = ''] = (name ?? '').trim();
	return first.toUpperCase();
}

/**
 * Where focus goes in a segmented control after a key press, or null when the
 * key isn't one the control handles. Arrows wrap around, Home and End jump to
 * the ends, as a radio group does.
 */
export function segmentedTarget(key: string, index: number, count: number): number | null {
	if (count <= 0) return null;
	switch (key) {
		case 'ArrowRight':
		case 'ArrowDown':
			return (index + 1) % count;
		case 'ArrowLeft':
		case 'ArrowUp':
			return (index - 1 + count) % count;
		case 'Home':
			return 0;
		case 'End':
			return count - 1;
		default:
			return null;
	}
}

/**
 * Where a popover card goes so it stays inside the viewport: under its trigger
 * when it fits there, above it when it doesn't, and slid sideways so neither
 * edge leaves the screen.
 *
 * A board can draw the card somewhere else, and these follow it:
 * - `endOffset`: the card's right edge sits this many px past the trigger's
 *   right edge, rather than its left edge on the trigger's (Power Rankings'
 *   Luck header, `right: -40px`)
 * - `topOffset`: the card's top sits this many px below the trigger's top,
 *   rather than `gap` under its bottom (Power Rankings, `top: 26px`)
 * - `span`: the card takes this box's left edge and width (Power Rankings'
 *   phone table, `left: 0; right: 0`), and the result carries the width
 */
export function placeCard(
	trigger: { top: number; bottom: number; left: number; right: number },
	card: { width: number; height: number },
	viewport: { width: number; height: number },
	{
		gap = 8,
		margin = 16,
		endOffset,
		topOffset,
		span
	}: {
		gap?: number;
		margin?: number;
		endOffset?: number;
		topOffset?: number;
		span?: { left: number; width: number };
	} = {}
): { top: number; left: number; width?: number } {
	const width = span ? Math.min(span.width, viewport.width - 2 * margin) : card.width;
	const below = topOffset != null ? trigger.top + topOffset : trigger.bottom + gap;
	const above = trigger.top - gap - card.height;
	const fitsBelow = below + card.height <= viewport.height - margin;
	const top = fitsBelow || above < margin ? below : above;

	// Line the card up as drawn, then pull it back in.
	const wanted = span
		? span.left
		: endOffset != null
			? trigger.right + endOffset - width
			: trigger.left;
	const maxLeft = viewport.width - margin - width;
	const left = Math.max(margin, Math.min(wanted, maxLeft));
	return span ? { top, left, width } : { top, left };
}
