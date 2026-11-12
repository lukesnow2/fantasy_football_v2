/**
 * The site's navigation: what the bar links to, and the two small rules the
 * shell runs on (which link is the current page, and where arrow keys move
 * focus in a menu). Kept apart from +layout.svelte so the rules have tests
 * (WP2, S2.6).
 */

export interface NavLink {
	name: string;
	href: string;
}

export interface NavItem extends NavLink {
	/** One line on what the page is for. The whole point of grouping is that
	 *  the group name plus this line answers "is what I want in here?". */
	blurb: string;
}

export interface NavGroup {
	label: string;
	items: NavItem[];
}

/**
 * The bar is three direct links plus three subject dropdowns.
 *
 * There is deliberately no "More". A catch-all bucket is where the pages
 * nobody owns go to be forgotten, and the last version put Hall of Fame,
 * Draft Central and the whole archive in it. Every page now sits under a
 * heading that says what it is, one click deep at most.
 */
export const directLinks: NavLink[] = [
	{ name: 'This Season', href: '/this-season' },
	{ name: 'Chat', href: '/chat' },
	// A direct link rather than a one-item dropdown: Trade Center and Draft
	// Central moved out to Activity, where they read as league movement rather
	// than as something belonging to a manager.
	{ name: 'Managers', href: '/managers' }
];

export const navGroups: NavGroup[] = [
	{
		label: 'Rankings',
		items: [
			{ name: 'Power Rankings', href: '/power-rankings', blurb: "Who's actually good right now" },
			{ name: 'Hall of Fame', href: '/hall-of-fame', blurb: 'The all-time leaderboard' },
			{ name: 'League History', href: '/historical', blurb: 'Twenty-two seasons of analysis' }
		]
	},
	{
		label: 'Activity',
		items: [
			{ name: 'Trade Center', href: '/trades', blurb: 'Every trade ever made' },
			{ name: 'Draft Central', href: '/draft', blurb: 'Draft boards and grades' },
			{ name: 'Bet Board', href: '/bets', blurb: 'Side bets, props, and who owes who' }
		]
	},
	{
		label: 'League',
		items: [
			{ name: 'Constitution', href: '/constitution', blurb: 'Rules, proposals and voting' },
			{ name: 'Data Dictionary', href: '/data-dictionary', blurb: 'What every metric means' }
		]
	}
];

/**
 * Whether `href` is the page you're on or a page under it, so
 * `/managers/Trevor` marks Managers. A trailing slash on either side doesn't
 * count, and `/managersx` is not under `/managers`. Home (`/`) matches only
 * itself; otherwise it would mark every page.
 */
export function isActivePath(pathname: string, href: string): boolean {
	const path = trimSlash(pathname);
	const target = trimSlash(href);
	if (target === '/') return path === '/';
	return path === target || path.startsWith(target + '/');
}

function trimSlash(path: string): string {
	return path.length > 1 && path.endsWith('/') ? path.slice(0, -1) : path;
}

/**
 * Where ↓ or ↑ moves focus in a menu of `length` items, from `current` (-1
 * when no item has focus). Both ends wrap. Any other key, or an empty menu,
 * returns null so the caller leaves focus alone.
 */
export function nextMenuIndex(current: number, length: number, key: string): number | null {
	if (length <= 0) return null;
	if (key === 'ArrowDown') return current < 0 ? 0 : (current + 1) % length;
	if (key === 'ArrowUp') return current <= 0 ? length - 1 : current - 1;
	return null;
}
