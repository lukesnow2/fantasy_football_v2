/**
 * Hall of Fame rules shared by its page (WP7) and the manager profiles (WP6).
 * Every derived value the Hall of Fame board shows (the tier a rank falls in,
 * a bar's width, the podium's sizes) is worked out here, where vitest holds it.
 */

/** A color token name, used as `var(--color-{token})`. */
export type ColorToken = 'pennant-dark' | 'field' | 'ink-2' | 'muted';

export interface HofTier {
	name: string;
	color: ColorToken;
}

/**
 * The five tiers, best first, with the ranks each one covers as the board's
 * Tiers card writes them. The cut lines (3, 6, 10, 15) are the Hall of Fame
 * API's `CASE`.
 */
const TIERS: readonly (HofTier & { last: number; ranks: string })[] = [
	{ name: 'Hall of Fame Elite', color: 'pennant-dark', last: 3, ranks: 'Top 3' },
	{ name: 'Hall of Fame', color: 'field', last: 6, ranks: 'Ranks 4 to 6' },
	{ name: 'Hall of Very Good', color: 'ink-2', last: 10, ranks: 'Ranks 7 to 10' },
	{ name: 'Solid Contributor', color: 'muted', last: 15, ranks: 'Ranks 11 to 15' },
	{ name: 'Developing Legacy', color: 'muted', last: Infinity, ranks: '16 and below' }
];

/**
 * The tier a Hall of Fame rank falls in, and its color: 1 to 3 Elite, 4 to 6
 * Hall of Fame, 7 to 10 Hall of Very Good, 11 to 15 Solid Contributor, then
 * Developing Legacy.
 */
export function hofTier(rank: number): HofTier {
	const tier = TIERS.find((t) => rank <= t.last) ?? TIERS[TIERS.length - 1];
	return { name: tier.name, color: tier.color };
}

/** The Tiers card: always all five rows, best first. */
export function tierKey(): { name: string; ranks: string; color: ColorToken }[] {
	return TIERS.map(({ name, ranks, color }) => ({ name, ranks, color }));
}

/**
 * The HoF index bar's width: the index as a share of the highest index in
 * the list, rounded to a whole percent. A top index of zero or less draws
 * every bar empty.
 */
export function indexBarPct(index: number, topIndex: number): number {
	if (!Number.isFinite(index) || !Number.isFinite(topIndex) || topIndex <= 0) return 0;
	return Math.min(100, Math.max(0, Math.round((index / topIndex) * 100)));
}

const WORDS = [
	'Zero',
	'One',
	'Two',
	'Three',
	'Four',
	'Five',
	'Six',
	'Seven',
	'Eight',
	'Nine',
	'Ten',
	'Eleven',
	'Twelve',
	'Thirteen',
	'Fourteen',
	'Fifteen',
	'Sixteen',
	'Seventeen',
	'Eighteen',
	'Nineteen',
	'Twenty'
];

/**
 * A count that opens a sentence: one to twenty spelled out with a capital
 * ("Twelve"), anything else in digits ("21").
 */
export function numberWord(n: number): string {
	return Number.isInteger(n) && n >= 1 && n <= 20 ? WORDS[n] : String(n);
}

/** One podium card's measurements, from the board's renderVals. */
export interface PodiumStyle {
	/** Color tokens. */
	bg: string;
	fg: string;
	border: string;
	accent: string;
	ring: string;
	muted: string;
	pole: string;
	flag: string;
	/** Px. */
	padY: number;
	rankSize: number;
	avatar: number;
	nameSize: number;
	statSize: number;
}

const FIRST_COLORS = {
	bg: 'clubhouse',
	fg: 'white',
	border: 'field-2',
	accent: 'pennant-light',
	ring: 'pennant-light',
	muted: 'on-clubhouse-2',
	pole: 'white',
	flag: 'pennant-light'
};

const OTHER_COLORS = {
	bg: 'card',
	fg: 'ink',
	border: 'line',
	accent: 'field',
	ring: 'field-tint',
	muted: 'muted',
	pole: 'ink',
	flag: 'pennant'
};

const DESKTOP_SIZES = [
	{ padY: 28, rankSize: 72, avatar: 116, nameSize: 44, statSize: 34 },
	{ padY: 20, rankSize: 52, avatar: 80, nameSize: 32, statSize: 26 },
	{ padY: 16, rankSize: 44, avatar: 64, nameSize: 28, statSize: 24 }
];

const PHONE_SIZES = [
	{ padY: 20, rankSize: 48, avatar: 76, nameSize: 30, statSize: 24 },
	{ padY: 16, rankSize: 36, avatar: 56, nameSize: 24, statSize: 20 },
	{ padY: 14, rankSize: 32, avatar: 48, nameSize: 22, statSize: 18 }
];

/**
 * The podium card for place 1, 2 or 3: first is the dark clubhouse card,
 * second and third white, each a size smaller. `phone` gives the 390 board's
 * sizes; the colors don't change. A place outside 1 to 3 takes third's.
 */
export function podiumStyle(place: number, phone = false): PodiumStyle {
	const i = place === 1 ? 0 : place === 2 ? 1 : 2;
	const sizes = (phone ? PHONE_SIZES : DESKTOP_SIZES)[i];
	return { ...(i === 0 ? FIRST_COLORS : OTHER_COLORS), ...sizes };
}

/**
 * The rankings table's groups: the rows in each tier, best tier first, each
 * group in the order the rows came in. A tier with no managers has no group.
 * A rank tie shares one rank, so tied managers land in the same tier.
 */
export function groupByTier<T extends { hallOfFameRank: number }>(
	rows: T[]
): (HofTier & { rows: T[] })[] {
	const groups = TIERS.map((t) => ({ name: t.name, color: t.color, rows: [] as T[] }));
	for (const row of rows) {
		const name = hofTier(row.hallOfFameRank).name;
		groups.find((g) => g.name === name)!.rows.push(row);
	}
	return groups.filter((g) => g.rows.length > 0);
}

/** A rate as the board writes Win %: three places, no leading zero (".563"). */
export function formatRate(value: number | string | null | undefined): string | null {
	const n = Number(value);
	if (value == null || value === '' || !Number.isFinite(n)) return null;
	return n.toFixed(3).replace(/^(-?)0\./, '$1.');
}

/** A rate as a percent to one place ("57.5%"). */
export function formatPercent1(value: number | string | null | undefined): string | null {
	const n = Number(value);
	if (value == null || value === '' || !Number.isFinite(n)) return null;
	return `${(n * 100).toFixed(1)}%`;
}

/** A number to a fixed count of places ("0.352", "1.8"). */
export function fixed(value: number | string | null | undefined, places: number): string | null {
	const n = Number(value);
	if (value == null || value === '' || !Number.isFinite(n)) return null;
	return n.toFixed(places);
}

/** "W-L", with "-T" when there are ties. */
export function careerRecord(wins: unknown, losses: unknown, ties: unknown): string {
	const t = Number(ties) || 0;
	const base = `${Number(wins) || 0}-${Number(losses) || 0}`;
	return t > 0 ? `${base}-${t}` : base;
}

/** "12 managers", "1 manager". */
export function managerCount(n: number): string {
	return `${n} ${n === 1 ? 'manager' : 'managers'}`;
}
