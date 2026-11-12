/**
 * Pure rules behind League History (WP10): the tabs in the URL, the Overview
 * findings and era cards, the head to head face-off and tables, and the
 * record book's labels, values and groups. The analysis stays in the APIs and
 * $lib/league-trends; this file only turns their rows into the boards' words.
 */

import { splitEras } from '$lib/league-trends';

/* ---------- Shared ---------- */

/** A color token's name, used as `var(--color-<name>)`. */
export type Tone = 'ink' | 'field' | 'pennant';

const num = (v: unknown): number => {
	const n = typeof v === 'number' ? v : Number(v);
	return Number.isFinite(n) ? n : 0;
};

/** One decimal place ("23.6"). */
export function oneDecimal(v: unknown): string {
	return num(v).toFixed(1);
}

/** A whole number with thousands separators ("3,018"). */
export function thousands(v: unknown): string {
	return Math.round(num(v)).toLocaleString('en-US');
}

/**
 * A manager's URL handle, the same rule as the server's slugifyManagerName
 * (lib/server/members.ts), which the client can't import: "Gabe the Younger"
 * is "gabe-the-younger".
 */
export function managerSlug(name: string): string {
	return name
		.toLowerCase()
		.normalize('NFKD')
		.replace(/[̀-ͯ]/g, '')
		.replace(/[^a-z0-9]+/g, '-')
		.replace(/^-+|-+$/g, '');
}

/** "1 title game", "2 title games". */
export function plural(n: number, one: string, many = `${one}s`): string {
	return `${n} ${n === 1 ? one : many}`;
}

/* ---------- S10.1 Tabs ---------- */

export type HistoryTab = 'overview' | 'head-to-head' | 'records';

export const HISTORY_TABS: { id: HistoryTab; label: string; href: string }[] = [
	{ id: 'overview', label: 'Overview', href: '/historical' },
	{ id: 'head-to-head', label: 'Head to head', href: '/historical?tab=head-to-head' },
	{ id: 'records', label: 'Record book', href: '/historical?tab=records' }
];

/** The tab `?tab=` names, or overview when it's missing or unknown. */
export function historyTab(params: URLSearchParams): HistoryTab {
	const tab = params.get('tab');
	return HISTORY_TABS.some((t) => t.id === tab) ? (tab as HistoryTab) : 'overview';
}

/**
 * The manager a `?a=` or `?b=` value names: the name itself (the form WP6's
 * profile links send, "Gabe%20the%20Younger") or its slug ("gabe-the-younger").
 * Null when it names nobody in the list, so an unknown value is ignored.
 */
export function resolveManager(value: string | null | undefined, names: string[]): string | null {
	if (!value) return null;
	const exact = names.find((n) => n === value);
	if (exact) return exact;
	const slug = managerSlug(value);
	return names.find((n) => managerSlug(n) === slug) ?? null;
}

/* ---------- S10.1 Header ---------- */

export function historyKicker(first: number | null | undefined, last: number | null | undefined): string {
	return first && last ? `Rankings · ${first} to ${last}` : 'Rankings';
}

export function historyIntro(seasons: number | null | undefined): string {
	return `What ${seasons ?? 'its'} seasons say about scoring and luck in this league, and how the format changed along the way.`;
}

/* ---------- S10.2 The era control and findings ---------- */

/** "Early Era", 2005, 2012 → "Early" and "05 to 12". */
export function eraButton(era: { name: string; start: number; end: number }): { name: string; years: string } {
	const yy = (y: number) => String(y).slice(-2);
	return { name: era.name.replace(/\s*era$/i, ''), years: `${yy(era.start)} to ${yy(era.end)}` };
}

export interface Headline {
	recordPersistence: { r: number | null; n: number };
	skillShare: number | null;
	championTopThree: { count: number; seasons: number };
	topScorerTitles: number;
	draftedShare: number | null;
	seasons: number;
}

export interface Finding {
	value: string;
	tone: Tone;
	head: string;
	sub: string;
}

/** A correlation signed with two decimals ("-0.01", "+0.32"). */
export function signedTwo(v: number): string {
	const text = Math.abs(v).toFixed(2);
	if (text === '0.00') return text;
	return v < 0 ? `-${text}` : `+${text}`;
}

const wholePercent = (v: number | null) => (v == null ? 'None' : `${Math.round(v * 100)}%`);

/** "once", "twice" reads oddly in the board's sentence, so only one is a word. */
const times = (n: number) => (n === 1 ? 'once' : `${n} times`);

/** The four findings cards, from an era's headline or the whole league's. */
export function findings(headline: Headline | null | undefined): Finding[] {
	if (!headline) return [];
	const { recordPersistence: rp } = headline;
	const r = rp.r;
	const abs = r == null ? 0 : Math.abs(r);
	const carry =
		abs < 0.1
			? "Last season's record tells you nothing about this one."
			: abs < 0.3
				? "Last season's record says little about this one."
				: "Last season's record carries over into this one.";
	return [
		{
			value: r == null ? 'None' : signedTwo(r),
			tone: 'ink',
			head: carry,
			sub: `Correlation across ${rp.n} manager-seasons.`
		},
		{
			value: wholePercent(headline.skillShare),
			tone: 'field',
			head: 'of the spread in records is skill.',
			sub: `The rest is luck, using Tango's method over ${headline.seasons} seasons.`
		},
		{
			value: `${headline.championTopThree.count} of ${headline.championTopThree.seasons}`,
			tone: 'pennant',
			head: 'champions were a top-3 scorer that year.',
			sub: `The top scorer won it ${times(headline.topScorerTitles)}.`
		},
		{
			value: wholePercent(headline.draftedShare),
			tone: 'ink',
			head: "of starting points came from a team's own draft picks.",
			sub: 'The rest came off waivers and trades.'
		}
	];
}

/* ---------- S10.2 Three eras ---------- */

export interface EraCard {
	name: string;
	span: string;
	/** "Trevor (4)", or two names joined with "and" for a tie; null with no titles. */
	titles: string | null;
	/** The managers named in `titles`, for links. */
	leaders: string[];
	leaderTitles: number;
	rows: { label: string; value: string }[];
}

/**
 * The league's seasons in three eras (splitEras), each with its real
 * aggregates from the overview rows: seasons, average weekly score, trades,
 * transactions and different champions, plus who won the most titles.
 */
export function computeEras(o: {
	leagueEvolution?: any[];
	tradeActivity?: any[];
	scoringPatterns?: any[];
} | null | undefined): EraCard[] {
	const le = o?.leagueEvolution ?? [];
	if (le.length < 3) return [];

	const tradesByYear = new Map<number, number>();
	(o?.tradeActivity ?? []).forEach((t: any) => tradesByYear.set(Number(t.seasonYear), parseInt(t.totalTrades) || 0));
	const scoreByYear = new Map<number, number>();
	(o?.scoringPatterns ?? []).forEach((s: any) => scoreByYear.set(Number(s.seasonYear), parseFloat(s.avgWeeklyScore) || 0));

	const seasons = le
		.map((x: any) => {
			const year = Number(x.seasonYear);
			return {
				year,
				champion: (x.championManager as string) || null,
				score: scoreByYear.get(year) ?? (parseFloat(x.averageWeeklyScore) || 0),
				trades: tradesByYear.get(year) ?? 0,
				transactions: parseInt(x.totalTransactions) || 0
			};
		})
		.sort((a, b) => a.year - b.year);

	const out: EraCard[] = [];
	for (const era of splitEras(seasons.map((s) => s.year))) {
		const slice = seasons.filter((s) => s.year >= era.start && s.year <= era.end);
		if (!slice.length) continue;
		const titles = new Map<string, number>();
		for (const s of slice) if (s.champion) titles.set(s.champion, (titles.get(s.champion) ?? 0) + 1);
		const most = Math.max(0, ...titles.values());
		const leaders = [...titles.entries()]
			.filter(([, n]) => n === most && most > 0)
			.map(([name]) => name)
			.sort((a, b) => a.localeCompare(b));
		out.push({
			name: `${era.name.replace(/\s*era$/i, '')} era`,
			span: `${slice[0].year} to ${slice[slice.length - 1].year}`,
			titles: leaders.length ? `${joinAnd(leaders)} (${most})` : null,
			leaders,
			leaderTitles: most,
			rows: [
				{ label: 'Seasons', value: String(slice.length) },
				{ label: 'Avg weekly score', value: (slice.reduce((s, x) => s + x.score, 0) / slice.length).toFixed(1) },
				{ label: 'Trades', value: thousands(slice.reduce((s, x) => s + x.trades, 0)) },
				{ label: 'Transactions', value: thousands(slice.reduce((s, x) => s + x.transactions, 0)) },
				{ label: 'Different champions', value: String(titles.size) }
			]
		});
	}
	return out;
}

/** "A", "A and B", "A, B and C". */
export function joinAnd(names: string[]): string {
	if (names.length <= 1) return names[0] ?? '';
	return `${names.slice(0, -1).join(', ')} and ${names[names.length - 1]}`;
}

/* ---------- S10.5 to S10.7 Head to head ---------- */

/** The API's head to head row; numbers arrive as strings. */
export type H2HRow = Record<string, any>;

/** "2005-W1", "2025-W5" → "2005 to 2025". One season reads as its year. */
export function spanYears(first: string | null | undefined, last: string | null | undefined): string {
	const year = (v: string | null | undefined) => String(v ?? '').match(/\d{4}/)?.[0] ?? '';
	const a = year(first);
	const b = year(last);
	if (!a || !b) return a || b;
	return a === b ? a : `${a} to ${b}`;
}

export interface Rivalry {
	a: string;
	b: string;
	aWins: number;
	bWins: number;
	ties: number;
	games: number;
	seasons: number;
	span: string;
	aPythag: number;
	bPythag: number;
	aLuck: number;
	bLuck: number;
	avgTotal: number;
	bestWin: number;
	titleGames: number;
	playoffGames: number;
	keyWinner: string | null;
	keySeason: number | null;
	row: H2HRow;
}

function tiesOf(row: H2HRow): number {
	const parts = String(row.seriesRecord ?? '').split('-');
	return parts.length === 3 ? num(parts[2]) : 0;
}

/** A row as the board reads it, with `a` on the left. */
export function rivalryFromRow(row: H2HRow, flip = false): Rivalry {
	const side = (s: 'A' | 'B') => (flip ? (s === 'A' ? 'B' : 'A') : s);
	const A = side('A');
	const B = side('B');
	return {
		a: row[`manager${A}Name`],
		b: row[`manager${B}Name`],
		aWins: num(row[`manager${A}Wins`]),
		bWins: num(row[`manager${B}Wins`]),
		ties: tiesOf(row),
		games: num(row.totalMatchups),
		seasons: num(row.seasonsPlayedTogether),
		span: spanYears(row.firstMatchupDate, row.lastMatchupDate),
		aPythag: num(row[`manager${A}PythagoreanWins`]),
		bPythag: num(row[`manager${B}PythagoreanWins`]),
		aLuck: num(row[`manager${A}LuckFactor`]),
		bLuck: num(row[`manager${B}LuckFactor`]),
		avgTotal: num(row.avgTotalPointsPerGame),
		bestWin: num(row.mostLopsidedGame),
		titleGames: num(row.championshipMatchups),
		playoffGames: num(row.playoffMatchups),
		keyWinner: row.mostImportantGameWinner || null,
		keySeason: row.mostImportantGameSeason ? num(row.mostImportantGameSeason) : null,
		row
	};
}

/**
 * The pair x and y in the rows, oriented so x is on the left. Rows are
 * unordered pairs, so a pair stored as (y, x) is flipped. Null when the pair
 * has no row (fewer than the API's minimum games).
 */
export function findRivalry(rows: H2HRow[], x: string, y: string): Rivalry | null {
	for (const row of rows) {
		if (row.managerAName === x && row.managerBName === y) return rivalryFromRow(row);
		if (row.managerAName === y && row.managerBName === x) return rivalryFromRow(row, true);
	}
	return null;
}

/** Every manager in the rivalry rows, A to Z (as extractUniqueManagers built it). */
export function rivalryManagers(rows: H2HRow[]): string[] {
	const names = new Set<string>();
	for (const r of rows) {
		if (r.managerAName) names.add(r.managerAName);
		if (r.managerBName) names.add(r.managerBName);
	}
	return [...names].sort((a, b) => a.localeCompare(b));
}

/**
 * The pair the face-off opens on: the URL's two names when both resolve to
 * different managers, else the longest rivalry (the first row). The series
 * leader goes on the left of the default pair.
 */
export function defaultPair(rows: H2HRow[], a: string | null, b: string | null): [string, string] | null {
	if (a && b && a !== b) return [a, b];
	const first = rows[0];
	if (!first) return null;
	const r = rivalryFromRow(first);
	return r.bWins > r.aWins ? [r.b, r.a] : [r.a, r.b];
}

/** Percent widths of the share bar: each side's games won out of all games, so ties leave track. */
export function seriesShare(aWins: number, bWins: number, games: number): { a: number; b: number } {
	if (!games) return { a: 0, b: 0 };
	return { a: (aWins / games) * 100, b: (bWins / games) * 100 };
}

/** Percent widths of a rivalry card's bar: each side's share of the wins. */
export function winShare(aWins: number, bWins: number): { a: number; b: number } {
	const total = aWins + bWins;
	if (!total) return { a: 50, b: 50 };
	return { a: (aWins / total) * 100, b: (bWins / total) * 100 };
}

/** "21-19", plus "-1" when there are ties. */
export function faceRecord(r: Pick<Rivalry, 'aWins' | 'bWins' | 'ties'>): string {
	return r.ties > 0 ? `${r.aWins}-${r.bWins}-${r.ties}` : `${r.aWins}-${r.bWins}`;
}

/** Who leads the series, or null when it's tied. */
export function seriesLeader(r: Pick<Rivalry, 'a' | 'b' | 'aWins' | 'bWins'>): string | null {
	if (r.aWins === r.bWins) return null;
	return r.aWins > r.bWins ? r.a : r.b;
}

/** The luckier side of a row and its luck factor signed with two decimals, or null when even. */
export function luckier(row: H2HRow): { name: string; luck: string } | null {
	const a = num(row.managerALuckFactor);
	const b = num(row.managerBLuckFactor);
	if (a.toFixed(2) === b.toFixed(2)) return null;
	const [name, v] = a > b ? [row.managerAName, a] : [row.managerBName, b];
	return { name, luck: signedTwo(v) };
}

/** The "Every rivalry" leader cell: the series leader, or null for a tie. */
export function tableLeader(row: H2HRow): string | null {
	const r = rivalryFromRow(row);
	return seriesLeader(r);
}

/**
 * D10: until a reader changes a name in the face-off, every rivalry; after,
 * the rivalries that include either chosen manager.
 */
export function filterRivalries(rows: H2HRow[], picked: boolean, a: string | null, b: string | null): H2HRow[] {
	if (!picked) return rows;
	const names = [a, b].filter(Boolean);
	return rows.filter((r) => names.includes(r.managerAName) || names.includes(r.managerBName));
}

export const TABLE_ROWS = 12;

/* ---------- S10.8 Record book ---------- */

export type RecordKind = 'game' | 'season' | 'career';
export type RecordGroup = 'championship' | 'scoring' | 'season' | 'winloss' | 'other';

interface RecordDef {
	view: string;
	label: string;
	group: RecordGroup;
	kind: RecordKind;
	/** Counts against the holder: drawn in pennant. */
	bad?: boolean;
}

/** The view's names, in each group's board order. */
const RECORDS: RecordDef[] = [
	{ view: 'Most Championships (Career)', label: 'Most championships', group: 'championship', kind: 'career' },
	{ view: 'Most Championship Appearances (Career)', label: 'Most title-game appearances', group: 'championship', kind: 'career' },
	{ view: 'Most Playoff Appearances (Career)', label: 'Most playoff appearances', group: 'championship', kind: 'career' },
	{ view: 'Highest Single Game Score', label: 'Highest single-game score', group: 'scoring', kind: 'game' },
	{ view: 'Most Points Scored in a Season', label: 'Most points in a season', group: 'scoring', kind: 'season' },
	{ view: 'Most Career Points', label: 'Most career points', group: 'scoring', kind: 'career' },
	{ view: 'Largest Point Differential in a Season', label: 'Largest point differential in a season', group: 'scoring', kind: 'season' },
	{ view: 'Most Weeks as Top Scorer in a Season', label: 'Most weeks as top scorer in a season', group: 'scoring', kind: 'season' },
	{ view: 'Highest Scoring Game (Combined Points)', label: 'Highest-scoring game, both teams', group: 'scoring', kind: 'game' },
	{ view: 'Lowest Single Game Score', label: 'Lowest single-game score', group: 'scoring', kind: 'game', bad: true },
	{ view: 'Lowest Scoring Game (Combined Points)', label: 'Lowest-scoring game, both teams', group: 'scoring', kind: 'game', bad: true },
	{ view: 'Worst Point Differential in a Season', label: 'Worst point differential in a season', group: 'scoring', kind: 'season', bad: true },
	{ view: 'Most Points Allowed in a Season (Unluckiest Team)', label: 'Most points allowed in a season', group: 'scoring', kind: 'season', bad: true },
	{ view: 'Most Career Points Allowed (Unluckiest Manager)', label: 'Most career points allowed', group: 'scoring', kind: 'career', bad: true },
	{ view: 'Fewest Points Allowed in a Season (Luckiest Team)', label: 'Fewest points allowed in a season', group: 'scoring', kind: 'season' },
	{ view: 'Fewest Points Scored in a Season', label: 'Fewest points scored in a season', group: 'scoring', kind: 'season', bad: true },
	{ view: 'Best Regular-Season Record', label: 'Best regular-season record', group: 'season', kind: 'season' },
	{ view: 'Worst Regular-Season Record', label: 'Worst regular-season record', group: 'season', kind: 'season', bad: true },
	{ view: 'Best Career Win Percentage (Min 3 Seasons)', label: 'Best career win percentage, 3+ seasons', group: 'winloss', kind: 'career' },
	{ view: 'Closest Win (Single Game)', label: 'Closest win', group: 'winloss', kind: 'game' },
	{ view: 'Most Career Transactions', label: 'Most career transactions', group: 'other', kind: 'career' },
	{ view: 'Most Trades in a Season', label: 'Most trades in a season', group: 'other', kind: 'season' },
	{ view: 'Biggest Blowout (Single Game)', label: 'Biggest blowout', group: 'other', kind: 'game' }
];

const byView = new Map(RECORDS.map((r, i) => [r.view, { ...r, order: i }]));

/** "Most FAAB Spent in a Season" → "Most faab spent in a season". */
function sentenceCase(name: string): string {
	const s = name.toLowerCase();
	return s.charAt(0).toUpperCase() + s.slice(1);
}

/** The board's label for a view name; an unknown name falls back to sentence case. */
export function recordLabel(name: string): string {
	return byView.get(name)?.label ?? sentenceCase(name);
}

/** Where a record sits in its group: the board's order, unknown names last. */
export function recordOrder(name: string): number {
	return byView.get(name)?.order ?? RECORDS.length;
}

export function recordKind(name: string, value?: string): RecordKind {
	const known = byView.get(name)?.kind;
	if (known) return known;
	if (/\(week \d+\)/i.test(value ?? '')) return 'game';
	if (/season/i.test(name)) return 'season';
	return 'career';
}

export function recordIsBad(name: string): boolean {
	return byView.get(name)?.bad === true;
}

const UNIT_NAMES: Record<string, string> = {
	'points allowed': 'allowed',
	'total points': 'points',
	transactions: 'moves',
	championships: 'titles'
};

/** Thousands commas on a number that may carry a sign or decimals ("2405.0" → "2,405.0"). */
function withCommas(text: string): string {
	const m = text.match(/^([+-]?)(\d+)(\.\d+)?$/);
	if (!m) return text;
	return `${m[1]}${Number(m[2]).toLocaleString('en-US')}${m[3] ?? ''}`;
}

/**
 * The view's value text as the board draws it: "220.7 points (Week 17)" is
 * the value "220.7", the unit "points" and week 17; "13-2 (86.7%)" is "13-2"
 * with "86.7%" as its unit. Units are renamed as drawn.
 */
export function parseRecordValue(value: string): { value: string; unit: string; week: number | null } {
	let text = String(value ?? '').trim();
	let week: number | null = null;
	const wk = text.match(/\s*\(week (\d+)\)\s*$/i);
	if (wk) {
		week = Number(wk[1]);
		text = text.slice(0, wk.index).trim();
	}
	const rec = text.match(/^(\d+-\d+(?:-\d+)?)\s*\(([^)]+)\)$/);
	if (rec) return { value: rec[1], unit: rec[2], week };
	const m = text.match(/^([+-]?\$?[\d.]+%?)\s*(.*)$/);
	if (!m) return { value: text, unit: '', week };
	const unit = m[2].trim();
	return { value: withCommas(m[1]), unit: UNIT_NAMES[unit] ?? unit, week };
}

const HIDDEN = '--hidden--';

/**
 * Who holds a record, by manager name only (never the team, G8). A pair reads
 * "A vs. B" with the hidden placeholder last as "hidden manager"; `face` is
 * the first named manager, for the Avatar.
 */
export function recordWho(manager: string | null | undefined): { who: string; face: string | null; names: string[] } {
	const parts = String(manager ?? '')
		.split(/\s+vs\.?\s+/)
		.map((p) => p.trim())
		.filter(Boolean);
	const named = parts.filter((p) => p !== HIDDEN);
	const hidden = parts.length - named.length;
	const words = [...named, ...Array(hidden).fill('hidden manager')];
	return { who: words.join(' vs. ') || 'hidden manager', face: named[0] ?? null, names: named };
}

/** " · Week 17, 2024" for a game, " · 2024" for a season, nothing for a career. */
export function recordWhen(kind: RecordKind, season: string | number | null | undefined, week: number | null): string {
	const year = season ? String(season) : '';
	if (kind === 'career' || !year) return '';
	if (kind === 'game' && week != null) return ` · Week ${week}, ${year}`;
	return ` · ${year}`;
}

export interface ApiRecord {
	record: string;
	value: string;
	year: string;
	manager: string;
	category: string;
}

export interface BookRow {
	key: string;
	label: string;
	value: string;
	unit: string;
	who: string;
	names: string[];
	face: string | null;
	when: string;
	tone: Tone;
	/** "2026 in progress" for a single game set in the season being played. */
	live: string | null;
	category: string;
}

export function bookRow(r: ApiRecord, liveSeason: number | null): BookRow {
	const parsed = parseRecordValue(r.value);
	const kind = recordKind(r.record, r.value);
	const holder = recordWho(r.manager);
	const live = kind === 'game' && liveSeason != null && String(liveSeason) === String(r.year) ? `${liveSeason} in progress` : null;
	return {
		key: r.record,
		label: recordLabel(r.record),
		value: parsed.value,
		unit: parsed.unit,
		who: holder.who,
		names: holder.names,
		face: holder.face,
		when: recordWhen(kind, r.year, parsed.week),
		tone: recordIsBad(r.record) ? 'pennant' : 'ink',
		live,
		category: r.category
	};
}

/** The API's categories, as the chips name them, in chip order. */
export const RECORD_CHIPS: { id: string; label: string }[] = [
	{ id: 'Championship Records', label: 'Championships' },
	{ id: 'Scoring Records', label: 'Scoring' },
	{ id: 'Season Records', label: 'Season' },
	{ id: 'Win/Loss Records', label: 'Win/loss' },
	{ id: 'Transaction Records', label: 'Transactions' },
	{ id: 'Other Records', label: 'Other' }
];

/** "All" first, then each category that has records, with its count. */
export function recordChips(records: ApiRecord[]): { id: string; label: string; count: number }[] {
	const count = (id: string) => records.filter((r) => r.category === id).length;
	return [
		{ id: 'all', label: 'All', count: records.length },
		...RECORD_CHIPS.map((c) => ({ ...c, count: count(c.id) })).filter((c) => c.count > 0)
	];
}

/** The board's cards, each holding these API categories. */
const GROUPS: { id: string; name: string; categories: string[] }[] = [
	{ id: 'scoring', name: 'Scoring records', categories: ['Scoring Records'] },
	{ id: 'season', name: 'Season records', categories: ['Season Records'] },
	{ id: 'winloss', name: 'Win and loss records', categories: ['Win/Loss Records'] },
	{ id: 'other', name: 'Transactions and other', categories: ['Transaction Records', 'Other Records', 'Draft Records'] }
];

export interface Book {
	champion: BookRow[];
	groups: { id: string; name: string; count: string; rows: BookRow[] }[];
}

/**
 * The record book as drawn: the Championship records band, then one card
 * per group in the board's order, each holding only the chosen chip's
 * category (or every category for "all"). Empty cards drop out.
 */
export function recordBook(records: ApiRecord[], chip: string, liveSeason: number | null): Book {
	const shown = records.filter((r) => chip === 'all' || r.category === chip);
	const rows = (cats: string[]) =>
		shown
			.filter((r) => cats.includes(r.category))
			.sort((x, y) => recordOrder(x.record) - recordOrder(y.record) || x.record.localeCompare(y.record))
			.map((r) => bookRow(r, liveSeason));
	return {
		champion: rows(['Championship Records']),
		groups: GROUPS.map((g) => {
			const list = rows(g.categories);
			return { id: g.id, name: g.name, count: plural(list.length, 'record'), rows: list };
		}).filter((g) => g.rows.length > 0)
	};
}

/** "Nov 7, 2026". */
export function shortDate(iso: string | null | undefined): string {
	const d = iso ? new Date(iso) : null;
	if (!d || Number.isNaN(d.getTime())) return '';
	return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

/** The footnote under the last card, word for word from the board. */
export function recordFootnote(liveSeason: number | null | undefined, lastUpdated: string | null | undefined): string {
	const when = shortDate(lastUpdated);
	const season = liveSeason
		? `Season records, titles and playoff appearances only count finished seasons, so ${liveSeason} joins them after the championship.`
		: 'Season records, titles and playoff appearances only count finished seasons.';
	return `Single-game records and career totals count each week once it's final. ${season}${when ? ` Last updated ${when}.` : ''}`;
}
