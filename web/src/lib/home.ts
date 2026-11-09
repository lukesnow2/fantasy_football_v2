/**
 * Pure rules behind Home (WP3). Every line on the page is computed from an API
 * response, so the rules live here, out of the page, where vitest holds them.
 */

import type { TagVariant } from '$lib/components/ui/Tag.svelte';

/* ---------- Shared wording ---------- */

const WORDS = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine'];

/** A count as the house writes it: one to nine as words, 10 and up as numerals. */
export function countWord(n: number): string {
	return n >= 0 && n < 10 && Number.isInteger(n) ? WORDS[n] : String(n);
}

function capitalize(s: string): string {
	return s.charAt(0).toUpperCase() + s.slice(1);
}

/** Names joined with ", " and a final " and ". */
export function joinNames(names: string[]): string {
	if (names.length <= 1) return names.join('');
	return `${names.slice(0, -1).join(', ')} and ${names[names.length - 1]}`;
}

/** A manager's profile URL. */
export function profileHref(name: string): string {
	return `/managers/${encodeURIComponent(name)}`;
}

/** A whole number with thousands separators ("10,612"). */
export function formatCount(n: number | string | null | undefined): string {
	const value = Number(n);
	return Number.isFinite(value) ? Math.round(value).toLocaleString('en-US') : '0';
}

/** W-L, with ties as a third number only when there are any. */
export function formatRecord(wins: number, losses: number, ties = 0): string {
	return ties > 0 ? `${wins}-${losses}-${ties}` : `${wins}-${losses}`;
}

/* ---------- S3.2 This week ---------- */

/**
 * Playoff odds as a percentage. A locked team (clinched or eliminated) prints
 * certainty; otherwise the simulation never claims 100% or 0%. The same rule
 * PlayoffOdds.svelte uses, so WP4 can share it.
 */
export function formatOdds(p: number, locked: boolean): string {
	if (locked) return p >= 1 ? '100%' : '0%';
	if (p > 0.995) return '>99%';
	if (p < 0.005) return '<1%';
	return `${Math.round(p * 100)}%`;
}

export type ThisWeekMode = 'regular' | 'playoffs' | 'final';

/**
 * Which "This week" card to draw (D4): the regular season as drawn, the
 * playoffs (odds hidden) once the odds API says the postseason is on, and the
 * final standings once the season has a champion. Without an answer from the
 * odds API, the standings' weeks decide: `currentWeek` is the last completed
 * week, so the playoffs are on once the whole regular season has been played.
 */
export function thisWeekMode(
	isSeasonComplete: boolean | undefined,
	oddsPhase: string | undefined,
	currentWeek?: number,
	regularSeasonWeeks?: number
): ThisWeekMode {
	if (isSeasonComplete) return 'final';
	if (oddsPhase === 'postseason') return 'playoffs';
	if (
		oddsPhase === undefined &&
		currentWeek !== undefined &&
		regularSeasonWeeks !== undefined &&
		currentWeek >= regularSeasonWeeks
	)
		return 'playoffs';
	return 'regular';
}

/**
 * The Finish tag (WP4 S4.9): the standings API's `playoffTier` in the board's
 * words and colors. No tier at all gives no tag, so a season whose tiers
 * haven't landed yet doesn't call its champion "Missed playoffs".
 */
export function finishTag(
	tier: string | null | undefined
): { text: string; variant: TagVariant } | null {
	if (!tier) return null;
	switch (tier) {
		case 'Champion':
			return { text: 'Champion', variant: 'champion-strong' };
		case 'Runner-up':
			return { text: 'Runner-up', variant: 'champion' };
		case 'Semifinalist':
		case 'Quarterfinalist':
			return { text: tier, variant: 'bye' };
		case 'Consolation Winner':
			return { text: 'Consolation winner', variant: 'eliminated' };
		case 'Consolation Runner-up':
			return { text: 'Consolation runner-up', variant: 'eliminated' };
		case 'Consolation 3rd':
			return { text: 'Consolation 3rd', variant: 'eliminated' };
		case 'Consolation 4th (Last Place)':
			return { text: 'Last Place Trophy', variant: 'last-place' };
		default:
			return { text: 'Missed playoffs', variant: 'eliminated' };
	}
}

/* ---------- S3.3 The stat strip ---------- */

export interface Overview {
	totalSeasons: number;
	firstSeason: string;
	lastSeason: string;
	totalManagers: number;
	totalTransactions: number;
	totalMatchups: number;
	championshipGames: number;
}

/** The four tiles, value first, in the board's order. */
export function statStrip(o: Overview): { value: string; label: string }[] {
	return [
		{ value: formatCount(o.totalSeasons), label: `seasons, ${o.firstSeason} to ${o.lastSeason}` },
		{ value: formatCount(o.totalManagers), label: 'managers all-time' },
		{ value: formatCount(o.totalTransactions), label: 'transactions' },
		{ value: formatCount(o.totalMatchups), label: 'matchups played' }
	];
}

/* ---------- S3.4 The banner wall ---------- */

export interface HallOfFameRow {
	managerName: string;
	championshipsWon?: number | string | null;
	hallOfFameRank?: number | string | null;
}

export function titlesOf(row: HallOfFameRow | undefined): number {
	const n = Number(row?.championshipsWon ?? 0);
	return Number.isFinite(n) ? n : 0;
}

/**
 * One row per manager with a title: most titles first, ties in Hall of Fame
 * rank order (the API's order when a rank is missing).
 */
export function bannerWall(hallOfFame: HallOfFameRow[]): { name: string; count: number }[] {
	return hallOfFame
		.map((row, index) => ({
			name: row.managerName,
			count: titlesOf(row),
			rank: Number(row.hallOfFameRank ?? Number.POSITIVE_INFINITY),
			index
		}))
		.filter((r) => r.count > 0)
		.sort((a, b) => b.count - a.count || a.rank - b.rank || a.index - b.index)
		.map(({ name, count }) => ({ name, count }));
}

/** The pennants' accessible name: "1 title", "7 titles". */
export function titlesLabel(n: number): string {
	return n === 1 ? '1 title' : `${n} titles`;
}

/* ---------- S3.5 From the record book ---------- */

export interface RecordRow {
	record: string;
	value: string;
	holder: string;
	year: string | number;
	manager?: string;
}

/**
 * The leading number of a record's value ("220.7 points (Week 17)" gives
 * "220.7") and when it happened ("Week 17, 2024"; the season alone when the
 * value names no week).
 */
export function parseRecord(value: string, year: string | number): { value: string; when: string } {
	const number = value.match(/^[+-]?[\d,]*\.?\d+/)?.[0] ?? value;
	const week = value.match(/\(Week (\d+)\)/)?.[1];
	const season = String(year ?? '').trim();
	const when = week ? (season ? `Week ${week}, ${season}` : `Week ${week}`) : season;
	return { value: number, when };
}

const FEATURED_RECORDS: [string, string][] = [
	['Highest Single Game Score', 'Highest single-game score'],
	['Biggest Blowout (Single Game)', 'Biggest blowout'],
	['Closest Win (Single Game)', 'Closest win']
];

/** The holder as a manager name, never a team name (G8). */
function holderName(row: RecordRow): string {
	return row.manager?.trim() || row.holder.replace(/\s*\(.*\)\s*$/, '');
}

/** The three single-game records Home features, in the board's order. */
export function featuredRecords(
	records: RecordRow[]
): { label: string; value: string; who: string; when: string }[] {
	return FEATURED_RECORDS.flatMap(([record, label]) => {
		const row = records.find((r) => r.record === record);
		if (!row) return [];
		return [{ label, who: holderName(row), ...parseRecord(row.value, row.year) }];
	});
}

/* ---------- S3.6 Biggest rivalry ---------- */

export interface H2HRow {
	managerAName: string;
	managerBName: string;
	totalMatchups: number;
	managerAWins: number;
	managerBWins: number;
	firstMatchupDate?: string | null;
}

export interface Rivalry {
	left: string;
	right: string;
	leftWins: number;
	rightWins: number;
	level: boolean;
	/** Leader first: "21-19". */
	score: string;
	meetings: number;
	since: string;
	/** The left manager's share of decided games, 0 to 100. */
	leftShare: number;
}

/** "40 meetings since 2005", or "40 meetings" when the start year is unknown. */
export function meetingsSince(count: number | string, since: string): string {
	return since ? `${count} meetings since ${since}` : `${count} meetings`;
}

/** The year a series began, from "2005-W1". */
export function seriesStart(firstMatchupDate: string | null | undefined): string {
	return firstMatchupDate?.match(/^\d{4}/)?.[0] ?? '';
}

/** The series leader on the left; manager A there when it's level. */
export function rivalry(pair: H2HRow): Rivalry {
	const aWins = Number(pair.managerAWins) || 0;
	const bWins = Number(pair.managerBWins) || 0;
	const bLeads = bWins > aWins;
	const [left, right, leftWins, rightWins] = bLeads
		? [pair.managerBName, pair.managerAName, bWins, aWins]
		: [pair.managerAName, pair.managerBName, aWins, bWins];
	const decided = leftWins + rightWins;
	return {
		left,
		right,
		leftWins,
		rightWins,
		level: aWins === bWins,
		score: `${leftWins}-${rightWins}`,
		meetings: Number(pair.totalMatchups) || 0,
		since: seriesStart(pair.firstMatchupDate),
		leftShare: decided > 0 ? (leftWins / decided) * 100 : 50
	};
}

/**
 * The line under the rivalry bar. The subject is the manager trailing the
 * series; the line lists the current managers with a winning record against
 * them, the rival first, then by meetings, up to three names.
 */
export function rivalryLine(
	pair: H2HRow,
	hallOfFame: HallOfFameRow[],
	h2hRows: H2HRow[],
	currentManagers: string[]
): string {
	const r = rivalry(pair);
	if (r.level) {
		return `${r.left} and ${r.right} have split ${meetingsSince(countWord(r.meetings), r.since)}.`;
	}

	const subject = r.right;
	const current = new Set(currentManagers);
	const ahead = h2hRows
		.flatMap((row, index) => {
			const subjectIsA = row.managerAName === subject;
			if (!subjectIsA && row.managerBName !== subject) return [];
			const other = subjectIsA ? row.managerBName : row.managerAName;
			const otherWins = Number(subjectIsA ? row.managerBWins : row.managerAWins) || 0;
			const subjectWins = Number(subjectIsA ? row.managerAWins : row.managerBWins) || 0;
			if (!current.has(other) || otherWins <= subjectWins) return [];
			return [{ name: other, meetings: Number(row.totalMatchups) || 0, index }];
		})
		.sort(
			(a, b) =>
				Number(b.name === r.left) - Number(a.name === r.left) ||
				b.meetings - a.meetings ||
				a.index - b.index
		);

	const titles = titlesOf(hallOfFame.find((h) => h.managerName === subject));
	const lead =
		titles >= 2
			? `${capitalize(countWord(titles))} titles, and ${subject} still`
			: titles === 1
				? `One title, and ${subject} still`
				: subject;

	if (ahead.length === 0) {
		const tail = `doesn't trail any current manager head to head.`;
		return titles > 0 ? `${lead.replace(/ still$/, '')} ${tail}` : `${subject} ${tail}`;
	}

	const shown = ahead.slice(0, 3).map((a) => a.name);
	const rest = ahead.length - shown.length;
	const list = joinNames(
		rest > 0 ? [...shown, `${countWord(rest)} ${rest === 1 ? 'other' : 'others'}`] : shown
	);
	const who =
		ahead.length === 1 ? 'one current manager' : `${countWord(ahead.length)} current managers`;
	return `${lead} trails ${who} head to head: ${list}.`;
}

/* ---------- S3.7 Latest trades ---------- */

export interface TradeRow {
	teamAManager: string;
	teamBManager: string;
	teamAFinalScore?: number | string | null;
	teamBFinalScore?: number | string | null;
	tradeWinner?: string | null;
}

function oneDecimal(n: number | string | null | undefined): string {
	const value = Number(n);
	return Number.isFinite(value) ? value.toFixed(1) : '0.0';
}

/**
 * The verdict tag: "{winner} wins {winner's score}-{loser's score}", or
 * "Even trade" on the wash.
 */
export function tradeTag(trade: TradeRow): { text: string; even: boolean } | null {
	const winner = trade.tradeWinner?.trim();
	if (!winner) return null;
	if (winner.toLowerCase() === 'even trade') return { text: 'Even trade', even: true };
	const a = oneDecimal(trade.teamAFinalScore);
	const b = oneDecimal(trade.teamBFinalScore);
	if (winner === trade.teamAManager) return { text: `${winner} wins ${a}-${b}`, even: false };
	if (winner === trade.teamBManager) return { text: `${winner} wins ${b}-${a}`, even: false };
	return { text: `${winner} wins`, even: false };
}
