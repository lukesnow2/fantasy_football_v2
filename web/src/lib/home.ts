/**
 * Pure rules behind Home (WP3). Every line on the page is computed from an API
 * response, so the rules live here, out of the page, where vitest holds them.
 */

import type { TagVariant } from '$lib/components/ui/Tag.svelte';

/* ---------- Shared ---------- */

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

function titlesOf(row: HallOfFameRow | undefined): number {
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
function seriesStart(firstMatchupDate: string | null | undefined): string {
	return firstMatchupDate?.match(/^\d{4}/)?.[0] ?? '';
}

/**
 * The league's biggest rivalry: the pair with the most meetings. The API
 * orders by meetings alone, so a tie is broken by the pair's names, and the
 * card doesn't swap pairs from one load to the next.
 */
export function biggestRivalry(rows: H2HRow[]): H2HRow | null {
	const key = (r: H2HRow) => `${r.managerAName}|${r.managerBName}`;
	return (
		[...rows].sort(
			(a, b) =>
				(Number(b.totalMatchups) || 0) - (Number(a.totalMatchups) || 0) ||
				key(a).localeCompare(key(b))
		)[0] ?? null
	);
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
		score: `${leftWins}-${rightWins}`,
		meetings: Number(pair.totalMatchups) || 0,
		since: seriesStart(pair.firstMatchupDate),
		leftShare: decided > 0 ? (leftWins / decided) * 100 : 50
	};
}

/**
 * The line under the rivalry bar: fixed copy, true by how the pair is chosen
 * (the API's most-played pair), so it can't go stale.
 */
export const RIVALRY_LINE = 'No two managers have played each other more.';

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
 * "Even trade" on the wash. A winner that is neither manager gets no tag, so
 * a team name or a stray label never reaches the page (G8).
 */
export function tradeTag(trade: TradeRow): { text: string; even: boolean } | null {
	const winner = trade.tradeWinner?.trim();
	if (!winner) return null;
	if (winner.toLowerCase() === 'even trade') return { text: 'Even trade', even: true };
	const a = oneDecimal(trade.teamAFinalScore);
	const b = oneDecimal(trade.teamBFinalScore);
	if (winner === trade.teamAManager) return { text: `${winner} wins ${a}-${b}`, even: false };
	if (winner === trade.teamBManager) return { text: `${winner} wins ${b}-${a}`, even: false };
	return null;
}
