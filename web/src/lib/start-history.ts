/**
 * How past teams with a given start finished (WP4 S4.2). The rows come from
 * /api/standings/start-history: one per team-season in a finished season,
 * with that team's record after the same week. Counting and wording live here,
 * out of the page, so vitest holds them.
 */

import { formatRecord } from '$lib/home';

/** One team-season's record after week N, and how its season ended. */
export interface StartHistoryRow {
	season: number;
	managerName: string | null;
	wins: number;
	losses: number;
	ties: number;
	madePlayoffs: boolean;
	wonTitle: boolean;
}

export interface StartRecord {
	wins: number;
	losses: number;
	ties: number;
}

export interface StartHistoryResult {
	/** Team-seasons with exactly this record after week N. */
	count: number;
	madePlayoffs: number;
	wonTitle: number;
	/** The first and last seasons the rows cover, matched or not. */
	firstSeason: number | null;
	lastSeason: number | null;
}

/**
 * Count the team-seasons that had exactly `record` after week N (wins, losses
 * and ties all equal, so 3-0 doesn't match 3-0-1), and how many of them made
 * the playoffs and won the title.
 */
export function startHistory(rows: StartHistoryRow[], record: StartRecord): StartHistoryResult {
	let count = 0;
	let madePlayoffs = 0;
	let wonTitle = 0;
	let firstSeason: number | null = null;
	let lastSeason: number | null = null;
	for (const row of rows) {
		if (firstSeason === null || row.season < firstSeason) firstSeason = row.season;
		if (lastSeason === null || row.season > lastSeason) lastSeason = row.season;
		if (row.wins !== record.wins || row.losses !== record.losses || row.ties !== record.ties)
			continue;
		count++;
		if (row.madePlayoffs) madePlayoffs++;
		if (row.wonTitle) wonTitle++;
	}
	return { count, madePlayoffs, wonTitle, firstSeason, lastSeason };
}

const WORDS = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine'];

/** One to nine as words, 10 and up as numerals (the house rule for counts). */
export function countWord(n: number): string {
	return n >= 0 && n < 10 && Number.isInteger(n) ? WORDS[n] : String(n);
}

function capitalize(text: string): string {
	return text.charAt(0).toUpperCase() + text.slice(1);
}

/** "None won the title.", "One won the title.", "Four won the title.", "12 won the title." */
function titlesSentence(won: number): string {
	if (won === 0) return 'None won the title.';
	return `${capitalize(countWord(won))} won the title.`;
}

export interface HistoryTile {
	label: string;
	value: string;
	sub: string;
}

/**
 * The stat strip's history tile: "3-0 starts that made the playoffs", "30 of
 * 32", "Since 2005. Four won the title." With no match, it says so: "3-0
 * starts since 2005", "None". Null when there are no finished seasons at all.
 */
export function historyTile(result: StartHistoryResult, record: StartRecord): HistoryTile | null {
	if (result.firstSeason === null || result.lastSeason === null) return null;
	const wl = formatRecord(record.wins, record.losses, record.ties);
	if (result.count === 0) {
		return {
			label: `${wl} starts since ${result.firstSeason}`,
			value: 'None',
			sub: `No team started ${wl} from ${result.firstSeason} to ${result.lastSeason}.`
		};
	}
	return {
		label: `${wl} starts that made the playoffs`,
		value: `${result.madePlayoffs} of ${result.count}`,
		sub: `Since ${result.firstSeason}. ${titlesSentence(result.wonTitle)}`
	};
}
