/**
 * How past teams with a given start finished, for This Season's notes (WP4
 * S4.6). The rows come from /api/standings/start-history: one per team-season
 * in a finished season, with that team's record after the same week. Counting
 * and wording live here, out of the page, so vitest holds them.
 */

/** One team-season's record after week N, and how its season ended. */
export interface StartHistoryRow {
	season: number;
	managerName: string | null;
	wins: number;
	losses: number;
	ties: number;
	madePlayoffs: boolean;
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
	/** The first season the rows cover, matched or not. */
	firstSeason: number | null;
}

/**
 * Count the team-seasons that had exactly `record` after week N (wins, losses
 * and ties all equal, so 3-0 doesn't match 3-0-1), and how many of them made
 * the playoffs.
 */
export function startHistory(rows: StartHistoryRow[], record: StartRecord): StartHistoryResult {
	let count = 0;
	let madePlayoffs = 0;
	let firstSeason: number | null = null;
	for (const row of rows) {
		if (firstSeason === null || row.season < firstSeason) firstSeason = row.season;
		if (row.wins !== record.wins || row.losses !== record.losses || row.ties !== record.ties)
			continue;
		count++;
		if (row.madePlayoffs) madePlayoffs++;
	}
	return { count, madePlayoffs, firstSeason };
}

const WORDS = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine'];

/** One to nine as words, 10 and up as numerals (the house rule for counts). */
export function countWord(n: number): string {
	return n >= 0 && n < 10 && Number.isInteger(n) ? WORDS[n] : String(n);
}
