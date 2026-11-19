export type GameResult = 'W' | 'L' | 'T';

export interface Streaks {
	longestWinStreak: number;
	longestLossStreak: number;
	totalGames: number;
}

/**
 * Longest run of wins and of losses in a manager's games, in the order
 * played. A tie ends both runs. Shared by the single-manager path and the
 * all-managers rows (WP14 S14.6) so the two can't disagree.
 */
export function longestStreaks(results: Iterable<GameResult>): Streaks {
	let longestWin = 0,
		longestLoss = 0,
		curWin = 0,
		curLoss = 0,
		totalGames = 0;
	for (const result of results) {
		totalGames++;
		if (result === 'W') {
			curWin++;
			curLoss = 0;
			if (curWin > longestWin) longestWin = curWin;
		} else if (result === 'L') {
			curLoss++;
			curWin = 0;
			if (curLoss > longestLoss) longestLoss = curLoss;
		} else {
			curWin = 0;
			curLoss = 0;
		}
	}
	return { longestWinStreak: longestWin, longestLossStreak: longestLoss, totalGames };
}

/**
 * Streaks for every manager from one ordered list of game rows (ordered by
 * manager, then season, team and week, as the query returns them).
 */
export function streaksByManager(
	rows: Iterable<{ managerName: string; result: GameResult }>
): Map<string, Streaks> {
	const games = new Map<string, GameResult[]>();
	for (const row of rows) {
		const list = games.get(row.managerName) ?? [];
		list.push(row.result);
		games.set(row.managerName, list);
	}
	return new Map([...games].map(([name, results]) => [name, longestStreaks(results)]));
}
