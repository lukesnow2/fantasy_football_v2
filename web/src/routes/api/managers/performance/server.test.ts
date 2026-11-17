import { beforeEach, describe, expect, it, vi } from 'vitest';
import { paramsOf, textOf } from '$lib/server/testing/sql-text';
import { longestStreaks, streaksByManager, type GameResult } from './streaks';

/**
 * WP14 S14.6: the all-managers rows gain pointsAgainst,
 * finishedPointsPerSeason and streaks, and the single-manager streaks still
 * come out the same. The database is a stub that answers each query by what
 * it reads from, so the test runs without a warehouse.
 */

const games: Record<string, GameResult[]> = {
	Ann: ['W', 'W', 'L', 'W', 'W', 'W', 'T', 'L', 'L'],
	Bo: ['L', 'L', 'L', 'W', 'L', 'W', 'W']
};

const execute = vi.fn(async (query: unknown) => {
	const text = textOf(query);
	if (text.includes('FROM edw.mart_manager_performance')) {
		return ['Ann', 'Bo'].map((name, i) => ({
			manager_name: name,
			first_season: 2005,
			last_season: 2026,
			total_seasons: 3,
			total_wins: 10 - i,
			total_losses: 5 + i,
			total_ties: 0,
			win_percentage: 0.6 - i / 10,
			total_points_scored: 3000,
			avg_points_per_game: 125,
			total_championships: 1 - i,
			// The mart stores no rate for a manager with no bracket games.
			playoff_win_percentage: i === 0 ? 0.7143 : null
		}));
	}
	if (
		text.includes('CROSS JOIN LATERAL (VALUES') &&
		text.includes('AS side(manager_key, team_key)')
	) {
		return Object.entries(games).flatMap(([managerName, results]) =>
			results.map((result) => ({ managerName, result }))
		);
	}
	if (text.includes("WHERE fm.matchup_type IN ('playoffs', 'championship')")) {
		// Bo has played no bracket game.
		return [{ managerName: 'Ann', playoffWins: '5', playoffLosses: '2' }];
	}
	if (text.includes('SUM(tp.points_against) AS points_against')) {
		return [
			{ managerName: 'Ann', pointsAgainst: '2890.50', finishedPointsPerSeason: '1950.25' },
			{ managerName: 'Bo', pointsAgainst: '3101.00', finishedPointsPerSeason: null }
		];
	}
	if (text.includes("ELSE 'L' END AS result") && text.includes('dm.manager_name =')) {
		const [name] = paramsOf(query) as string[];
		return (games[name] ?? []).map((result) => ({ result }));
	}
	return [];
});

vi.mock('$lib/server/db', () => ({ db: { execute: (q: unknown) => execute(q) } }));
vi.mock('$lib/server/playoff-odds-service', () => ({
	getSeasonOdds: async () => ({ available: false, teams: [] })
}));

const { GET } = await import('./+server');

async function get(query: string) {
	const response = await GET({
		url: new URL(`http://x/api/managers/performance${query}`)
	} as never);
	return (await response.json()).data;
}

describe('longestStreaks', () => {
	it('counts the longest runs, a tie ending both', () => {
		expect(longestStreaks(games.Ann)).toEqual({
			longestWinStreak: 3,
			longestLossStreak: 2,
			totalGames: 9
		});
		expect(longestStreaks(['W', 'T', 'W'])).toMatchObject({ longestWinStreak: 1 });
		expect(longestStreaks([])).toEqual({
			longestWinStreak: 0,
			longestLossStreak: 0,
			totalGames: 0
		});
	});

	it('splits one ordered list by manager', () => {
		const map = streaksByManager([
			{ managerName: 'A', result: 'W' },
			{ managerName: 'A', result: 'W' },
			{ managerName: 'B', result: 'L' }
		]);
		expect(map.get('A')?.longestWinStreak).toBe(2);
		expect(map.get('B')?.longestLossStreak).toBe(1);
	});
});

describe('GET /api/managers/performance', () => {
	beforeEach(() => execute.mockClear());

	it('gives every all-managers row the three career fields', async () => {
		const data = await get('');
		expect(data.performance).toHaveLength(2);
		for (const row of data.performance) {
			expect(row).toHaveProperty('pointsAgainst');
			expect(row).toHaveProperty('finishedPointsPerSeason');
			expect(row).toHaveProperty('streaks');
			expect(row).toHaveProperty('playoffWins');
			expect(row).toHaveProperty('playoffLosses');
		}
		const ann = data.performance.find((r: { managerName: string }) => r.managerName === 'Ann');
		expect(ann.pointsAgainst).toBe(2890.5);
		expect(ann.finishedPointsPerSeason).toBe(1950.25);
		expect(ann.streaks).toEqual({ longestWinStreak: 3, longestLossStreak: 2, totalGames: 9 });
		expect(ann.playoffWins).toBe(5);
		expect(ann.playoffLosses).toBe(2);
		const bo = data.performance.find((r: { managerName: string }) => r.managerName === 'Bo');
		expect(bo.finishedPointsPerSeason).toBeNull();
		// No bracket games is 0-0, not missing.
		expect(bo.playoffWins).toBe(0);
		expect(bo.playoffLosses).toBe(0);
		expect(bo.streaks).toMatchObject({ longestWinStreak: 2, longestLossStreak: 3 });
	});

	it('gives no playoff rank to a manager with no playoff win rate', async () => {
		const data = await get('');
		const by = (name: string) =>
			data.performance.find((r: { managerName: string }) => r.managerName === name);
		expect(by('Ann').playoffRank).toBe(1);
		expect(by('Bo').playoffWinPercentage).toBeNull();
		expect(by('Bo').playoffRank).toBeNull();
	});

	it('matches the single-manager streaks for every manager', async () => {
		const all = await get('');
		for (const row of all.performance) {
			const one = await get(`?manager=${encodeURIComponent(row.managerName)}&analysis=all`);
			expect(one.streaks, row.managerName).toEqual(row.streaks);
		}
	});

	it('leaves the single-manager rows as they were', async () => {
		const one = await get('?manager=Ann');
		expect(one.performance[0]).not.toHaveProperty('pointsAgainst');
		expect(one.performance[0]).not.toHaveProperty('streaks');
	});
});
