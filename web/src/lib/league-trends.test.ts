import { describe, expect, it } from 'vitest';
import {
	balanceOf,
	pearson,
	recordPersistence,
	recordRank,
	rollingMean,
	scoringRank,
	seasonRecords,
	type RegularGame
} from './league-trends';

const game = (teamA: number, teamB: number, pointsA: number, pointsB: number): RegularGame => ({
	season: 2025,
	teamA,
	teamB,
	pointsA,
	pointsB,
	winner: pointsA > pointsB ? teamA : pointsB > pointsA ? teamB : null,
	tie: pointsA === pointsB
});

describe('balanceOf', () => {
	it('a league where every team goes .500 spreads less than chance', () => {
		const records = [1, 2, 3, 4, 5, 6].map((team) => ({ team, wins: 7, games: 14, pointsFor: 1400 }));
		const b = balanceOf(2025, records);
		expect(b.nollScully).toBe(0);
		expect(b.skillShare).toBe(0);
	});

	it('reads spread beyond chance as skill', () => {
		// 14 games: luck sd = 0.5/sqrt(14) = 0.134. Win% from .071 to .929 is far wider.
		const wins = [1, 3, 5, 7, 9, 11, 13];
		const records = wins.map((w, team) => ({ team, wins: w, games: 14, pointsFor: 0 }));
		const b = balanceOf(2025, records);
		expect(b.nollScully!).toBeGreaterThan(2);
		expect(b.skillShare!).toBeGreaterThan(0.75);
	});
});

describe('seasonRecords and ranks', () => {
	const games = [game(1, 2, 150, 100), game(3, 4, 90, 95), game(1, 3, 140, 141), game(2, 4, 110, 110)];
	const recs = seasonRecords(games).get(2025)!;

	it('counts wins, games and points per team, ties as half', () => {
		const t2 = recs.find((r) => r.team === 2)!;
		expect(t2).toMatchObject({ wins: 0.5, games: 2, pointsFor: 210 });
	});

	it('ranks by points per game and by record', () => {
		expect(scoringRank(recs, 1)).toBe(1); // 145 per game
		expect(recordRank(recs, 4)).toBe(1); // a win and a tie: .750
		expect(recordRank(recs, 3)).toBe(3); // 1-1, behind 1-1 team 1 on points
		expect(scoringRank(recs, 99)).toBeNull();
	});
});

describe('recordPersistence', () => {
	it('correlates a manager with their own next season', () => {
		const m = new Map([
			['a|2020', 0.8],
			['a|2021', 0.7],
			['b|2020', 0.2],
			['b|2021', 0.3],
			['c|2020', 0.5],
			['c|2021', 0.5],
			['d|2021', 0.9] // no 2022, no pair
		]);
		const { r, n } = recordPersistence(m);
		expect(n).toBe(3);
		expect(r!).toBeGreaterThan(0.9);
	});
});

describe('helpers', () => {
	it('pearson needs three points and variance', () => {
		expect(pearson([1, 2], [1, 2])).toBeNull();
		expect(pearson([1, 1, 1], [1, 2, 3])).toBeNull();
		expect(pearson([1, 2, 3], [2, 4, 6])).toBeCloseTo(1);
	});

	it('rolling mean trails and skips gaps', () => {
		expect(rollingMean([1, 3, null, 5], 2)).toEqual([1, 2, 3, 5]);
	});
});
