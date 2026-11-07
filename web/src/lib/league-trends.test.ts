import { describe, expect, it } from 'vitest';
import {
	balanceOf,
	coinFlipBand,
	kde,
	leagueEvents,
	quantile,
	splitEras,
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

describe('coinFlipBand', () => {
	it('brackets 1.0 and is stable for a seed', () => {
		const a = coinFlipBand(10, 13, { sims: 1500 });
		expect(a.lo).toBeLessThan(1);
		expect(a.hi).toBeGreaterThan(1);
		// Ten teams over 13 coin-flip games: roughly 0.6 to 1.4.
		expect(a.lo).toBeGreaterThan(0.45);
		expect(a.hi).toBeLessThan(1.6);
		expect(coinFlipBand(10, 13, { sims: 1500 })).toEqual(a);
	});

	it('handles an odd number of teams', () => {
		const b = coinFlipBand(9, 12, { sims: 300 });
		expect(b.hi).toBeGreaterThan(b.lo);
	});
});

describe('kde and quantile', () => {
	it('peaks at the data and integrates to about 1', () => {
		const values = [100, 102, 98, 101, 99, 150];
		const xs = Array.from({ length: 301 }, (_, i) => i);
		const d = kde(values, xs);
		expect(xs[d.indexOf(Math.max(...d))]).toBeGreaterThan(95);
		expect(xs[d.indexOf(Math.max(...d))]).toBeLessThan(105);
		expect(d.reduce((a, b) => a + b, 0)).toBeCloseTo(1, 1);
	});

	it('interpolates quantiles', () => {
		expect(quantile([1, 2, 3, 4], 0.5)).toBe(2.5);
		expect(quantile([], 0.5)).toBeNull();
	});
});

describe('splitEras', () => {
	it('splits 22 seasons 8, 7, 7, the early eras taking the remainder', () => {
		const years = Array.from({ length: 22 }, (_, i) => 2005 + i);
		expect(splitEras(years)).toEqual([
			{ id: 'early', name: 'Early Era', start: 2005, end: 2012 },
			{ id: 'middle', name: 'Middle Era', start: 2013, end: 2019 },
			{ id: 'modern', name: 'Modern Era', start: 2020, end: 2026 }
		]);
		expect(splitEras([2005, 2006])).toEqual([]);
	});
});

describe('leagueEvents', () => {
	it('reports only stored settings that changed, and the first FAAB season', () => {
		const s = (season: number, teams: number, regularWeeks: number, playoffTeams: number) => ({ season, teams, regularWeeks, playoffTeams });
		const events = leagueEvents([s(2005, 8, 14, 6), s(2006, 10, 13, 6), s(2007, 10, 13, 6), s(2008, 10, 13, 8)], 2007);
		expect(events).toEqual([
			{ season: 2006, labels: ['10 teams', '13-week season'] },
			{ season: 2007, labels: ['FAAB bidding'] },
			{ season: 2008, labels: ['8-team playoff'] }
		]);
	});

	it('ignores settings the data leaves blank, like a season still being played', () => {
		const events = leagueEvents(
			[
				{ season: 2025, teams: 10, regularWeeks: 14, playoffTeams: 6 },
				{ season: 2026, teams: 10, regularWeeks: null, playoffTeams: null }
			],
			null
		);
		expect(events).toEqual([]);
	});
});
