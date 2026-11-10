import { describe, expect, it } from 'vitest';
import {
	calibration,
	grade,
	scoreForecasts,
	seasonResult,
	type Forecast,
	type SeasonGame
} from './playoff-odds-grading';

const game = (
	week: number,
	teamA: string,
	teamB: string,
	scoreA: number,
	scoreB: number,
	extra: Partial<SeasonGame> = {}
): SeasonGame => ({
	week,
	teamA,
	teamB,
	scoreA,
	scoreB,
	isPlayoffs: false,
	isConsolation: false,
	isChampionship: false,
	winner: scoreA > scoreB ? teamA : scoreB > scoreA ? teamB : null,
	...extra
});

// Four teams, two regular-season weeks, a four-team field with no byes.
// a 2-0, b 1-1 (more points than c), c 1-1, d 0-2.
const regular = [
	game(1, 'a', 'b', 120, 100),
	game(1, 'c', 'd', 110, 90),
	game(2, 'a', 'c', 105, 95),
	game(2, 'b', 'd', 130, 80)
];

describe('seasonResult', () => {
	it('knows nothing until the playoffs start', () => {
		expect(seasonResult(regular, 2)).toEqual({
			playoffs: null,
			bye: null,
			topSeed: null,
			champion: null
		});
	});

	it('reads the field, byes, top seed and champion', () => {
		// Six-team bracket shape on a smaller scale: a and b sit out week 3.
		const games = [
			...regular,
			game(3, 'c', 'd', 100, 90, { isPlayoffs: true }),
			game(4, 'a', 'c', 110, 100, { isPlayoffs: true }),
			game(4, 'b', 'e', 90, 95, { isPlayoffs: true }),
			game(5, 'a', 'e', 120, 100, { isPlayoffs: true, isChampionship: true })
		];
		const r = seasonResult(games, 2);
		expect([...r.playoffs!].sort()).toEqual(['a', 'b', 'c', 'd', 'e']);
		expect([...r.bye!].sort()).toEqual(['a', 'b', 'e']);
		expect(r.topSeed).toBe('a');
		expect(r.champion).toBe('a');
	});

	it('leaves consolation games out of the field and the standings', () => {
		const games = [
			...regular,
			game(3, 'a', 'b', 100, 90, { isPlayoffs: true }),
			game(3, 'c', 'd', 200, 10, { isConsolation: true })
		];
		const r = seasonResult(games, 2);
		expect([...r.playoffs!].sort()).toEqual(['a', 'b']);
		expect(r.champion).toBeNull();
	});
});

const forecast = (teamId: string, p: number, title = 0.25): Forecast => ({
	season: 2026,
	throughWeek: 1,
	regularSeasonWeeks: 2,
	teams: 4,
	playoffTeams: 2,
	byes: 0,
	teamId,
	p: { playoffs: p, bye: 0, topSeed: 0.25, title }
});

describe('scoreForecasts and grade', () => {
	const result = new Map([
		[2026, { playoffs: new Set(['a', 'b']), bye: new Set<string>(), topSeed: 'a', champion: null }]
	]);

	it('skips outcomes not yet known and brackets without byes', () => {
		const scored = scoreForecasts([forecast('a', 0.9)], result);
		expect(scored.map((s) => s.outcome).sort()).toEqual(['playoffs', 'topSeed']);
	});

	it('grades only the title once the regular season is over', () => {
		const done = { ...forecast('a', 1), throughWeek: 2 };
		const withTitle = new Map([[2026, { ...result.get(2026)!, champion: 'a' }]]);
		expect(scoreForecasts([done], withTitle).map((s) => s.outcome)).toEqual(['title']);
	});

	it('scores a perfect forecast at zero and the baseline at zero skill', () => {
		const perfect = scoreForecasts(
			[forecast('a', 1), forecast('b', 1), forecast('c', 0), forecast('d', 0)],
			result
		).filter((s) => s.outcome === 'playoffs');
		const g = grade(perfect)!;
		expect(g.brier).toBe(0);
		expect(g.baselineBrier).toBeCloseTo(0.25);
		expect(g.skill).toBe(1);

		const flat = scoreForecasts(
			['a', 'b', 'c', 'd'].map((t) => forecast(t, 0.5)),
			result
		).filter((s) => s.outcome === 'playoffs');
		expect(grade(flat)!.skill).toBeCloseTo(0);
		expect(grade(flat)!.logLoss).toBeCloseTo(Math.log(2));
	});

	it('caps the log loss of a confident miss', () => {
		const miss = scoreForecasts([forecast('c', 1)], result).filter((s) => s.outcome === 'playoffs');
		expect(grade(miss)!.logLoss).toBeCloseTo(-Math.log(0.005));
	});

	it('returns null with nothing to grade', () => {
		expect(grade([])).toBeNull();
	});
});

describe('calibration', () => {
	it('bins forecasts and includes 100% in the top bin', () => {
		const result = new Map([
			[2026, { playoffs: new Set(['a']), bye: null, topSeed: null, champion: null }]
		]);
		const scored = scoreForecasts(
			[forecast('a', 1), forecast('b', 0.96), forecast('c', 0.02)],
			result
		).filter((s) => s.outcome === 'playoffs');
		const bins = calibration(scored);
		expect(bins.map((b) => [b.from, b.n, b.observed])).toEqual([
			[0, 1, 0],
			[0.95, 2, 0.5]
		]);
	});
});
