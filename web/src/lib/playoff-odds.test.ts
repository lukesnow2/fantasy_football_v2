import { describe, expect, it } from 'vitest';
import {
	bracketSlots,
	buildSchedule,
	gamesFingerprint,
	rng,
	simulatePlayoffOdds,
	unknownCycleWeeks,
	type PlayedGame
} from './playoff-odds';

const TEAMS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

/** Circle-method round robin: 9 weeks, every pair exactly once. */
function roundRobin(teams: number[]): [number, number][][] {
	const n = teams.length;
	const fixed = teams[0];
	let rot = teams.slice(1);
	const weeks: [number, number][][] = [];
	for (let w = 0; w < n - 1; w++) {
		const line = [fixed, ...rot];
		const week: [number, number][] = [];
		for (let i = 0; i < n / 2; i++) week.push([line[i], line[n - 1 - i]]);
		weeks.push(week);
		rot = [rot[rot.length - 1], ...rot.slice(0, -1)];
	}
	return weeks;
}

/** Played games for weeks 1..k where team t scores `level(t)` every week. */
function season(k: number, level: (team: number, week: number) => number): PlayedGame[] {
	const rr = roundRobin(TEAMS);
	const games: PlayedGame[] = [];
	for (let w = 1; w <= k; w++) {
		for (const [a, b] of rr[(w - 1) % 9]) {
			games.push({ week: w, teamA: a, teamB: b, scoreA: level(a, w), scoreB: level(b, w) });
		}
	}
	return games;
}

const pairs = (week: [number, number][]) =>
	week.map(([a, b]) => (a < b ? `${a}-${b}` : `${b}-${a}`)).sort();

describe('buildSchedule', () => {
	it('repeats the first cycle after every team has met', () => {
		const games = season(9, () => 100);
		const sch = buildSchedule(TEAMS, games, 14, rng(1));
		for (let w = 10; w <= 14; w++) expect(pairs(sch.get(w)!)).toEqual(pairs(sch.get(w - 9)!));
	});

	it('fills unplayed first-cycle weeks with pairs that have not met', () => {
		const games = season(5, () => 100);
		const sch = buildSchedule(TEAMS, games, 14, rng(7));
		const seen = new Set<string>();
		for (let w = 1; w <= 9; w++) {
			const week = sch.get(w)!;
			// A full week: every team exactly once.
			expect(week.flat().sort((a, b) => a - b)).toEqual(TEAMS);
			for (const p of pairs(week)) {
				expect(seen.has(p)).toBe(false);
				seen.add(p);
			}
		}
		expect(seen.size).toBe(45);
	});

	it('copies a later-cycle week back onto its unplayed first-cycle twin', () => {
		// Only week 10 known: week 1 must be the same pairings.
		const rr = roundRobin(TEAMS);
		const games = rr[0].map(([a, b]) => ({ week: 10, teamA: a, teamB: b, scoreA: 1, scoreB: 0 }));
		const sch = buildSchedule(TEAMS, games, 14, rng(3));
		expect(pairs(sch.get(1)!)).toEqual(pairs(rr[0]));
	});
});

describe('simulatePlayoffOdds', () => {
	it('gives every team the same odds before a game is played', () => {
		const res = simulatePlayoffOdds({
			teamKeys: TEAMS,
			games: [],
			regularSeasonWeeks: 14,
			sims: 4000
		});
		for (const t of res.teams) expect(t.playoffOdds).toBeCloseTo(0.6, 1);
		const total = res.teams.reduce((s, t) => s + t.playoffOdds, 0);
		expect(total).toBeCloseTo(6, 6);
	});

	it('hands out exactly six playoff spots, two byes, one title', () => {
		const games = season(8, (t, w) => 90 + t * 3 + ((t * w) % 7) * 4);
		const res = simulatePlayoffOdds({ teamKeys: TEAMS, games, regularSeasonWeeks: 14, sims: 3000 });
		const sum = (f: (t: (typeof res.teams)[number]) => number) =>
			res.teams.reduce((s, t) => s + f(t), 0);
		expect(sum((t) => t.playoffOdds)).toBeCloseTo(6, 1);
		expect(sum((t) => t.byeOdds)).toBeCloseTo(2, 6);
		expect(sum((t) => t.topSeedOdds)).toBeCloseTo(1, 6);
		expect(sum((t) => t.titleOdds)).toBeCloseTo(1, 6);
	});

	it('rates a lucky 6-0 below a 6-0 built on points', () => {
		// Team 1 edges its opponent by half a point every week. Same schedule,
		// same 6-0 record, once on average scoring and once on 125 a week.
		const rr = roundRobin(TEAMS);
		const build = (oneScores: (opp: number) => number) => {
			const games: PlayedGame[] = [];
			for (let w = 1; w <= 6; w++) {
				for (const [a, b] of rr[w - 1]) {
					const base = (t: number) => 95 + ((t * 7 + w * 3) % 10);
					let sa = base(a);
					let sb = base(b);
					if (a === 1) sa = oneScores(sb);
					if (b === 1) sb = oneScores(sa);
					games.push({ week: w, teamA: a, teamB: b, scoreA: sa, scoreB: sb });
				}
			}
			return simulatePlayoffOdds({ teamKeys: TEAMS, games, regularSeasonWeeks: 14, sims: 6000 });
		};
		const lucky = build((opp) => opp + 0.5).teams.find((t) => t.teamKey === 1)!;
		const good = build(() => 125).teams.find((t) => t.teamKey === 1)!;
		expect(lucky.wins).toBe(6);
		expect(good.wins).toBe(6);
		expect(lucky.luck).toBeGreaterThan(1.5);
		expect(good.luck).toBeLessThan(0.5);
		expect(lucky.projectedWins).toBeLessThan(good.projectedWins - 1);
		expect(lucky.playoffOdds).toBeLessThan(good.playoffOdds);
		expect(lucky.titleOdds).toBeLessThan(good.titleOdds / 2);
	});

	it('flags clinched and eliminated only when every remaining outcome agrees', () => {
		// 13 of 14 weeks played with bunched scores, so the bubble is live.
		const games = season(13, (t, w) => 100 + ((t * 7 + w * 11) % 13) * 3);
		const res = simulatePlayoffOdds({ teamKeys: TEAMS, games, regularSeasonWeeks: 14, sims: 2000 });
		const wins = new Map(res.teams.map((t) => [t.teamKey, t.wins + 0.5 * t.ties]));
		const lastWeek = buildSchedule(TEAMS, games, 14, rng(1)).get(14)!;

		// Every one of the 2^5 ways the last week can go.
		const outcomes: Map<number, number>[] = [];
		for (let mask = 0; mask < 1 << lastWeek.length; mask++) {
			const w = new Map(wins);
			lastWeek.forEach(([a, b], i) => {
				const winner = mask & (1 << i) ? a : b;
				w.set(winner, w.get(winner)! + 1);
			});
			outcomes.push(w);
		}
		let flagged = 0;
		for (const t of res.teams) {
			const k = t.teamKey;
			// Rivals at or above this team on wins; a tie could go either way on points.
			const atOrAbove = (w: Map<number, number>) =>
				TEAMS.filter((j) => j !== k && w.get(j)! >= w.get(k)!).length;
			const strictlyAbove = (w: Map<number, number>) =>
				TEAMS.filter((j) => j !== k && w.get(j)! > w.get(k)!).length;
			if (t.clinched) {
				flagged++;
				expect(t.playoffOdds).toBe(1);
				for (const w of outcomes) expect(atOrAbove(w)).toBeLessThan(6);
			}
			if (t.eliminated) {
				flagged++;
				expect(t.playoffOdds).toBe(0);
				for (const w of outcomes) expect(strictlyAbove(w)).toBeGreaterThanOrEqual(6);
			}
			expect(t.clinched && t.eliminated).toBe(false);
		}
		// The scenario should exercise the flags, not pass vacuously.
		expect(flagged).toBeGreaterThan(0);
		expect(flagged).toBeLessThan(10);
	});

	it('is deterministic for a given seed', () => {
		const games = season(5, (t, w) => 100 + ((t * 13 + w * 5) % 20));
		const a = simulatePlayoffOdds({
			teamKeys: TEAMS,
			games,
			regularSeasonWeeks: 14,
			sims: 1000,
			seed: 9
		});
		const b = simulatePlayoffOdds({
			teamKeys: TEAMS,
			games,
			regularSeasonWeeks: 14,
			sims: 1000,
			seed: 9
		});
		expect(a.teams).toEqual(b.teams);
	});
});

describe('unknownCycleWeeks', () => {
	it('treats a played rematch week as knowing its first-cycle twin', () => {
		// Ten teams: weeks 10-14 are rematches of 1-5.
		expect(unknownCycleWeeks(10, 14, [1, 2, 3])).toEqual([4, 5, 6, 7, 8, 9]);
		expect(unknownCycleWeeks(10, 14, [10, 11])).toEqual([3, 4, 5, 6, 7, 8, 9]);
		expect(unknownCycleWeeks(10, 14, [1, 2, 3, 4, 5, 6, 7, 8, 9])).toEqual([]);
	});

	it('only asks for weeks the season actually has', () => {
		expect(unknownCycleWeeks(10, 6, [1, 2])).toEqual([3, 4, 5, 6]);
	});
});

describe('gamesFingerprint', () => {
	const games = season(4, (t, w) => 100 + t + w);

	it('ignores game order', () => {
		expect(gamesFingerprint([...games].reverse())).toBe(gamesFingerprint(games));
	});

	it('changes when a single score is corrected', () => {
		const corrected = games.map((g, i) => (i === 7 ? { ...g, scoreA: g.scoreA + 0.1 } : g));
		expect(gamesFingerprint(corrected)).not.toBe(gamesFingerprint(games));
	});

	it('changes when a game is added', () => {
		expect(gamesFingerprint(games.slice(1))).not.toBe(gamesFingerprint(games));
	});
});

describe('bracket', () => {
	it('keeps the top seeds apart until the final', () => {
		expect(bracketSlots(2)).toEqual([1, 2]);
		expect(bracketSlots(4)).toEqual([1, 4, 2, 3]);
		// Six teams in an eight-slot bracket: 1 and 2 face byes (7, 8), then the
		// 4/5 and 3/6 winners. That's the league's real bracket.
		expect(bracketSlots(8)).toEqual([1, 8, 4, 5, 2, 7, 3, 6]);
	});

	it.each([
		[4, 0],
		[6, 2],
		[8, 0]
	])('simulates a %i-team field with %i byes', (playoffTeams, byes) => {
		const games = season(8, (t, w) => 90 + t * 3 + ((t * w) % 7) * 4);
		const res = simulatePlayoffOdds({
			teamKeys: TEAMS,
			games,
			regularSeasonWeeks: 14,
			playoffTeams,
			sims: 3000
		});
		const sum = (f: (t: (typeof res.teams)[number]) => number) =>
			res.teams.reduce((s, t) => s + f(t), 0);
		expect(res.playoffTeams).toBe(playoffTeams);
		expect(res.byes).toBe(byes);
		expect(sum((t) => t.playoffOdds)).toBeCloseTo(playoffTeams, 1);
		expect(sum((t) => t.byeOdds)).toBeCloseTo(byes, 6);
		expect(sum((t) => t.titleOdds)).toBeCloseTo(1, 6);
		// Only playoff teams can win it.
		for (const t of res.teams) expect(t.titleOdds).toBeLessThanOrEqual(t.playoffOdds + 1e-9);
	});
});
