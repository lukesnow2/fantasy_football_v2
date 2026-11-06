import { describe, expect, it } from 'vitest';
import { summarizeLuck, type SeasonLuck } from './manager-luck';

function season(seasonYear: number, over: Partial<SeasonLuck> = {}): SeasonLuck {
	return {
		seasonYear,
		teamKey: seasonYear,
		weekNumber: 14,
		wins: 8,
		ties: 0,
		pythagoreanWins: 7,
		luckFactor: 1,
		playoffOdds: 0.9,
		inProgress: false,
		...over
	};
}

describe('summarizeLuck', () => {
	it('uses the season being played, with its playoff odds', () => {
		const live = season(2026, { inProgress: true, weekNumber: 9, wins: 4, playoffOdds: 0.38 });
		const s = summarizeLuck([season(2025), live], 2026);
		expect(s.currentSeason).toMatchObject({
			seasonYear: 2026,
			throughWeek: 9,
			final: false,
			regularSeasonWins: 4,
			playoffOdds: 0.38
		});
	});

	it('between seasons, shows the season that just ended as final', () => {
		const s = summarizeLuck([season(2024), season(2025, { wins: 10, luckFactor: 2.5 })], 2025);
		expect(s.currentSeason).toMatchObject({
			seasonYear: 2025,
			final: true,
			regularSeasonWins: 10,
			luckFactor: 2.5,
			playoffOdds: null
		});
	});

	it('a manager who left the league gets no current season, not an old one', () => {
		const s = summarizeLuck([season(2016), season(2018)], 2025);
		expect(s.currentSeason).toBeNull();
		expect(s.latestSeason).toBe(2025);
	});

	it('a manager sitting out a season in progress gets no current season', () => {
		const s = summarizeLuck([season(2025)], 2026);
		expect(s.currentSeason).toBeNull();
	});

	it('adds up two teams in one season and gives no single playoff odds', () => {
		const a = season(2026, {
			teamKey: 1,
			inProgress: true,
			wins: 3,
			pythagoreanWins: 2.5,
			luckFactor: 0.5
		});
		const b = season(2026, {
			teamKey: 2,
			inProgress: true,
			wins: 1,
			ties: 1,
			pythagoreanWins: 2,
			luckFactor: -0.5
		});
		expect(summarizeLuck([a, b], 2026).currentSeason).toMatchObject({
			regularSeasonWins: 4.5,
			pythagoreanWins: 4.5,
			luckFactor: 0,
			playoffOdds: null
		});
	});

	it('counts career seasons by year, not by team', () => {
		const s = summarizeLuck(
			[season(2007, { teamKey: 1 }), season(2007, { teamKey: 2 }), season(2008)],
			2008
		);
		expect(s.luckSeasons).toBe(2);
		expect(s.careerLuckFactor).toBe(3);
	});
});
