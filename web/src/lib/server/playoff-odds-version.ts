import { createHash } from 'node:crypto';
import { simulatePlayoffOdds, type ModelParams, type PlayedGame } from '$lib/playoff-odds';

/**
 * The playoff-odds model's version, for the saved forecasts
 * (model.playoff_odds_snapshots): a hash of what the model does, not of its
 * source. It runs the model on three fixed sample seasons, with the
 * parameters the site uses, and hashes every team's odds and projections,
 * along with the site's simulation count and seed rule. Refactors, comments,
 * renames and exports leave it alone; anything that changes a number starts a
 * new version.
 *
 * Not covered: how getSeasonOdds picks the games it passes in (regular season
 * only, finished games only). A change there is a change to the inputs, which
 * each snapshot's games fingerprint records.
 */

export interface SiteSettings {
	sims: number;
	seed: (season: number, playedWeeks: number) => number;
	params: Readonly<ModelParams>;
}

type Simulate = typeof simulatePlayoffOdds;

/** Circle-method round robin over `teams`, as many weeks as asked. */
function schedule(teams: number, weeks: number): [number, number][][] {
	const rest = Array.from({ length: teams - 1 }, (_, i) => i + 2);
	const out: [number, number][][] = [];
	for (let w = 0; w < weeks; w++) {
		const line = [1, ...rest];
		const week: [number, number][] = [];
		for (let i = 0; i < teams / 2; i++) week.push([line[i], line[teams - 1 - i]]);
		out.push(week);
		rest.unshift(rest.pop()!);
	}
	return out;
}

/** Fixed, uneven scores: some teams better than others, some weeks noisier. */
function season(teams: number, played: number, tieWeek?: number): PlayedGame[] {
	const score = (team: number, week: number) =>
		90 + ((team * 37 + week * 53) % 41) + (team % 3) * 6;
	return schedule(teams, played).flatMap((pairs, i) =>
		pairs.map(([a, b]) => {
			const week = i + 1;
			const scoreA = score(a, week);
			return {
				week,
				teamA: a,
				teamB: b,
				scoreA,
				scoreB: week === tieWeek && a === 1 ? scoreA : score(b, week)
			};
		})
	);
}

const keys = (n: number) => Array.from({ length: n }, (_, i) => i + 1);

/** Mid-season, opening week, and a small bracket with a tie on the books. */
const FIXTURES = [
	{ teamKeys: keys(10), games: season(10, 6), regularSeasonWeeks: 14, playoffTeams: 6 },
	{ teamKeys: keys(10), games: season(10, 1), regularSeasonWeeks: 13, playoffTeams: 6 },
	{ teamKeys: keys(8), games: season(8, 9, 4), regularSeasonWeeks: 13, playoffTeams: 4 }
];
const FIXTURE_SIMS = 2000;

const r6 = (x: number) => Math.round(x * 1e6) / 1e6;

export function modelVersion(
	settings: SiteSettings,
	simulate: Simulate = simulatePlayoffOdds
): string {
	const runs = FIXTURES.map((f, i) => {
		const res = simulate({ ...f, sims: FIXTURE_SIMS, seed: 1000 + i, params: settings.params });
		return {
			byes: res.byes,
			leagueAverage: r6(res.leagueAverage),
			teams: res.teams.map((t) => [
				t.teamKey,
				t.playoffOdds,
				t.byeOdds,
				t.topSeedOdds,
				t.titleOdds,
				r6(t.projectedPoints),
				r6(t.projectedWins),
				t.clinched,
				t.eliminated
			])
		};
	});
	const site = {
		sims: settings.sims,
		seeds: [settings.seed(2005, 0), settings.seed(2026, 9), settings.seed(2030, 14)]
	};
	return createHash('sha256').update(JSON.stringify({ runs, site })).digest('hex').slice(0, 12);
}
