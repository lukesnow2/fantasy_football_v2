/**
 * Grading saved playoff-odds forecasts (model.playoff_odds_snapshots) against
 * what happened. Pure, so the scoring is tested apart from the database;
 * scripts/grade-playoff-odds.ts loads the rows and prints the report.
 *
 * Every forecast is one team in one week: a probability for each of four
 * outcomes (made the playoffs, earned a bye, was the top seed, won the
 * title). Each is scored with the Brier score (mean squared error, lower is
 * better, 0.25 for a coin flip) and log loss, and against a no-skill baseline
 * that gives every team the same chance (six playoff spots in ten teams is
 * 60% each). Skill is 1 - Brier / baseline Brier: 0 is no better than that,
 * 1 is perfect.
 */

import { compareSeeding } from './playoff-odds';

export const OUTCOMES = ['playoffs', 'bye', 'topSeed', 'title'] as const;
export type Outcome = (typeof OUTCOMES)[number];

export interface Forecast {
	season: number;
	throughWeek: number;
	regularSeasonWeeks: number;
	teams: number;
	playoffTeams: number;
	byes: number;
	teamId: string;
	p: Record<Outcome, number>;
}

/** What happened in one season, by Yahoo team id. Null until it's known. */
export interface SeasonResult {
	playoffs: Set<string> | null;
	bye: Set<string> | null;
	topSeed: string | null;
	champion: string | null;
}

export interface SeasonGame {
	week: number;
	teamA: string;
	teamB: string;
	scoreA: number;
	scoreB: number;
	isPlayoffs: boolean;
	isConsolation: boolean;
	isChampionship: boolean;
	winner: string | null;
}

/**
 * A season's results from its matchups. The playoff field is every team in a
 * championship-bracket game; byes are the field's teams that sat out its first
 * week; the champion won the title game. The top seed comes from the
 * regular-season standings (wins, then points for), the rule that seeds the
 * real bracket. Nothing counts until the playoffs have started.
 */
export function seasonResult(games: SeasonGame[], regularSeasonWeeks: number): SeasonResult {
	const bracket = games.filter((g) => g.isPlayoffs && !g.isConsolation);
	if (bracket.length === 0) return { playoffs: null, bye: null, topSeed: null, champion: null };

	const playoffs = new Set(bracket.flatMap((g) => [g.teamA, g.teamB]));
	const firstWeek = Math.min(...bracket.map((g) => g.week));
	const playedFirst = new Set(
		bracket.filter((g) => g.week === firstWeek).flatMap((g) => [g.teamA, g.teamB])
	);
	const bye = new Set([...playoffs].filter((t) => !playedFirst.has(t)));

	const table = new Map<string, { wins: number; ties: number; pointsFor: number }>();
	const row = (t: string) => {
		if (!table.has(t)) table.set(t, { wins: 0, ties: 0, pointsFor: 0 });
		return table.get(t)!;
	};
	for (const g of games) {
		if (g.isPlayoffs || g.isConsolation || g.week > regularSeasonWeeks) continue;
		const a = row(g.teamA);
		const b = row(g.teamB);
		a.pointsFor += g.scoreA;
		b.pointsFor += g.scoreB;
		if (g.scoreA > g.scoreB) a.wins++;
		else if (g.scoreB > g.scoreA) b.wins++;
		else {
			a.ties++;
			b.ties++;
		}
	}
	const standings = [...table].sort((x, y) => compareSeeding(x[1], y[1]));

	const title = bracket.find((g) => g.isChampionship && g.winner != null);
	return {
		playoffs,
		bye,
		topSeed: standings[0]?.[0] ?? null,
		champion: title?.winner ?? null
	};
}

function happened(result: SeasonResult, outcome: Outcome, teamId: string): 0 | 1 | null {
	switch (outcome) {
		case 'playoffs':
			return result.playoffs ? (result.playoffs.has(teamId) ? 1 : 0) : null;
		case 'bye':
			return result.bye ? (result.bye.has(teamId) ? 1 : 0) : null;
		case 'topSeed':
			return result.topSeed == null ? null : result.topSeed === teamId ? 1 : 0;
		case 'title':
			return result.champion == null ? null : result.champion === teamId ? 1 : 0;
	}
}

/** The chance a team would have if every team were equal. */
function baseline(f: Forecast, outcome: Outcome): number {
	const slots = { playoffs: f.playoffTeams, bye: f.byes, topSeed: 1, title: 1 }[outcome];
	return slots / f.teams;
}

export interface Scored {
	forecast: Forecast;
	outcome: Outcome;
	p: number;
	base: number;
	y: 0 | 1;
}

/** Each forecast paired with its result, for the outcomes that are known. */
export function scoreForecasts(
	forecasts: Forecast[],
	results: Map<number, SeasonResult>
): Scored[] {
	const out: Scored[] = [];
	for (const f of forecasts) {
		const r = results.get(f.season);
		if (!r) continue;
		// Once the regular season is over the standings are final, so only the
		// title is still a forecast; scoring the rest would flatter the model.
		const standingsFinal = f.throughWeek >= f.regularSeasonWeeks;
		for (const outcome of OUTCOMES) {
			if (standingsFinal && outcome !== 'title') continue;
			// A bracket with no byes has nothing to forecast.
			if (outcome === 'bye' && f.byes === 0) continue;
			const y = happened(r, outcome, f.teamId);
			if (y == null) continue;
			out.push({ forecast: f, outcome, p: f.p[outcome], base: baseline(f, outcome), y });
		}
	}
	return out;
}

export interface Grade {
	n: number;
	brier: number;
	logLoss: number;
	baselineBrier: number;
	/** 1 - brier / baselineBrier. */
	skill: number;
}

const EPS = 0.005;
const clamp = (p: number) => Math.min(Math.max(p, EPS), 1 - EPS);

/**
 * Brier score, log loss and skill. Log loss clamps probabilities to
 * [0.5%, 99.5%]: 10,000 simulations report 0% for a team that is merely
 * unlikely, and one such miss shouldn't read as infinitely wrong.
 */
export function grade(scored: Scored[]): Grade | null {
	if (scored.length === 0) return null;
	let brier = 0;
	let logLoss = 0;
	let base = 0;
	for (const s of scored) {
		brier += (s.p - s.y) ** 2;
		base += (s.base - s.y) ** 2;
		logLoss -= s.y ? Math.log(clamp(s.p)) : Math.log(1 - clamp(s.p));
	}
	const n = scored.length;
	return {
		n,
		brier: brier / n,
		logLoss: logLoss / n,
		baselineBrier: base / n,
		skill: base > 0 ? 1 - brier / base : 0
	};
}

export interface CalibrationBin {
	from: number;
	to: number;
	n: number;
	meanForecast: number;
	observed: number;
}

/** Forecasts bucketed by probability: in a calibrated model the two match. */
export function calibration(
	scored: Scored[],
	edges: number[] = [0, 0.05, 0.15, 0.3, 0.5, 0.7, 0.85, 0.95, 1]
): CalibrationBin[] {
	const bins: CalibrationBin[] = [];
	for (let i = 0; i < edges.length - 1; i++) {
		const last = i === edges.length - 2;
		const inBin = scored.filter(
			(s) => s.p >= edges[i] && (last ? s.p <= edges[i + 1] : s.p < edges[i + 1])
		);
		if (inBin.length === 0) continue;
		bins.push({
			from: edges[i],
			to: edges[i + 1],
			n: inBin.length,
			meanForecast: inBin.reduce((t, s) => t + s.p, 0) / inBin.length,
			observed: inBin.reduce((t, s) => t + s.y, 0) / inBin.length
		});
	}
	return bins;
}
