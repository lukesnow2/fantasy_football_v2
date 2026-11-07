/**
 * What a draft pick was expected to score, and how far it beat or missed that.
 *
 * Expectation comes from this league's own history: what players at the same
 * position, taken around the same round, have scored in finished seasons. Two
 * things in the data force that shape:
 *
 *  - Position. QBs out-score everyone at every round (about 2.7x an average
 *    pick in round 1, still ~1x in round 15), so one curve for all positions
 *    makes every QB a steal and none a bust.
 *  - Round, not raw points. Kickers and defenses go in the last rounds and score
 *    like the other late picks (0.5-0.9x average). Judged against their slot they
 *    are expected to score little, so a dropped one is no "bust"; a kicker
 *    reached for in round 5 carries a round-5 expectation and can be.
 *
 * Points are measured relative to each season's average drafted pick, so seasons
 * with different scoring, and the season still being played (partial totals),
 * are judged on the same scale. A position with few picks near a round (a kicker
 * in round 5) is pulled toward that round's overall average.
 *
 * `seasonPoints` counts only the weeks a player was on a roster in this league,
 * so a pick that was cut early has a small total. For an early pick that is
 * exactly a bust; for a late flier it is expected.
 */

export interface DraftPickInput {
	seasonYear: number;
	roundNumber: number;
	position: string | null;
	seasonPoints: number;
}

export interface PickValue {
	/** Points a pick at this position and round was expected to score that season. */
	expectedPoints: number;
	/** seasonPoints - expectedPoints. Positive beat the slot, negative missed it. */
	valueOverExpected: number;
	/**
	 * A pick the draft counted on: taken in the first MAX_BUST_ROUND rounds and
	 * expected to out-score an average pick. Only these can be busts.
	 */
	highExpectation: boolean;
	/** Expectation as a multiple of the season's average pick. */
	expectedRelative: number;
	/** Points scored by the season's average drafted pick (the unit for draft scores). */
	seasonAverage: number;
}

/**
 * Busts come from the first eight rounds only. Expectation alone let QBs
 * through from much later (QBs score so much here that even a round-11 QB is
 * expected to beat an average pick), and a round-11 flier is not a pick the
 * draft counted on: 2019's Andrew Luck (R11, 0 points) ranked #3 all-time.
 */
export const MAX_BUST_ROUND = 8;

export interface DraftValueOptions {
	/** Rounds either side pooled into a round's average (default 1). */
	roundWindow?: number;
	/** Picks' worth of weight pulling a position toward the round average (default 10). */
	prior?: number;
}

/**
 * Values every pick in `picks` (same order). Expectations are fitted on the
 * picks whose season is in `completedSeasons`, then applied to all of them.
 */
export function valueDraftPicks<T extends DraftPickInput>(
	picks: T[],
	completedSeasons: Set<number>,
	options: DraftValueOptions = {}
): (T & PickValue)[] {
	const window = options.roundWindow ?? 1;
	const prior = options.prior ?? 10;

	const seasonMean = new Map<number, number>();
	{
		const acc = new Map<number, { sum: number; n: number }>();
		for (const p of picks) {
			const a = acc.get(p.seasonYear) ?? { sum: 0, n: 0 };
			a.sum += p.seasonPoints;
			a.n += 1;
			acc.set(p.seasonYear, a);
		}
		for (const [s, a] of acc) seasonMean.set(s, a.n ? a.sum / a.n : 0);
	}

	const round = new Map<number, { sum: number; n: number }>();
	const posRound = new Map<string, { sum: number; n: number }>();
	const add = (m: Map<any, { sum: number; n: number }>, k: any, v: number) => {
		const a = m.get(k) ?? { sum: 0, n: 0 };
		a.sum += v;
		a.n += 1;
		m.set(k, a);
	};
	for (const p of picks) {
		const mean = seasonMean.get(p.seasonYear) ?? 0;
		if (!completedSeasons.has(p.seasonYear) || mean <= 0) continue;
		const rel = p.seasonPoints / mean;
		for (let r = p.roundNumber - window; r <= p.roundNumber + window; r++) {
			add(round, r, rel);
			add(posRound, `${p.position ?? '?'}|${r}`, rel);
		}
	}

	const fitted = [...round.keys()].filter((r) => round.get(r)!.n > 0).sort((a, b) => a - b);
	const roundAverage = (r: number): number => {
		if (fitted.length === 0) return 1;
		// A round beyond any fitted one (a longer draft) takes the nearest.
		const nearest = fitted.reduce((best, k) => (Math.abs(k - r) < Math.abs(best - r) ? k : best));
		const a = round.get(round.get(r)?.n ? r : nearest)!;
		return a.sum / a.n;
	};

	return picks.map((p) => {
		const base = roundAverage(p.roundNumber);
		const pos = posRound.get(`${p.position ?? '?'}|${p.roundNumber}`) ?? { sum: 0, n: 0 };
		const expectedRelative = (pos.sum + prior * base) / (pos.n + prior);
		const expectedPoints = expectedRelative * (seasonMean.get(p.seasonYear) ?? 0);
		return {
			...p,
			expectedPoints,
			valueOverExpected: p.seasonPoints - expectedPoints,
			highExpectation: expectedRelative >= 1 && p.roundNumber <= MAX_BUST_ROUND,
			expectedRelative,
			seasonAverage: seasonMean.get(p.seasonYear) ?? 0
		};
	});
}

/** Steals: the picks that beat their expectation by the most. */
export function topSteals<T extends PickValue>(picks: T[], n: number): T[] {
	return [...picks].sort((a, b) => b.valueOverExpected - a.valueOverExpected).slice(0, n);
}

/** Busts: picks the draft counted on (highExpectation) that missed by the most. */
export function topBusts<T extends PickValue>(picks: T[], n: number): T[] {
	return picks
		.filter((p) => p.highExpectation)
		.sort((a, b) => a.valueOverExpected - b.valueOverExpected)
		.slice(0, n);
}

/**
 * One reading of a pick against its expectation, shared by the draft board's
 * icons and grades and the draft page's steal/bust/hit counts so they can't
 * disagree. Ratios are points / expected points. A bust needs highExpectation:
 * a pick the draft didn't count on that missed is only a disappointment.
 */
export const STEAL_RATIO = 1.5;
export const GOOD_RATIO = 1.15;
export const FAIR_RATIO = 0.85;
export const HIT_RATIO = 0.8;
export const BUST_RATIO = 0.5;

export type PickVerdict = 'steal' | 'good' | 'fair' | 'disappointing' | 'bust';

function ratio(points: number, expected: number | null | undefined): number | null {
	return expected != null && expected > 0 ? points / expected : null;
}

/** Null when the pick has no expectation to judge against. */
export function pickVerdict(
	points: number,
	expected: number | null | undefined,
	highExpectation: boolean
): PickVerdict | null {
	const r = ratio(points, expected);
	if (r == null) return null;
	if (r > STEAL_RATIO) return 'steal';
	if (r > GOOD_RATIO) return 'good';
	if (r > FAIR_RATIO) return 'fair';
	if (r < BUST_RATIO && highExpectation) return 'bust';
	return 'disappointing';
}

/** Scored at least HIT_RATIO of what the slot was expected to. */
export function isHit(points: number, expected: number | null | undefined): boolean {
	const r = ratio(points, expected);
	return r != null && r >= HIT_RATIO;
}

/** Letter grade against expectation. F is kept for busts; anything else bottoms out at D. */
export function pickGrade(
	points: number,
	expected: number | null | undefined,
	highExpectation: boolean
): string | null {
	const r = ratio(points, expected);
	if (r == null) return null;
	if (r >= 1.6) return 'A+';
	if (r >= 1.3) return 'A';
	if (r >= 1.1) return 'B+';
	if (r >= 0.95) return 'B';
	if (r >= 0.8) return 'C+';
	if (r >= 0.65) return 'C';
	return pickVerdict(points, expected, highExpectation) === 'bust' ? 'F' : 'D';
}

// ---- Manager draft grades ---------------------------------------------------
//
// A draft's score is how much its picks beat (or missed) what their positions and
// rounds were expected to score, summed over every pick a manager made that
// season, in units of the season's average pick: +2.4 means the draft produced
// about two and a half average picks more than its slots promised. Drafting from
// #1 or #10 doesn't matter: each pick is judged against its own slot.
//
// Checked on this league (198 manager-seasons, 2005-2025): a season's draft score
// correlates with that season's wins (r = 0.53), so it measures something that
// matters; year to year it barely persists (r = 0.19), so one draft says little
// about the next. That shapes the two grades:
//
//  - Season grade: where the score falls among every finished manager-season in
//    league history (A top 15%, B next 25%, C middle 30%, D next 20%, F bottom
//    10%). Not curved within the season, so a year when everyone drafted well
//    isn't forced into Cs.
//  - Career grade: how strong the evidence is that a manager drafts above or
//    below average: their average season score divided by its standard error
//    (season spread / sqrt(seasons)). A at 2+ standard errors above, B at 0.75+,
//    C within 0.75 either way, D and F below. Most managers land on C, which is
//    the honest result when drafting skill is mostly noise.

export interface ScoredPick extends PickValue {
	managerName: string;
	seasonYear: number;
}

export interface SeasonDraftScore {
	managerName: string;
	seasonYear: number;
	/** Points over expectation across the draft, in average picks. */
	score: number;
	picks: number;
}

export function seasonDraftScores(picks: ScoredPick[]): SeasonDraftScore[] {
	const acc = new Map<string, SeasonDraftScore>();
	for (const p of picks) {
		if (!(p.seasonAverage > 0)) continue;
		const key = `${p.managerName}|${p.seasonYear}`;
		const a = acc.get(key) ?? {
			managerName: p.managerName,
			seasonYear: p.seasonYear,
			score: 0,
			picks: 0
		};
		a.score += p.valueOverExpected / p.seasonAverage;
		a.picks += 1;
		acc.set(key, a);
	}
	return [...acc.values()];
}

export const SEASON_GRADE_PERCENTILES: [string, number][] = [
	['A', 0.85],
	['B', 0.6],
	['C', 0.3],
	['D', 0.1]
];

/** Letter and percentile of a season score among finished manager-seasons (`history`). */
export function seasonGrade(
	score: number,
	history: number[]
): { grade: string; percentile: number } {
	if (history.length === 0) return { grade: 'C', percentile: 0.5 };
	const percentile = history.filter((h) => h < score).length / history.length;
	const grade = SEASON_GRADE_PERCENTILES.find(([, cut]) => percentile >= cut)?.[0] ?? 'F';
	return { grade, percentile };
}

/** Standard errors from average: A at +2 or more, B at +0.75, C within 0.75, D to -2, F beyond. */
function careerLetter(z: number): string {
	if (z >= 2) return 'A';
	if (z >= 0.75) return 'B';
	if (z > -0.75) return 'C';
	if (z > -2) return 'D';
	return 'F';
}

/**
 * Career grade from a manager's season scores. `spread` is the standard
 * deviation of season scores across all finished manager-seasons.
 */
export function careerGrade(
	seasonScores: number[],
	spread: number
): { grade: string; average: number; z: number; seasons: number } {
	const seasons = seasonScores.length;
	const average = seasons ? seasonScores.reduce((a, b) => a + b, 0) / seasons : 0;
	const z = seasons && spread > 0 ? average / (spread / Math.sqrt(seasons)) : 0;
	return { grade: careerLetter(z), average, z, seasons };
}

/** Population standard deviation. */
export function spreadOf(values: number[]): number {
	if (values.length === 0) return 0;
	const mean = values.reduce((a, b) => a + b, 0) / values.length;
	return Math.sqrt(values.reduce((a, b) => a + (b - mean) ** 2, 0) / values.length);
}
