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
 *    like the other late picks (0.5-0.9x average), so their slot expects little
 *    of them. They still get an expectation and a grade, but never a verdict:
 *    a kicker or defense is never a steal or a bust, wherever it was taken
 *    (NO_VERDICT_POSITIONS). A late kicker that hit is no heist, and one reached
 *    for early is no draft disaster.
 *
 * Points are measured relative to each season's average drafted pick, so seasons
 * with different scoring, and the season still being played (partial totals),
 * are judged on the same scale. A position with few picks near a round (a kicker
 * in round 5) is pulled toward that round's overall average.
 *
 * Ranking steals and busts needs one more step. Positions don't just score
 * differently, they swing differently: a late QB is either a backup who scores
 * next to nothing or a starter who scores 350, so late QBs land about 0.9 of an
 * average pick either side of their expectation, where WRs land about 0.5 either
 * side. Ranked on raw points over expectation, a QB who hit beats an equally
 * rare WR hit by nearly double, and QBs were 13% of skill picks but half the
 * all-time top 50 steals. `valueZ` divides the gap by how widely picks at that
 * position and round usually land, so a pick ranks by how much it stood out
 * from the players taken like it (2019 Lamar Jackson, QB12 taken and QB1
 * finished from round 13, still ranks near the top; a merely good late QB
 * does not). The lists rank on valueZ; the points shown stay points.
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
	 * valueOverExpected in units of how widely picks at this position and round
	 * usually land (their standard deviation): +2 stood out about as much as
	 * the best pick in forty taken like it. What steal and bust lists rank on.
	 */
	valueZ: number;
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

	// The unit is the average pick in the first `common` rounds, the length of
	// the shortest finished draft. The 2005-07 drafts ran 24-25 rounds;
	// averaged over all of them, their unit came out 15-20% low and every pick
	// from those seasons read that much bigger, enough to crowd the steals list.
	// Finished seasons only, so a draft loaded partway (a live draft) can't
	// shrink every season's unit; with none finished, every season counts.
	const lastRound = new Map<number, number>();
	const anyDone = picks.some((p) => completedSeasons.has(p.seasonYear));
	for (const p of picks) {
		if (anyDone && !completedSeasons.has(p.seasonYear)) continue;
		lastRound.set(p.seasonYear, Math.max(lastRound.get(p.seasonYear) ?? 0, p.roundNumber));
	}
	const common = lastRound.size ? Math.min(...lastRound.values()) : 0;
	const seasonMean = new Map<number, number>();
	{
		const acc = new Map<number, { sum: number; n: number }>();
		for (const p of picks) {
			if (p.roundNumber > common) continue;
			const a = acc.get(p.seasonYear) ?? { sum: 0, n: 0 };
			a.sum += p.seasonPoints;
			a.n += 1;
			acc.set(p.seasonYear, a);
		}
		for (const [s, a] of acc) seasonMean.set(s, a.n ? a.sum / a.n : 0);
	}

	type Acc = { sum: number; sq: number; n: number };
	const round = new Map<number, Acc>();
	const posRound = new Map<string, Acc>();
	const add = (m: Map<any, Acc>, k: any, v: number) => {
		const a = m.get(k) ?? { sum: 0, sq: 0, n: 0 };
		a.sum += v;
		a.sq += v * v;
		a.n += 1;
		m.set(k, a);
	};
	const variance = (a: Acc) => (a.n ? Math.max(a.sq / a.n - (a.sum / a.n) ** 2, 0) : 0);
	for (const p of picks) {
		const mean = seasonMean.get(p.seasonYear) ?? 0;
		if (!completedSeasons.has(p.seasonYear) || mean <= 0) continue;
		const rel = p.seasonPoints / mean;
		for (let r = p.roundNumber - window; r <= p.roundNumber + window; r++) {
			add(round, r, rel);
			add(posRound, `${p.position ?? '?'}|${r}`, rel);
		}
	}

	// Each round's spread within positions (every position around its own
	// mean, pooled). The round's raw spread would also count the gap between
	// QB and WR averages, and inflate the spread of positions with few picks.
	const within = new Map<number, number>();
	{
		const acc = new Map<number, { ss: number; n: number }>();
		for (const [key, a] of posRound) {
			const r = Number(key.slice(key.lastIndexOf('|') + 1));
			const w = acc.get(r) ?? { ss: 0, n: 0 };
			w.ss += a.n * variance(a);
			w.n += a.n;
			acc.set(r, w);
		}
		for (const [r, w] of acc) within.set(r, w.n ? w.ss / w.n : 0);
	}

	const fitted = [...round.keys()].filter((r) => round.get(r)!.n > 0).sort((a, b) => a - b);
	// A round beyond any fitted one (a longer draft) takes the nearest.
	const roundKey = (r: number): number | null => {
		if (fitted.length === 0) return null;
		if (round.get(r)?.n) return r;
		return fitted.reduce((best, k) => (Math.abs(k - r) < Math.abs(best - r) ? k : best));
	};

	return picks.map((p) => {
		const rk = roundKey(p.roundNumber);
		const r = rk == null ? null : round.get(rk)!;
		const base = r ? r.sum / r.n : 1;
		const pos = posRound.get(`${p.position ?? '?'}|${p.roundNumber}`) ?? { sum: 0, sq: 0, n: 0 };
		const expectedRelative = (pos.sum + prior * base) / (pos.n + prior);
		const mean = seasonMean.get(p.seasonYear) ?? 0;
		const expectedPoints = expectedRelative * mean;
		// The position's spread, pulled toward the round's like the expectation is.
		const spread = Math.sqrt(
			(pos.n * variance(pos) + prior * (rk == null ? 0 : (within.get(rk) ?? 0))) / (pos.n + prior)
		);
		const gap = mean > 0 ? p.seasonPoints / mean - expectedRelative : 0;
		return {
			...p,
			expectedPoints,
			valueOverExpected: p.seasonPoints - expectedPoints,
			valueZ: spread > 0 ? gap / spread : gap,
			highExpectation: expectedRelative >= 1 && p.roundNumber <= MAX_BUST_ROUND,
			expectedRelative,
			seasonAverage: seasonMean.get(p.seasonYear) ?? 0
		};
	});
}

export interface PositionRank {
	/** Order taken among the season's drafted players at the position (QB12 = 12th QB). */
	drafted: number;
	/** Order by points among those same players (QB1 = most points). */
	scored: number;
}

/**
 * Where each pick sat among the season's drafted players at its position:
 * the order it was taken and the order it scored, keyed `season:overallPick`.
 * This is the "relative to the other QBs taken" view: 2019 Lamar Jackson was
 * drafted QB12 and scored QB1. Only drafted players count, and points are the
 * weeks on a roster in this league, so "scored QB1" means first among the
 * league's drafted QBs, not the NFL. Ties in points go to the earlier pick.
 */
export function positionRanks(
	picks: {
		seasonYear: number;
		overallPick: number;
		position: string | null;
		seasonPoints: number;
	}[]
): Map<string, PositionRank> {
	const groups = new Map<string, typeof picks>();
	for (const p of picks) {
		if (!p.position) continue;
		const k = `${p.seasonYear}|${p.position}`;
		const g = groups.get(k);
		if (g) g.push(p);
		else groups.set(k, [p]);
	}
	const out = new Map<string, PositionRank>();
	for (const g of groups.values()) {
		const scoredAt = new Map<(typeof picks)[number], number>();
		[...g]
			.sort((a, b) => b.seasonPoints - a.seasonPoints || a.overallPick - b.overallPick)
			.forEach((p, i) => scoredAt.set(p, i + 1));
		g.sort((a, b) => a.overallPick - b.overallPick).forEach((p, i) =>
			out.set(`${p.seasonYear}:${p.overallPick}`, { drafted: i + 1, scored: scoredAt.get(p)! })
		);
	}
	return out;
}

/**
 * Positions that never get a verdict (steal, value, letdown, bust): kickers and
 * defenses, in every spelling the data and the board use. Their points swing on
 * things a draft can't call, so a verdict on them says nothing about the pick.
 */
export const NO_VERDICT_POSITIONS: ReadonlySet<string> = new Set(['K', 'DEF', 'D/ST', 'DEFENSE']);

/** Whether a pick at this position can be judged a steal or a bust. */
export function getsVerdict(position: string | null | undefined): boolean {
	return !NO_VERDICT_POSITIONS.has(String(position ?? '').trim().toUpperCase());
}

type Positioned = { position?: string | null };

/**
 * Steals: the picks that stood out most from players at their position taken
 * around the same round (valueZ, not raw points, so QBs don't crowd the list
 * just for scoring more). Only picks that beat their slot; never a kicker or
 * defense.
 */
export function topSteals<T extends PickValue & Positioned>(picks: T[], n: number): T[] {
	return picks
		.filter((p) => p.valueZ > 0 && getsVerdict(p.position))
		.sort((a, b) => b.valueZ - a.valueZ)
		.slice(0, n);
}

/**
 * Busts: picks the board tags as busts (pickVerdict: counted on, and scored
 * under half their expectation), ranked by how far they fell short of players
 * at their position taken around the same round (valueZ). A season with fewer
 * than `n` busts lists fewer rather than fill a row with a pick the board
 * calls a letdown. Never a kicker or defense.
 */
export function topBusts<T extends PickValue & Positioned & { seasonPoints: number }>(
	picks: T[],
	n: number
): T[] {
	return picks
		.filter(
			(p) =>
				pickVerdict(p.seasonPoints, p.expectedPoints, p.highExpectation, p.position) === 'bust'
		)
		.sort((a, b) => a.valueZ - b.valueZ)
		.slice(0, n);
}

/**
 * The all-time steals and busts. Records wait for a season to finish: ranked
 * within position, a few weeks of points read like a whole season, so the
 * season still being played is left out.
 */
export function draftRecords<
	T extends PickValue & Positioned & { seasonYear: number; seasonPoints: number }
>(
	picks: T[],
	completedSeasons: Set<number>,
	n: number
): { steals: T[]; busts: T[] } {
	const finished = picks.filter((p) => completedSeasons.has(p.seasonYear));
	return { steals: topSteals(finished, n), busts: topBusts(finished, n) };
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

/**
 * Null when the pick has no expectation to judge against, or is a kicker or
 * defense (NO_VERDICT_POSITIONS).
 */
export function pickVerdict(
	points: number,
	expected: number | null | undefined,
	highExpectation: boolean,
	position: string | null | undefined
): PickVerdict | null {
	if (!getsVerdict(position)) return null;
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

/**
 * Letter grade against expectation. F is kept for busts; anything else bottoms
 * out at D. Kickers and defenses are graded too, but can't be busts, so their
 * floor is D.
 */
export function pickGrade(
	points: number,
	expected: number | null | undefined,
	highExpectation: boolean,
	position: string | null | undefined
): string | null {
	const r = ratio(points, expected);
	if (r == null) return null;
	if (r >= 1.6) return 'A+';
	if (r >= 1.3) return 'A';
	if (r >= 1.1) return 'B+';
	if (r >= 0.95) return 'B';
	if (r >= 0.8) return 'C+';
	if (r >= 0.65) return 'C';
	return pickVerdict(points, expected, highExpectation, position) === 'bust' ? 'F' : 'D';
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
