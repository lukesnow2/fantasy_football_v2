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
			expectedRelative
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
