/**
 * Playoff odds by Monte Carlo simulation of the rest of the season.
 *
 * The model, in the order it runs:
 *
 *  1. Remaining schedule. The warehouse only stores completed weeks, but every
 *     season since 2005 has been a straight round robin that repeats: week
 *     w + (teams - 1) is a rematch of week w, without exception. So weeks after
 *     the first cycle are copies of weeks already played, and any first-cycle
 *     week not yet played is drawn from the pairs that haven't met yet.
 *
 *  2. Team strength. Fantasy points don't depend on the opponent (there is no
 *     defence), so points scored is the real signal and the win-loss record is
 *     mostly that signal plus luck. Each team's expected weekly score is its
 *     average score, shrunk toward the league average by a prior worth
 *     `shrinkage` games: after three weeks a team's own scores are half the
 *     estimate, after twelve they're 80%. `decay` can weight recent weeks more,
 *     but the backtest found that hurt (week-to-week form in fantasy is mostly
 *     noise), so it's off; real changes in a team are carried by the drift below.
 *
 *  3. Uncertainty about that strength. A team's true level is not known exactly
 *     (that's what the shrinkage admits), and it moves during the season:
 *     injuries, trades, waiver pickups. Each simulated season draws the team's
 *     starting level from its posterior, then lets it random-walk week to week.
 *     This is what keeps "two wins clear with four to play" from reading as a
 *     lock when the team is a pretender on points.
 *
 *  4. Weekly scores. Each game draws a score around the team's current level
 *     with the team's own volatility (boom-or-bust rosters stay boom-or-bust),
 *     also shrunk toward the league's typical spread.
 *
 *  5. Seeding and bracket. Final standings sort by wins, then points for, which
 *     reproduces the real playoff field in every season 2010-2025. Simulated
 *     points count toward the tiebreak. The top `playoffTeams` make it into a
 *     standard single-elimination bracket: the smallest power of two that fits
 *     them, top seeds on byes, 1 and 2 on opposite sides. For the league's six
 *     teams that's the real bracket: 3v6 and 4v5, then 1 plays the 4/5 winner
 *     and 2 plays the 3/6 winner.
 *
 * The parameters were chosen by backtesting every season 2008-2025 from every
 * week of the regular season (scripts/backtest-playoff-odds.ts): tuned on
 * 2008-2018, checked on held-out 2019-2025. From week 3 on with two or more
 * weeks left, Brier 0.1450 against 0.1484 for record-and-schedule alone, and
 * calibrated: teams given 85-95% made it 90.5% of the time.
 */

export interface PlayedGame {
	week: number;
	teamA: number;
	teamB: number;
	scoreA: number;
	scoreB: number;
}

export interface ModelParams {
	/** Weight multiplier per week of age. 1 = all weeks count the same. */
	decay: number;
	/** Prior strength toward the league mean, in games. */
	shrinkage: number;
	/** Weekly random-walk drift in true strength, as a fraction of score noise. */
	drift: number;
	/** Prior weight, in games, pulling each team's volatility to the league's. */
	volatilityPrior: number;
}

export const DEFAULT_PARAMS: ModelParams = {
	decay: 1,
	shrinkage: 3,
	drift: 0.3,
	volatilityPrior: 8
};

export interface OddsOptions {
	teamKeys: number[];
	/** Completed regular-season games only. */
	games: PlayedGame[];
	regularSeasonWeeks: number;
	/** Teams in the championship bracket (Yahoo's num_playoff_teams). */
	playoffTeams?: number;
	sims?: number;
	seed?: number;
	params?: Partial<ModelParams>;
}

export interface TeamOdds {
	teamKey: number;
	wins: number;
	losses: number;
	ties: number;
	pointsFor: number;
	/** Wins if the team had played every other team every week. */
	allPlayWins: number;
	allPlayLosses: number;
	/** Actual wins minus all-play expected wins over the same games. */
	luck: number;
	/** Model's expected score next week. */
	projectedPoints: number;
	/** Average projected score of the opponents still on the schedule. */
	remainingOpponentPoints: number | null;
	remainingGames: number;
	projectedWins: number;
	playoffOdds: number;
	byeOdds: number;
	topSeedOdds: number;
	titleOdds: number;
	/** Mathematically certain, not just 100% in simulation. */
	clinched: boolean;
	eliminated: boolean;
}

export interface OddsResult {
	throughWeek: number;
	sims: number;
	/** The format that was simulated, so callers can draw the cut line from it. */
	playoffTeams: number;
	/** First-round byes the bracket gives the top seeds. */
	byes: number;
	leagueAverage: number;
	teams: TeamOdds[];
}

/**
 * A short fingerprint of every game and score, independent of order. Results
 * cached under it go stale the moment any score changes (a stat correction
 * reloads a week with the same games and different points).
 */
export function gamesFingerprint(games: PlayedGame[]): string {
	const lines = games.map((g) => `${g.week}:${g.teamA}:${g.teamB}:${g.scoreA}:${g.scoreB}`).sort();
	// FNV-1a.
	let h = 0x811c9dc5;
	for (const line of lines) {
		for (let i = 0; i < line.length; i++) {
			h ^= line.charCodeAt(i);
			h = Math.imul(h, 0x01000193);
		}
		h ^= 0x0a;
		h = Math.imul(h, 0x01000193);
	}
	return `${games.length}:${(h >>> 0).toString(36)}`;
}

// ---------------------------------------------------------------- randomness

/** mulberry32: small, fast, seedable. Seeded so the odds don't jitter on reload. */
export function rng(seed: number): () => number {
	let a = seed >>> 0;
	return () => {
		a = (a + 0x6d2b79f5) >>> 0;
		let t = a;
		t = Math.imul(t ^ (t >>> 15), t | 1);
		t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
}

/** Seed for one simulated season's draws for one week (week 0: starting levels). */
function streamSeed(seed: number, sim: number, week: number): number {
	let h = seed >>> 0;
	h = Math.imul(h ^ sim, 0x9e3779b1);
	h = Math.imul(h ^ week, 0x85ebca77);
	return (h ^ (h >>> 16)) >>> 0;
}

/**
 * Standard-normal draws (Box-Muller) from a mulberry32 stream that is reseeded
 * in place, so switching streams per simulated week allocates nothing.
 */
class NormalStream {
	private state = 0;
	private spare: number | null = null;

	reseed(seed: number) {
		this.state = seed >>> 0;
		this.spare = null;
	}

	private uniform(): number {
		this.state = (this.state + 0x6d2b79f5) >>> 0;
		let t = this.state;
		t = Math.imul(t ^ (t >>> 15), t | 1);
		t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	}

	next(): number {
		if (this.spare !== null) {
			const s = this.spare;
			this.spare = null;
			return s;
		}
		let u = 0;
		let v = 0;
		while (u === 0) u = this.uniform();
		while (v === 0) v = this.uniform();
		const mag = Math.sqrt(-2 * Math.log(u));
		this.spare = mag * Math.sin(2 * Math.PI * v);
		return mag * Math.cos(2 * Math.PI * v);
	}
}

// ------------------------------------------------------------------ schedule

type Pairing = [number, number];

const pairKey = (a: number, b: number) => (a < b ? `${a}-${b}` : `${b}-${a}`);

/**
 * First-cycle weeks (1 .. teams-1, capped at the season length) whose
 * pairings are unknown: neither the week nor any of its rematch weeks has
 * been played. These are the weeks a schedule has to be drawn for.
 */
export function unknownCycleWeeks(
	teamCount: number,
	regularSeasonWeeks: number,
	playedWeeks: Iterable<number>
): number[] {
	const cycle = teamCount - 1;
	const known = new Set<number>();
	for (const w of playedWeeks) known.add(((w - 1) % cycle) + 1);
	const out: number[] = [];
	for (let w = 1; w <= Math.min(cycle, regularSeasonWeeks); w++) if (!known.has(w)) out.push(w);
	return out;
}

/**
 * Every regular-season week's pairings. Known weeks come from the games;
 * later-cycle weeks copy the matching first-cycle week; first-cycle weeks not
 * yet played are filled with a random completion of the round robin.
 */
export function buildSchedule(
	teamKeys: number[],
	games: PlayedGame[],
	regularSeasonWeeks: number,
	rand: () => number
): Map<number, Pairing[]> {
	const cycle = teamKeys.length - 1;
	const known = new Map<number, Pairing[]>();
	for (const g of games) {
		if (!known.has(g.week)) known.set(g.week, []);
		known.get(g.week)!.push([g.teamA, g.teamB]);
	}

	const schedule = new Map<number, Pairing[]>(known);
	const cycleWeek = (w: number) => ((w - 1) % cycle) + 1;

	// A later-cycle week that has been played fixes its first-cycle twin too.
	const firstCycle = new Map<number, Pairing[]>();
	for (const [w, pairs] of known) {
		const c = cycleWeek(w);
		if (!firstCycle.has(c)) firstCycle.set(c, pairs);
	}

	const missing = unknownCycleWeeks(teamKeys.length, regularSeasonWeeks, known.keys());

	if (missing.length > 0) {
		const met = new Set<string>();
		for (const pairs of firstCycle.values()) for (const [a, b] of pairs) met.add(pairKey(a, b));
		const fill = completeRoundRobin(teamKeys, met, missing.length, rand);
		missing.forEach((w, i) => firstCycle.set(w, fill[i]));
	}

	for (let w = 1; w <= regularSeasonWeeks; w++) {
		if (!schedule.has(w)) schedule.set(w, firstCycle.get(cycleWeek(w)) ?? []);
	}
	return schedule;
}

/**
 * `weeks` perfect matchings covering only pairs not in `met`. Randomised
 * backtracking; with ten teams this is instant. If the history isn't a clean
 * round robin (no completion exists), falls back to random pairings.
 */
function completeRoundRobin(
	teamKeys: number[],
	met: Set<string>,
	weeks: number,
	rand: () => number
): Pairing[][] {
	const shuffle = <T>(xs: T[]) => {
		const a = [...xs];
		for (let i = a.length - 1; i > 0; i--) {
			const j = Math.floor(rand() * (i + 1));
			[a[i], a[j]] = [a[j], a[i]];
		}
		return a;
	};

	const used = new Set(met);
	const out: Pairing[][] = [];
	let budget = 20000;

	const matchWeek = (left: number[], acc: Pairing[]): boolean => {
		if (--budget < 0) return false;
		if (left.length === 0) {
			out.push([...acc]);
			if (out.length === weeks) return true;
			if (fillWeek()) return true;
			out.pop();
			return false;
		}
		const [a, ...rest] = left;
		for (const b of shuffle(rest)) {
			const k = pairKey(a, b);
			if (used.has(k)) continue;
			used.add(k);
			acc.push([a, b]);
			if (
				matchWeek(
					rest.filter((x) => x !== b),
					acc
				)
			)
				return true;
			acc.pop();
			used.delete(k);
		}
		return false;
	};
	const fillWeek = (): boolean => matchWeek(shuffle(teamKeys), []);

	if (teamKeys.length % 2 === 0 && fillWeek()) return out;

	// Fallback: random pairings, ignoring who has met.
	const fallback: Pairing[][] = [];
	for (let i = 0; i < weeks; i++) {
		const order = shuffle(teamKeys);
		const week: Pairing[] = [];
		for (let j = 0; j + 1 < order.length; j += 2) week.push([order[j], order[j + 1]]);
		fallback.push(week);
	}
	return fallback;
}

// ------------------------------------------------------------------- seeding

/**
 * The league's seeding order: wins (ties as half), then points for. It
 * reproduces the real playoff field in every season 2010-2025. The standings
 * and the final field use this; the simulation sorts by the same two keys.
 */
export function compareSeeding(
	a: { wins: number; ties: number; pointsFor: number },
	b: { wins: number; ties: number; pointsFor: number }
): number {
	return b.wins + b.ties / 2 - (a.wins + a.ties / 2) || b.pointsFor - a.pointsFor;
}

// ------------------------------------------------------------------- bracket

/**
 * Seeds (1-based) in bracket order for a bracket of `size` slots, a power of
 * two. Adjacent slots meet in round one and the halves meet in the final, so
 * the top seeds can't meet until the end: 8 slots gives 1,8,4,5,2,7,3,6.
 * Seeds beyond the number of playoff teams are byes.
 */
export function bracketSlots(size: number): number[] {
	let slots = [1];
	while (slots.length < size) {
		const sum = slots.length * 2 + 1;
		slots = slots.flatMap((seed) => [seed, sum - seed]);
	}
	return slots;
}

// ------------------------------------------------------------------ strength

/** Per-team arrays, index-aligned with the team keys passed in. */
interface Strengths {
	mean: number[];
	/** Posterior sd of the team's true level. */
	meanSd: number[];
	/** Week-to-week score sd. */
	noiseSd: number[];
	leagueAverage: number;
}

function estimateStrength(
	teamKeys: number[],
	games: PlayedGame[],
	throughWeek: number,
	p: ModelParams
): Strengths {
	const scores = new Map<number, { week: number; pts: number }[]>();
	for (const k of teamKeys) scores.set(k, []);
	for (const g of games) {
		scores.get(g.teamA)?.push({ week: g.week, pts: g.scoreA });
		scores.get(g.teamB)?.push({ week: g.week, pts: g.scoreB });
	}

	const all = games.flatMap((g) => [g.scoreA, g.scoreB]);
	// No games yet: every team is identical, so the level is arbitrary.
	const leagueAverage = all.length ? all.reduce((s, x) => s + x, 0) / all.length : 100;

	// Pooled within-team spread is the league's typical weekly noise. Fantasy
	// scores run about 20% of the mean week to week; that's the prior before
	// there's enough data to measure it.
	let ss = 0;
	let dof = 0;
	for (const list of scores.values()) {
		if (list.length < 2) continue;
		const m = list.reduce((s, x) => s + x.pts, 0) / list.length;
		for (const x of list) ss += (x.pts - m) ** 2;
		dof += list.length - 1;
	}
	const priorVar = (0.2 * leagueAverage) ** 2;
	const priorDof = 10;
	const leagueVar = (ss + priorVar * priorDof) / (dof + priorDof);

	const out: Strengths = { mean: [], meanSd: [], noiseSd: [], leagueAverage };
	for (const k of teamKeys) {
		const list = scores.get(k)!;
		let wSum = 0;
		let w2Sum = 0;
		let wPts = 0;
		for (const x of list) {
			const w = Math.pow(p.decay, throughWeek - x.week);
			wSum += w;
			w2Sum += w * w;
			wPts += w * x.pts;
		}
		const mean = (wPts + p.shrinkage * leagueAverage) / (wSum + p.shrinkage);

		let teamSs = 0;
		if (list.length >= 2) {
			const m = list.reduce((s, x) => s + x.pts, 0) / list.length;
			for (const x of list) teamSs += (x.pts - m) ** 2;
		}
		const teamDof = Math.max(list.length - 1, 0);
		const noiseVar = (teamSs + leagueVar * p.volatilityPrior) / (teamDof + p.volatilityPrior);

		// Effective sample size of a weighted mean, plus the prior's weight.
		const nEff = wSum > 0 ? (wSum * wSum) / w2Sum : 0;
		const meanSd = Math.sqrt(leagueVar / (nEff + p.shrinkage));

		out.mean.push(mean);
		out.meanSd.push(meanSd);
		out.noiseSd.push(Math.sqrt(noiseVar));
	}
	return out;
}

// -------------------------------------------------------------- clinch math

/**
 * Conservative clinch and elimination flags. Each team's best and worst
 * possible win totals are bounded independently (treating every remaining
 * game as winnable and losable), which can only overstate rivals, so a team
 * flagged here really is in or out. Ties on wins don't count either way,
 * because the points tiebreak isn't settled.
 */
function lockStatus(
	teamKeys: number[],
	wins: Map<number, number>,
	remaining: Map<number, number>,
	playoffTeams: number
) {
	const out = new Map<number, { clinched: boolean; eliminated: boolean }>();
	for (const k of teamKeys) {
		const myMin = wins.get(k)!;
		const myMax = myMin + remaining.get(k)!;
		let canCatch = 0;
		let surelyAhead = 0;
		for (const j of teamKeys) {
			if (j === k) continue;
			const jMin = wins.get(j)!;
			const jMax = jMin + remaining.get(j)!;
			if (jMax >= myMin) canCatch++;
			if (jMin > myMax) surelyAhead++;
		}
		out.set(k, {
			clinched: canCatch < playoffTeams,
			eliminated: surelyAhead >= playoffTeams
		});
	}
	return out;
}

// ---------------------------------------------------------------- simulation

export function simulatePlayoffOdds(opts: OddsOptions): OddsResult {
	const p: ModelParams = { ...DEFAULT_PARAMS, ...opts.params };
	const playoffTeams = Math.min(opts.playoffTeams ?? 6, opts.teamKeys.length);
	const bracketSize = 2 ** Math.ceil(Math.log2(Math.max(playoffTeams, 2)));
	const byes = bracketSize - playoffTeams;
	// Each slot holds a seeding position (0-based rank) or -1 for a bye.
	const slots = bracketSlots(bracketSize).map((seed) => (seed <= playoffTeams ? seed - 1 : -1));
	const sims = opts.sims ?? 10000;
	const R = opts.regularSeasonWeeks;
	const keys = opts.teamKeys;
	const n = keys.length;
	const idx = new Map(keys.map((k, i) => [k, i]));

	const games = opts.games.filter((g) => g.week <= R && idx.has(g.teamA) && idx.has(g.teamB));
	const throughWeek = games.reduce((m, g) => Math.max(m, g.week), 0);

	// Current record, points, all-play record.
	const wins = new Float64Array(n);
	const losses = new Float64Array(n);
	const ties = new Float64Array(n);
	const pf = new Float64Array(n);
	const allW = new Float64Array(n);
	const allL = new Float64Array(n);
	const byWeek = new Map<number, Map<number, number>>();
	for (const g of games) {
		const a = idx.get(g.teamA)!;
		const b = idx.get(g.teamB)!;
		pf[a] += g.scoreA;
		pf[b] += g.scoreB;
		if (g.scoreA > g.scoreB) {
			wins[a]++;
			losses[b]++;
		} else if (g.scoreB > g.scoreA) {
			wins[b]++;
			losses[a]++;
		} else {
			ties[a]++;
			ties[b]++;
		}
		if (!byWeek.has(g.week)) byWeek.set(g.week, new Map());
		byWeek.get(g.week)!.set(a, g.scoreA).set(b, g.scoreB);
	}
	for (const week of byWeek.values()) {
		for (const [i, s] of week) {
			for (const [j, t] of week) {
				if (i === j) continue;
				if (s > t) allW[i]++;
				else if (s < t) allL[i]++;
				else {
					allW[i] += 0.5;
					allL[i] += 0.5;
				}
			}
		}
	}

	const { mean, meanSd, noiseSd, leagueAverage } = estimateStrength(keys, games, throughWeek, p);
	const pooledNoise = Math.sqrt(noiseSd.reduce((s, x) => s + x * x, 0) / n);
	const driftSd = p.drift * pooledNoise;

	const seed = opts.seed ?? 20261106;
	// Schedule draws get their own stream. Score draws come from one stream per
	// (simulated season, week), so two runs with the same seed see identical
	// randomness for every week both simulate: comparing this week's odds with
	// last week's then measures last week's results, not sampling noise.
	const rand = rng(seed);
	const normal = new NormalStream();
	const gauss = () => normal.next();
	const useStream = (sim: number, week: number) => normal.reseed(streamSeed(seed, sim, week));

	// The schedule only varies when first-cycle weeks are unplayed. Redraw it
	// every so often so the odds average over the possible schedules.
	const scheduleUncertain = unknownCycleWeeks(n, R, new Set(games.map((g) => g.week))).length > 0;
	const toIdx = (pairs: Pairing[]) =>
		pairs.map(([a, b]) => [idx.get(a)!, idx.get(b)!] as [number, number]);
	let schedule = buildSchedule(keys, games, R, rand);
	const remainingWeeks: number[] = [];
	for (let w = throughWeek + 1; w <= R; w++) remainingWeeks.push(w);

	// Remaining-schedule stats come from the first drawn schedule; when it is
	// uncertain they're averaged below with the sims.
	const remCount = new Float64Array(n);
	const remOppPts = new Float64Array(n);
	const tallySchedule = (sch: Map<number, Pairing[]>, weight: number) => {
		for (const w of remainingWeeks) {
			for (const [a, b] of toIdx(sch.get(w) ?? [])) {
				remCount[a] += weight;
				remCount[b] += weight;
				remOppPts[a] += weight * mean[b];
				remOppPts[b] += weight * mean[a];
			}
		}
	};

	const made = new Float64Array(n);
	const bye = new Float64Array(n);
	const top = new Float64Array(n);
	const title = new Float64Array(n);
	const winSum = new Float64Array(n);

	const level = new Float64Array(n);
	const simW = new Float64Array(n);
	const simPf = new Float64Array(n);
	const order = keys.map((_, i) => i);
	const redrawEvery = 250;
	const draws = scheduleUncertain ? Math.ceil(sims / redrawEvery) : 1;
	let weeksIdx = remainingWeeks.map((w) => toIdx(schedule.get(w) ?? []));
	tallySchedule(schedule, 1 / draws);

	// Scores of the last game played, written in place: this runs ~600k times
	// per simulation, so it avoids allocating a result object each call.
	let sa = 0;
	let sb = 0;
	const play = (a: number, b: number) => {
		sa = level[a] + noiseSd[a] * gauss();
		sb = level[b] + noiseSd[b] * gauss();
	};
	const step = () => {
		for (let i = 0; i < n; i++) level[i] += driftSd * gauss();
	};

	for (let s = 0; s < sims; s++) {
		if (scheduleUncertain && s > 0 && s % redrawEvery === 0) {
			schedule = buildSchedule(keys, games, R, rand);
			weeksIdx = remainingWeeks.map((w) => toIdx(schedule.get(w) ?? []));
			tallySchedule(schedule, 1 / draws);
		}
		useStream(s, 0);
		for (let i = 0; i < n; i++) {
			level[i] = mean[i] + meanSd[i] * gauss();
			simW[i] = wins[i] + 0.5 * ties[i];
			simPf[i] = pf[i];
		}
		for (let wi = 0; wi < weeksIdx.length; wi++) {
			useStream(s, remainingWeeks[wi]);
			step();
			for (const [a, b] of weeksIdx[wi]) {
				play(a, b);
				simPf[a] += sa;
				simPf[b] += sb;
				if (sa > sb) simW[a]++;
				else simW[b]++;
			}
		}
		for (let i = 0; i < n; i++) winSum[i] += simW[i];

		order.sort((i, j) => simW[j] - simW[i] || simPf[j] - simPf[i]);
		for (let r = 0; r < playoffTeams && r < n; r++) made[order[r]]++;
		for (let r = 0; r < byes && r < n; r++) bye[order[r]]++;
		top[order[0]]++;

		if (playoffTeams >= 2) {
			const game = (a: number, b: number) => {
				play(a, b);
				return sa >= sb ? a : b;
			};
			// One playoff week per round, each with its own random stream.
			let field = slots.map((rank) => (rank < 0 ? -1 : order[rank]));
			for (let round = 1; field.length > 1; round++) {
				useStream(s, R + round);
				step();
				const next: number[] = [];
				for (let i = 0; i < field.length; i += 2) {
					const [a, b] = [field[i], field[i + 1]];
					next.push(a < 0 ? b : b < 0 ? a : game(a, b));
				}
				field = next;
			}
			title[field[0]]++;
		}
	}

	const remainingByTeam = new Map<number, number>();
	keys.forEach((k, i) => remainingByTeam.set(k, Math.round(remCount[i])));
	const winsByTeam = new Map<number, number>();
	keys.forEach((k, i) => winsByTeam.set(k, wins[i] + 0.5 * ties[i]));
	const locks = lockStatus(keys, winsByTeam, remainingByTeam, playoffTeams);

	const teams: TeamOdds[] = keys.map((k, i) => {
		const allGames = allW[i] + allL[i];
		const played = wins[i] + losses[i] + ties[i];
		const expectedWins = allGames > 0 ? (allW[i] / allGames) * played : 0;
		const lock = locks.get(k)!;
		return {
			teamKey: k,
			wins: wins[i],
			losses: losses[i],
			ties: ties[i],
			pointsFor: pf[i],
			allPlayWins: allW[i],
			allPlayLosses: allL[i],
			luck: wins[i] + 0.5 * ties[i] - expectedWins,
			projectedPoints: mean[i],
			remainingOpponentPoints: remCount[i] > 0 ? remOppPts[i] / remCount[i] : null,
			remainingGames: Math.round(remCount[i]),
			projectedWins: winSum[i] / sims,
			// Never show a simulated certainty the math doesn't back up, and vice versa.
			playoffOdds: lock.clinched ? 1 : lock.eliminated ? 0 : made[i] / sims,
			byeOdds: lock.eliminated ? 0 : bye[i] / sims,
			topSeedOdds: lock.eliminated ? 0 : top[i] / sims,
			titleOdds: lock.eliminated ? 0 : title[i] / sims,
			clinched: lock.clinched,
			eliminated: lock.eliminated
		};
	});
	teams.sort((a, b) => b.playoffOdds - a.playoffOdds || b.projectedWins - a.projectedWins);

	return { throughWeek, sims, playoffTeams, byes, leagueAverage, teams };
}
