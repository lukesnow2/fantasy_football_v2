/**
 * Long-run measures of the league, from regular-season games and champions.
 *
 * Grounded in the sports-economics literature and checked on this league:
 *
 *  - Noll-Scully ratio: the spread of win percentages divided by the spread a
 *    league of coin flips would produce over the same number of games
 *    (0.5 / sqrt(games)). 1.0 means records spread no more than chance would;
 *    above 1 is skill showing through. It depends on season length (Doria &
 *    Nalebuff), so compare seasons of similar length, as this league's are
 *    (12-14 games).
 *  - Skill share (Tango): observed variance = talent variance + luck variance,
 *    luck variance = 0.25 / games. The talent share of a season's spread is
 *    (observed - luck) / observed, floored at 0.
 *  - Record persistence: correlation of a manager's win percentage in one
 *    season with the next.
 *  - Champion's scoring rank: where the champion ranked in regular-season
 *    points per game.
 *
 * One season of ten teams over thirteen games is a small sample, so each
 * season's value is noisy; the rolling mean carries the trend.
 */

export interface RegularGame {
	season: number;
	teamA: number;
	teamB: number;
	pointsA: number;
	pointsB: number;
	winner: number | null;
	tie: boolean;
}

export interface TeamSeason {
	team: number;
	wins: number;
	games: number;
	pointsFor: number;
}

export interface SeasonBalance {
	season: number;
	teams: number;
	gamesPerTeam: number;
	nollScully: number | null;
	skillShare: number | null;
	records: TeamSeason[];
}

export function seasonRecords(games: RegularGame[]): Map<number, TeamSeason[]> {
	const acc = new Map<string, TeamSeason & { season: number }>();
	const add = (season: number, team: number, points: number, result: number) => {
		const key = `${season}|${team}`;
		const r = acc.get(key) ?? { season, team, wins: 0, games: 0, pointsFor: 0 };
		r.games += 1;
		r.pointsFor += points;
		r.wins += result;
		acc.set(key, r);
	};
	for (const g of games) {
		const a = g.tie ? 0.5 : g.winner === g.teamA ? 1 : 0;
		const b = g.tie ? 0.5 : g.winner === g.teamB ? 1 : 0;
		add(g.season, g.teamA, g.pointsA, a);
		add(g.season, g.teamB, g.pointsB, b);
	}
	const bySeason = new Map<number, TeamSeason[]>();
	for (const { season, ...r } of acc.values()) {
		const list = bySeason.get(season) ?? [];
		list.push(r);
		bySeason.set(season, list);
	}
	return bySeason;
}

const mean = (v: number[]) => v.reduce((a, b) => a + b, 0) / v.length;
const variance = (v: number[]) => {
	const m = mean(v);
	return mean(v.map((x) => (x - m) ** 2));
};

export function balanceOf(season: number, records: TeamSeason[]): SeasonBalance {
	const teams = records.length;
	const gamesPerTeam = teams ? mean(records.map((r) => r.games)) : 0;
	if (teams < 4 || gamesPerTeam <= 0) {
		return { season, teams, gamesPerTeam, nollScully: null, skillShare: null, records };
	}
	const pct = records.map((r) => r.wins / r.games);
	const observed = variance(pct);
	const luck = 0.25 / gamesPerTeam;
	return {
		season,
		teams,
		gamesPerTeam,
		nollScully: Math.sqrt(observed) / Math.sqrt(luck),
		skillShare: observed > 0 ? Math.max(0, (observed - luck) / observed) : 0,
		records
	};
}

/** 1-based rank of `team` by regular-season points per game (1 = top scorer). */
export function scoringRank(records: TeamSeason[], team: number): number | null {
	const order = [...records].sort((a, b) => b.pointsFor / b.games - a.pointsFor / a.games);
	const i = order.findIndex((r) => r.team === team);
	return i >= 0 ? i + 1 : null;
}

/** 1-based rank of `team` by regular-season win percentage, points per game breaking ties. */
export function recordRank(records: TeamSeason[], team: number): number | null {
	const order = [...records].sort(
		(a, b) => b.wins / b.games - a.wins / a.games || b.pointsFor / b.games - a.pointsFor / a.games
	);
	const i = order.findIndex((r) => r.team === team);
	return i >= 0 ? i + 1 : null;
}

export function pearson(a: number[], b: number[]): number | null {
	if (a.length !== b.length || a.length < 3) return null;
	const ma = mean(a);
	const mb = mean(b);
	let num = 0;
	let da = 0;
	let db = 0;
	for (let i = 0; i < a.length; i++) {
		num += (a[i] - ma) * (b[i] - mb);
		da += (a[i] - ma) ** 2;
		db += (b[i] - mb) ** 2;
	}
	return da > 0 && db > 0 ? num / Math.sqrt(da * db) : null;
}

/**
 * Correlation of each manager's win percentage with their next season's.
 * `byManagerSeason` maps `${manager}|${season}` to a win percentage.
 */
export function recordPersistence(byManagerSeason: Map<string, number>): { r: number | null; n: number } {
	const a: number[] = [];
	const b: number[] = [];
	for (const [key, pct] of byManagerSeason) {
		const [manager, season] = key.split('|');
		const next = byManagerSeason.get(`${manager}|${Number(season) + 1}`);
		if (next != null) {
			a.push(pct);
			b.push(next);
		}
	}
	return { r: pearson(a, b), n: a.length };
}

/** Trailing mean over the last `window` values (fewer at the start); nulls skipped. */
export function rollingMean(values: (number | null)[], window: number): (number | null)[] {
	return values.map((_, i) => {
		const slice = values.slice(Math.max(0, i - window + 1), i + 1).filter((v): v is number => v != null);
		return slice.length ? mean(slice) : null;
	});
}

/** A small seeded random generator (mulberry32), so simulated bands are stable. */
export function seededRandom(seed: number): () => number {
	let a = seed >>> 0;
	return () => {
		a = (a + 0x6d2b79f5) >>> 0;
		let t = a;
		t = Math.imul(t ^ (t >>> 15), t | 1);
		t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
}

/**
 * The range of Noll-Scully ratios a league of pure coin flips produces: `teams`
 * teams, `games` games each, random pairings every week. Returns the central
 * `level` share of simulated seasons (0.9 = the 5th to the 95th percentile).
 * A season outside it spread out more (or less) than luck alone usually would.
 */
export function coinFlipBand(
	teams: number,
	games: number,
	{ sims = 2000, level = 0.9, seed = 7 }: { sims?: number; level?: number; seed?: number } = {}
): { lo: number; hi: number } {
	const rand = seededRandom(seed + teams * 100 + games);
	const order = Array.from({ length: teams }, (_, i) => i);
	const ratios: number[] = [];
	const luckSd = 0.5 / Math.sqrt(games);
	for (let s = 0; s < sims; s++) {
		const wins = new Array(teams).fill(0);
		const played = new Array(teams).fill(0);
		for (let w = 0; w < games; w++) {
			for (let i = teams - 1; i > 0; i--) {
				const j = Math.floor(rand() * (i + 1));
				[order[i], order[j]] = [order[j], order[i]];
			}
			// With an odd count the last team has a bye.
			for (let i = 0; i + 1 < teams; i += 2) {
				const a = order[i];
				const b = order[i + 1];
				played[a]++;
				played[b]++;
				if (rand() < 0.5) wins[a]++;
				else wins[b]++;
			}
		}
		const pct = wins.map((v, i) => (played[i] ? v / played[i] : 0.5));
		ratios.push(Math.sqrt(variance(pct)) / luckSd);
	}
	ratios.sort((a, b) => a - b);
	const at = (q: number) => ratios[Math.min(ratios.length - 1, Math.max(0, Math.round(q * (ratios.length - 1))))];
	const tail = (1 - level) / 2;
	return { lo: at(tail), hi: at(1 - tail) };
}

/**
 * Kernel density estimate (Epanechnikov kernel) of `values` at each of `xs`.
 * Bandwidth defaults to Silverman's rule, converted from the Gaussian kernel
 * it's stated for to this one (x 2.214, the ratio of canonical bandwidths).
 * Densities integrate to about 1.
 */
export function kde(values: number[], xs: number[], bandwidth?: number): number[] {
	const n = values.length;
	if (!n) return xs.map(() => 0);
	const sd = Math.sqrt(variance(values)) || 1;
	const h = bandwidth ?? 2.214 * 1.06 * sd * Math.pow(n, -0.2);
	return xs.map((x) => {
		let sum = 0;
		for (const v of values) {
			const u = (x - v) / h;
			if (Math.abs(u) <= 1) sum += 0.75 * (1 - u * u);
		}
		return sum / (n * h);
	});
}

/** A quantile of `values` (linear interpolation), or null when empty. */
export function quantile(values: number[], q: number): number | null {
	if (!values.length) return null;
	const v = [...values].sort((a, b) => a - b);
	const pos = (v.length - 1) * q;
	const lo = Math.floor(pos);
	const hi = Math.ceil(pos);
	return v[lo] + (v[hi] - v[lo]) * (pos - lo);
}

export interface Era {
	id: 'early' | 'middle' | 'modern';
	name: string;
	start: number;
	end: number;
}

/**
 * The league's seasons in three contiguous eras of near-equal size, the
 * earlier eras taking the remainder (22 seasons: 8, 7, 7). The historical
 * page's era cards and the trends chart's era presets both use this.
 */
export function splitEras(years: number[]): Era[] {
	const sorted = [...new Set(years)].sort((a, b) => a - b);
	const n = sorted.length;
	if (n < 3) return [];
	const base = Math.floor(n / 3);
	const rem = n % 3;
	const sizes = [base + (rem > 0 ? 1 : 0), base + (rem > 1 ? 1 : 0), base];
	const meta: [Era['id'], string][] = [
		['early', 'Early Era'],
		['middle', 'Middle Era'],
		['modern', 'Modern Era']
	];
	const out: Era[] = [];
	let i = 0;
	sizes.forEach((size, e) => {
		const slice = sorted.slice(i, i + size);
		i += size;
		if (slice.length) out.push({ id: meta[e][0], name: meta[e][1], start: slice[0], end: slice[slice.length - 1] });
	});
	return out;
}

export interface LeagueSettings {
	season: number;
	teams: number;
	regularWeeks: number | null;
	playoffTeams: number | null;
}

export interface LeagueEvent {
	season: number;
	/** Short labels for the axis ("10 teams", "13-game season"). */
	labels: string[];
}

/**
 * Seasons where the league's stored settings changed from the season before:
 * team count, regular-season length, playoff field. Plus the first season
 * with FAAB bids. Only what the data records; nothing inferred.
 */
export function leagueEvents(settings: LeagueSettings[], firstFaabSeason: number | null): LeagueEvent[] {
	const byYear = [...settings].sort((a, b) => a.season - b.season);
	const out = new Map<number, string[]>();
	const add = (season: number, label: string) => out.set(season, [...(out.get(season) ?? []), label]);
	for (let i = 1; i < byYear.length; i++) {
		const prev = byYear[i - 1];
		const cur = byYear[i];
		if (cur.teams !== prev.teams) add(cur.season, `${cur.teams} teams`);
		if (cur.regularWeeks != null && prev.regularWeeks != null && cur.regularWeeks !== prev.regularWeeks) {
			add(cur.season, `${cur.regularWeeks}-week season`);
		}
		if (cur.playoffTeams != null && prev.playoffTeams != null && cur.playoffTeams !== prev.playoffTeams) {
			add(cur.season, `${cur.playoffTeams}-team playoff`);
		}
	}
	if (firstFaabSeason != null) add(firstFaabSeason, 'FAAB bidding');
	return [...out.entries()].sort((a, b) => a[0] - b[0]).map(([season, labels]) => ({ season, labels }));
}
