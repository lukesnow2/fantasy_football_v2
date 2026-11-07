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
