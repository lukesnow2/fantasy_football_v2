/**
 * Career and current-season expected (Pythagorean) wins and luck for one
 * manager, from their power-rankings rows: one per team-season, taken from that
 * season's latest ranked week.
 */

export type SeasonLuck = {
	seasonYear: number;
	teamKey: number;
	weekNumber: number;
	wins: number;
	ties: number;
	pythagoreanWins: number | null;
	luckFactor: number | null;
	playoffOdds: number | null;
	inProgress: boolean;
	/** A playoff game has been played: the regular season, and the bracket, are settled. */
	bracketSet: boolean;
	madePlayoffs: boolean;
};

export type CurrentSeasonLuck = {
	seasonYear: number;
	throughWeek: number;
	/** True between seasons: the league's latest season, already over. */
	final: boolean;
	regularSeasonWins: number;
	pythagoreanWins: number | null;
	luckFactor: number | null;
	/**
	 * Only during the regular season, and only for a single team. Once the
	 * bracket is set the odds are stale (they are week 14's), so they give way
	 * to inPlayoffs.
	 */
	playoffOdds: number | null;
	/** Whether the bracket includes this team, once it is set; otherwise null. */
	inPlayoffs: boolean | null;
};

/**
 * `latestSeason` is the league's most recent season, played or being played.
 * The "current" season is that one: in progress, or, between seasons, just
 * finished. A manager who didn't play in it gets null rather than their own
 * last season, which for someone who left the league could be years old.
 */
export function summarizeLuck(seasons: SeasonLuck[], latestSeason: number | null) {
	const scored = seasons.filter((s) => s.pythagoreanWins != null && s.luckFactor != null);
	const sum = (f: (s: SeasonLuck) => number) =>
		scored.length ? scored.reduce((acc, s) => acc + f(s), 0) : null;

	return {
		careerRegularSeasonWins: sum((s) => s.wins + 0.5 * s.ties),
		careerPythagoreanWins: sum((s) => s.pythagoreanWins as number),
		careerLuckFactor: sum((s) => s.luckFactor as number),
		// Seasons, not teams: a manager who ran two teams in a year played one season.
		luckSeasons: new Set(scored.map((s) => s.seasonYear)).size,
		latestSeason,
		currentSeason: currentSeason(seasons, latestSeason)
	};
}

function currentSeason(
	seasons: SeasonLuck[],
	latestSeason: number | null
): CurrentSeasonLuck | null {
	const live = seasons.filter((s) => s.inProgress);
	const rows = live.length
		? live
		: seasons.filter((s) => latestSeason != null && s.seasonYear === latestSeason);
	if (!rows.length) return null;

	// Usually one row; a manager running two teams in the season gets their
	// totals added, and no single playoff odds (each team has its own).
	const total = (f: (s: SeasonLuck) => number | null) =>
		rows.some((s) => f(s) == null) ? null : rows.reduce((acc, s) => acc + (f(s) as number), 0);
	const final = live.length === 0;
	const bracketSet = rows.every((s) => s.bracketSet);
	return {
		seasonYear: rows[0].seasonYear,
		throughWeek: Math.max(...rows.map((s) => s.weekNumber)),
		final,
		regularSeasonWins: (total((s) => s.wins) as number) + 0.5 * (total((s) => s.ties) as number),
		pythagoreanWins: total((s) => s.pythagoreanWins),
		luckFactor: total((s) => s.luckFactor),
		playoffOdds: !final && !bracketSet && rows.length === 1 ? rows[0].playoffOdds : null,
		inPlayoffs: !final && bracketSet ? rows.some((s) => s.madePlayoffs) : null
	};
}
