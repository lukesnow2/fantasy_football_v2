import { sql } from 'drizzle-orm';
import { db } from '$lib/server/db';
import { getLeagueFormat } from '$lib/server/league-format';
import {
	DEFAULT_PARAMS,
	gamesFingerprint,
	simulatePlayoffOdds,
	type ModelParams,
	type OddsResult,
	type PlayedGame,
	type TeamOdds
} from '$lib/playoff-odds';

/**
 * Simulated playoff odds for a season, shared by every page that shows them
 * (the This Season widget, the manager pages) so they can't disagree.
 *
 * Runs the model twice: on every completed regular-season week, and on the
 * weeks before the latest one, so callers can show how each team's odds
 * moved with last week's results. The regular-season length and bracket size
 * come from the season's stored Yahoo settings (getLeagueFormat), inferred
 * only until they're stored.
 */

/** Rows come back camel-cased with numerics as strings; read them loosely. */
type Row = Record<string, unknown>;

export const SIMS = 10000;

/** The model parameters the site runs with (a copy: DEFAULT_PARAMS is shared). */
export const PARAMS: Readonly<ModelParams> = Object.freeze({ ...DEFAULT_PARAMS });

/**
 * The simulation seed for a season as it stood after `playedWeeks`: fixed, so
 * the same games always give the same odds.
 */
export const oddsSeed = (season: number, playedWeeks: number): number => season * 100 + playedWeeks;

export interface SeasonTeamOdds extends TeamOdds {
	teamName: string;
	managerName: string | null;
	projectedPointsRank: number | undefined;
	previousOdds: number | null;
}

export type SeasonOdds =
	| { season: number; available: false; reason: string }
	| {
			season: number;
			available: true;
			throughWeek: number;
			regularSeasonWeeks: number;
			formatSource: 'league-settings' | 'inferred';
			phase: 'regular' | 'postseason';
			sims: number;
			playoffTeams: number;
			byes: number;
			leagueAverage: number;
			teams: SeasonTeamOdds[];
	  };

/**
 * Simulations are cached by a fingerprint of every game's score, so a reloaded
 * week with a stat correction recomputes even though the game count is the
 * same. Names are joined per call, so a team rename shows immediately.
 */
const cache = new Map<string, { now: OddsResult; prev: OddsResult | null }>();

export async function latestSeason(): Promise<number> {
	const rows = await db.execute(sql`SELECT MAX(season_year) AS season FROM edw.dim_league`);
	return Number((rows[0] as Row | undefined)?.season) || new Date().getFullYear();
}

/**
 * `throughWeek` simulates the season as it stood after that week, for pages
 * that show a past week (power rankings): only games up to it count, and the
 * movement compares it with the week before. Omitted, it is the latest week.
 */
export async function getSeasonOdds(season: number, throughWeek?: number): Promise<SeasonOdds> {
	return (await getSeasonOddsForSnapshot(season, throughWeek)).odds;
}

/**
 * getSeasonOdds plus the fingerprint of every game and score the forecast saw
 * (playoff-odds.ts gamesFingerprint), for the saved snapshots
 * (scripts/snapshot-playoff-odds.ts). Kept off SeasonOdds so the API
 * responses that return the odds whole don't carry it. Null when there are no
 * odds.
 */
export async function getSeasonOddsForSnapshot(
	season: number,
	throughWeek?: number
): Promise<{ odds: SeasonOdds; gamesFingerprint: string | null }> {
	const [matchupRows, teamRowsRaw, format] = await Promise.all([
		db.execute(sql`
			SELECT dw.week_number AS week,
			       fm.team1_key AS a, fm.team2_key AS b,
			       fm.team1_points AS sa, fm.team2_points AS sb,
			       fm.is_playoffs AS po, fm.is_consolation AS cons
			FROM edw.fact_matchup fm
			JOIN edw.dim_week dw ON dw.week_key = fm.week_key
			WHERE fm.season_year = ${season}
		`),
		db.execute(sql`
			SELECT DISTINCT dt.team_key AS team_key, dt.team_name AS team_name,
			       dt.manager_name AS manager_name
			FROM edw.dim_team dt
			JOIN edw.dim_league dl ON dl.league_key = dt.league_key
			WHERE dl.season_year = ${season}
		`),
		getLeagueFormat(season)
	]);
	const rows = Array.from(matchupRows) as Row[];
	const teamRows = Array.from(teamRowsRaw) as Row[];

	const asOf = (week: number) => throughWeek == null || week <= throughWeek;
	const games: PlayedGame[] = rows
		.filter((r) => !r.po && !r.cons && r.sa != null && r.sb != null && asOf(Number(r.week)))
		.map((r) => ({
			week: Number(r.week),
			teamA: Number(r.a),
			teamB: Number(r.b),
			scoreA: Number(r.sa),
			scoreB: Number(r.sb)
		}));
	const postseasonStarted = throughWeek == null && rows.some((r) => r.po || r.cons);

	// Games define who is in the league once there are any; dim_team only
	// supplies names. A team missing from dim_team keeps its games (and its
	// opponents keep their results) under a placeholder name.
	const named = new Map(
		teamRows.map((r) => [
			Number(r.teamKey),
			{
				teamName: String(r.teamName ?? ''),
				managerName: r.managerName == null ? null : String(r.managerName)
			}
		])
	);
	const inGames = new Set(games.flatMap((g) => [g.teamA, g.teamB]));
	const teamKeys = [...(inGames.size > 0 ? inGames : named.keys())].sort((x, y) => x - y);
	const teams = teamKeys.map((teamKey) => ({
		teamKey,
		teamName: named.get(teamKey)?.teamName ?? `Team ${teamKey}`,
		managerName: named.get(teamKey)?.managerName ?? null
	}));

	if (teams.length < Math.max(2, format.playoffTeams)) {
		return {
			odds: { season, available: false, reason: 'Not enough teams loaded for this season yet.' },
			gamesFingerprint: null
		};
	}

	const playedWeeks = games.reduce((m, g) => Math.max(m, g.week), 0);
	const regularSeasonWeeks = Math.max(format.regularSeasonWeeks, playedWeeks);
	const { playoffTeams, source: formatSource } = format;

	const keys = teams.map((t) => t.teamKey);
	const fingerprint = gamesFingerprint(games);
	const key = `${season}:${regularSeasonWeeks}:${playoffTeams}:${keys.join(',')}:${fingerprint}`;
	let sims = cache.get(key);
	if (!sims) {
		// One seed for both runs: the engine draws each week's randomness from
		// its own stream, so the two runs share draws for every week both
		// simulate and the change mostly reflects last week's results.
		const seed = oddsSeed(season, playedWeeks);
		const run = (through: number): OddsResult =>
			simulatePlayoffOdds({
				teamKeys: keys,
				games: games.filter((g) => g.week <= through),
				regularSeasonWeeks,
				playoffTeams,
				sims: SIMS,
				seed,
				params: PARAMS
			});
		sims = { now: run(playedWeeks), prev: playedWeeks > 0 ? run(playedWeeks - 1) : null };
		// Superseded fingerprints are never read again; keep the map small.
		if (cache.size >= 32) cache.clear();
		cache.set(key, sims);
	}
	const { now, prev } = sims;
	const prevOdds = new Map(prev?.teams.map((t) => [t.teamKey, t.playoffOdds]) ?? []);
	const info = new Map(teams.map((t) => [t.teamKey, t]));

	// League ranks for the scoring projection, so "118.2" has context.
	const byProj = [...now.teams].sort((a, b) => b.projectedPoints - a.projectedPoints);
	const projRank = new Map(byProj.map((t, i) => [t.teamKey, i + 1]));

	const odds: SeasonOdds = {
		season,
		available: true,
		throughWeek: now.throughWeek,
		regularSeasonWeeks,
		formatSource,
		phase: postseasonStarted || playedWeeks >= regularSeasonWeeks ? 'postseason' : 'regular',
		sims: now.sims,
		playoffTeams: now.playoffTeams,
		byes: now.byes,
		leagueAverage: now.leagueAverage,
		teams: now.teams.map((t) => ({
			...t,
			teamName: info.get(t.teamKey)?.teamName ?? '',
			managerName: info.get(t.teamKey)?.managerName ?? null,
			projectedPointsRank: projRank.get(t.teamKey),
			previousOdds: prev ? (prevOdds.get(t.teamKey) ?? null) : null
		}))
	};
	return { odds, gamesFingerprint: fingerprint };
}
