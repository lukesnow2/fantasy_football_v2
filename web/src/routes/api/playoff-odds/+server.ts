import { json } from '@sveltejs/kit';
import { sql } from 'drizzle-orm';
import { db } from '$lib/server/db';
import { simulatePlayoffOdds, type OddsResult, type PlayedGame } from '$lib/playoff-odds';
import type { RequestHandler } from './$types';

/** Rows come back camel-cased with numerics as strings; read them loosely. */
type Row = Record<string, unknown>;

/**
 * Simulated playoff odds for one season (default: the latest).
 *
 * Runs the model twice: on every completed regular-season week, and on the
 * weeks before the latest one, so the widget can show how each team's odds
 * moved with last week's results.
 *
 * Regular-season length isn't stored anywhere for a season in progress (the
 * warehouse only holds completed weeks), so it's taken from the most recent
 * season that has a playoff game: 14 weeks since 2022.
 */

const SIMS = 10000;

/**
 * Simulations are cached by a fingerprint of every game's score, so a reloaded
 * week with a stat correction recomputes even though the game count is the
 * same. Names are joined per request, so a team rename shows immediately.
 */
const cache = new Map<string, { now: OddsResult; prev: OddsResult | null }>();

function fingerprint(games: PlayedGame[]): string {
	// FNV-1a over the games in a fixed order.
	const lines = games.map((g) => `${g.week}:${g.teamA}:${g.teamB}:${g.scoreA}:${g.scoreB}`).sort();
	let h = 0x811c9dc5;
	for (const line of lines) {
		for (let i = 0; i < line.length; i++) {
			h ^= line.charCodeAt(i);
			h = Math.imul(h, 0x01000193);
		}
	}
	return `${games.length}:${(h >>> 0).toString(36)}`;
}

export const GET: RequestHandler = async ({ url }) => {
	try {
		const maxRow = await db.execute(sql`SELECT MAX(season_year) AS season FROM edw.dim_league`);
		const latest = Number((maxRow[0] as Row | undefined)?.season) || new Date().getFullYear();

		const seasonParam = url.searchParams.get('season');
		let season = latest;
		if (seasonParam !== null) {
			const parsed = Number(seasonParam);
			if (!Number.isInteger(parsed) || parsed < 2008 || parsed > latest) {
				return json({ error: 'Invalid season parameter' }, { status: 400 });
			}
			season = parsed;
		}

		const [matchupRows, teamRowsRaw, lengthRowsRaw] = await Promise.all([
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
			db.execute(sql`
				SELECT MAX(dw.week_number) AS weeks
				FROM edw.fact_matchup fm
				JOIN edw.dim_week dw ON dw.week_key = fm.week_key
				WHERE fm.season_year = (
					SELECT MAX(season_year) FROM edw.fact_matchup
					WHERE is_playoffs AND season_year <= ${season}
				)
				AND NOT fm.is_playoffs AND NOT fm.is_consolation
			`)
		]);
		const rows = Array.from(matchupRows) as Row[];
		const teamRows = Array.from(teamRowsRaw) as Row[];
		const lengthRows = Array.from(lengthRowsRaw) as Row[];

		const games: PlayedGame[] = rows
			.filter((r) => !r.po && !r.cons && r.sa != null && r.sb != null)
			.map((r) => ({
				week: Number(r.week),
				teamA: Number(r.a),
				teamB: Number(r.b),
				scoreA: Number(r.sa),
				scoreB: Number(r.sb)
			}));
		const postseasonStarted = rows.some((r) => r.po || r.cons);

		const inGames = new Set(games.flatMap((g) => [g.teamA, g.teamB]));
		const teams = teamRows
			.map((r) => ({
				teamKey: Number(r.teamKey),
				teamName: String(r.teamName ?? ''),
				managerName: r.managerName == null ? null : String(r.managerName)
			}))
			// Once games exist, they define who is in the league.
			.filter((t) => inGames.size === 0 || inGames.has(t.teamKey));

		if (teams.length < 6) {
			return json({
				season,
				available: false,
				reason: 'Not enough teams loaded for this season yet.'
			});
		}

		const playedWeeks = games.reduce((m, g) => Math.max(m, g.week), 0);
		const regularSeasonWeeks = Math.max(Number(lengthRows[0]?.weeks) || 14, playedWeeks);

		const keys = teams.map((t) => t.teamKey);
		const key = `${season}:${regularSeasonWeeks}:${keys.join(',')}:${fingerprint(games)}`;
		let sims = cache.get(key);
		if (!sims) {
			// One seed for both runs, so the week-over-week change reflects last
			// week's results rather than two independent draws of sampling noise.
			const seed = season * 100 + playedWeeks;
			const run = (through: number): OddsResult =>
				simulatePlayoffOdds({
					teamKeys: keys,
					games: games.filter((g) => g.week <= through),
					regularSeasonWeeks,
					sims: SIMS,
					seed
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

		const body = {
			season,
			available: true,
			throughWeek: now.throughWeek,
			regularSeasonWeeks,
			phase: postseasonStarted || playedWeeks >= regularSeasonWeeks ? 'postseason' : 'regular',
			sims: now.sims,
			leagueAverage: now.leagueAverage,
			teams: now.teams.map((t) => ({
				...t,
				teamName: info.get(t.teamKey)?.teamName ?? '',
				managerName: info.get(t.teamKey)?.managerName ?? null,
				projectedPointsRank: projRank.get(t.teamKey),
				previousOdds: prev ? (prevOdds.get(t.teamKey) ?? null) : null
			}))
		};
		return json(body);
	} catch (error) {
		console.error('Playoff odds API error:', error);
		return json({ error: 'Failed to compute playoff odds' }, { status: 500 });
	}
};
