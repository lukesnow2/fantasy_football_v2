import { sql } from 'drizzle-orm';
import { db } from '$lib/server/db';
import { compareSeeding } from '$lib/playoff-odds';
import {
	getSeasonOdds,
	getSeasonOddsWithFingerprint,
	type SeasonOdds
} from '$lib/server/playoff-odds-service';
import { bracketField, type OddsHistory, type OddsHistoryPoint } from '$lib/odds-history';

/**
 * Every team's playoff, bye and title odds week by week through a season, for
 * the odds chart and the end-of-season grade.
 *
 * Regular-season weeks come from the saved forecasts
 * (model.playoff_odds_snapshots), so the chart shows what the site said at the
 * time: the copy taken live that week when there is one, else the latest
 * rebuilt copy. A week with no saved copy is rebuilt here from that week's
 * games and listed in `rebuiltWeeks`. Playoff weeks add the title odds after
 * each round of the real bracket; the playoff and bye odds are settled by then.
 */

type Row = Record<string, unknown>;

/**
 * Built histories, by everything that changes them, least recently used
 * first. A history is small (ten teams, under twenty weeks), so room for
 * every season since 2008 costs little and keeps the page's season picker
 * from rebuilding seasons it just showed.
 */
const cache = new Map<string, OddsHistory>();
const CACHE_SIZE = 32;

/** Postgres codes for a missing table or schema: a warehouse without migration 015. */
const NOT_THERE = new Set(['42P01', '3F000']);

function pgCode(err: unknown): string | undefined {
	const e = err as { code?: unknown; cause?: { code?: unknown } } | null;
	const code = e?.code ?? e?.cause?.code;
	return typeof code === 'string' ? code : undefined;
}

async function savedWeeks(season: number, upTo: number): Promise<Map<number, Row[]>> {
	const out = new Map<number, Row[]>();
	if (upTo < 1) return out;
	let rows: Row[];
	try {
		rows = Array.from(
			await db.execute(sql`
				WITH pick AS (
					SELECT DISTINCT ON (through_week) snapshot_id, through_week
					FROM model.playoff_odds_snapshots
					WHERE season_year = ${season} AND through_week BETWEEN 1 AND ${upTo}
					ORDER BY through_week, captured_live DESC, captured_at DESC
				)
				SELECT p.through_week AS week, p.snapshot_id, t.team_key, t.playoff_odds::float AS playoff,
				       t.bye_odds::float AS bye, t.title_odds::float AS title,
				       t.clinched, t.eliminated
				FROM pick p
				JOIN model.playoff_odds_snapshot_teams t USING (snapshot_id)
			`)
		) as Row[];
	} catch (err) {
		// A warehouse without the model schema (migration 015) has nothing
		// saved. Anything else is a real failure: rebuilding every week would
		// tell readers the odds weren't saved when they were.
		if (!NOT_THERE.has(pgCode(err) ?? '')) throw err;
		console.error('[odds-history] no saved forecasts table:', err);
		return out;
	}
	for (const r of rows) {
		const w = Number(r.week);
		if (!out.has(w)) out.set(w, []);
		out.get(w)!.push(r);
	}
	return out;
}

type AvailableOdds = Extract<SeasonOdds, { available: true }>;

/**
 * The playoff field and byes, for the grade: from the bracket once it has a
 * game (bracketField), else the seeding the Playoff Field card shows (wins,
 * then points for) once the standings are final. The seeding alone only
 * reproduces the real field from 2010 on.
 */
async function seasonField(
	season: number,
	now: AvailableOdds,
	R: number
): Promise<{ field: number[] | null; byes: number[] | null }> {
	const rows = Array.from(
		await db.execute(sql`
			SELECT dw.week_number AS week, fm.team1_key AS a, fm.team2_key AS b
			FROM edw.fact_matchup fm
			JOIN edw.dim_week dw ON dw.week_key = fm.week_key
			WHERE fm.season_year = ${season} AND fm.is_playoffs AND NOT fm.is_consolation
		`)
	) as Row[];
	const seeded = [...now.teams].sort(compareSeeding).map((t) => t.teamKey);
	const games = rows.map((r) => ({ week: Number(r.week), teamA: Number(r.a), teamB: Number(r.b) }));
	if (games.length > 0) return bracketField(games, seeded, now.playoffTeams);
	if (now.phase !== 'postseason' || now.throughWeek < R) return { field: null, byes: null };
	return { field: seeded.slice(0, now.playoffTeams), byes: seeded.slice(0, now.byes) };
}

export async function getOddsHistory(season: number): Promise<OddsHistory> {
	const { odds: now, gamesFingerprint } = await getSeasonOddsWithFingerprint(season);
	if (!now.available) return { season, available: false, reason: now.reason };

	const R = now.regularSeasonWeeks;
	const played = Math.min(now.throughWeek, R);
	const saved = await savedWeeks(season, played);
	// Which copy of each week, so a week saved again (a new model version, a
	// later live copy) rebuilds the history.
	const savedKey = [...saved]
		.sort((a, b) => a[0] - b[0])
		.map(([week, rows]) => `${week}=${rows[0]?.snapshotId}`)
		.join(',');
	const key = `${season}:${gamesFingerprint}:${now.bracketWeek}:${now.champion}:${savedKey}`;
	const hit = cache.get(key);
	if (hit) {
		cache.delete(key);
		cache.set(key, hit);
		return hit;
	}

	const points = new Map<number, OddsHistoryPoint[]>(now.teams.map((t) => [t.teamKey, []]));
	const rebuiltWeeks: number[] = [];
	for (let week = 1; week <= played; week++) {
		const rows = saved.get(week);
		if (rows && rows.length > 0) {
			for (const r of rows) {
				points.get(Number(r.teamKey))?.push({
					week,
					playoffOdds: Number(r.playoff),
					byeOdds: Number(r.bye),
					titleOdds: Number(r.title),
					clinched: r.clinched === true,
					eliminated: r.eliminated === true,
					// Saved forecasts are regular-season ones: nobody's out of a bracket yet.
					knockedOut: false
				});
			}
			continue;
		}
		const odds = await getSeasonOdds(season, week, { bracket: false, previous: false });
		if (!odds.available || odds.throughWeek !== week) continue;
		rebuiltWeeks.push(week);
		for (const t of odds.teams) {
			points.get(t.teamKey)?.push({
				week,
				playoffOdds: t.playoffOdds,
				byeOdds: t.byeOdds,
				titleOdds: t.titleOdds,
				clinched: t.clinched,
				eliminated: t.eliminated,
				knockedOut: t.knockedOut
			});
		}
	}

	// Title odds after each playoff week that has a decided bracket game.
	const playoffWeeks: number[] = [];
	if (now.bracketWeek != null) {
		for (let week = R + 1; week <= now.bracketWeek; week++) {
			const odds = await getSeasonOdds(season, week, { previous: false });
			if (!odds.available || odds.bracketWeek !== week) continue;
			playoffWeeks.push(week);
			for (const t of odds.teams) {
				points.get(t.teamKey)?.push({
					week,
					playoffOdds: t.playoffOdds,
					byeOdds: t.byeOdds,
					titleOdds: t.titleOdds,
					clinched: t.clinched,
					eliminated: t.eliminated,
					knockedOut: t.knockedOut
				});
			}
		}
	}

	const result = await seasonField(season, now, R);

	const history: OddsHistory = {
		season,
		available: true,
		regularSeasonWeeks: R,
		playoffTeams: now.playoffTeams,
		byes: now.byes,
		weeks: [
			...[...new Set([...points.values()].flatMap((p) => p.map((x) => x.week)))]
				.filter((w) => w <= R)
				.sort((a, b) => a - b),
			...playoffWeeks
		],
		playoffWeeks,
		rebuiltWeeks,
		teams: now.teams.map((t) => ({
			teamKey: t.teamKey,
			teamName: t.teamName,
			managerName: t.managerName,
			points: (points.get(t.teamKey) ?? []).sort((a, b) => a.week - b.week)
		})),
		result: { ...result, champion: now.champion }
	};
	if (cache.size >= CACHE_SIZE) cache.delete(cache.keys().next().value!);
	cache.set(key, history);
	return history;
}
