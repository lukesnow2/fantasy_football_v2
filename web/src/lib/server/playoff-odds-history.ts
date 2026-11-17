import { sql } from 'drizzle-orm';
import { db } from '$lib/server/db';
import { compareSeeding } from '$lib/playoff-odds';
import { getSeasonOdds, getSeasonOddsWithFingerprint } from '$lib/server/playoff-odds-service';
import type { OddsHistory, OddsHistoryPoint } from '$lib/odds-history';

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

/** Built histories, by everything that changes them. Small: one or two seasons get asked for. */
const cache = new Map<string, OddsHistory>();

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
				SELECT p.through_week AS week, t.team_key, t.playoff_odds::float AS playoff,
				       t.bye_odds::float AS bye, t.title_odds::float AS title
				FROM pick p
				JOIN model.playoff_odds_snapshot_teams t USING (snapshot_id)
			`)
		) as Row[];
	} catch (err) {
		// A warehouse without the model schema (migration 015) has nothing saved.
		console.error('[odds-history] saved forecasts unavailable:', err);
		return out;
	}
	for (const r of rows) {
		const w = Number(r.week);
		if (!out.has(w)) out.set(w, []);
		out.get(w)!.push(r);
	}
	return out;
}

export async function getOddsHistory(season: number): Promise<OddsHistory> {
	const { odds: now, gamesFingerprint } = await getSeasonOddsWithFingerprint(season);
	if (!now.available) return { season, available: false, reason: now.reason };

	const R = now.regularSeasonWeeks;
	const played = Math.min(now.throughWeek, R);
	const saved = await savedWeeks(season, played);
	const savedKey = [...saved.keys()].sort((a, b) => a - b).join(',');
	const key = `${season}:${gamesFingerprint}:${now.bracketWeek}:${now.champion}:${savedKey}`;
	const hit = cache.get(key);
	if (hit) return hit;

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
					titleOdds: Number(r.title)
				});
			}
			continue;
		}
		const odds = await getSeasonOdds(season, week, { bracket: false });
		if (!odds.available || odds.throughWeek !== week) continue;
		rebuiltWeeks.push(week);
		for (const t of odds.teams) {
			points.get(t.teamKey)?.push({
				week,
				playoffOdds: t.playoffOdds,
				byeOdds: t.byeOdds,
				titleOdds: t.titleOdds
			});
		}
	}

	// Title odds after each playoff week that has a decided bracket game.
	const playoffWeeks: number[] = [];
	if (now.bracketWeek != null) {
		for (let week = R + 1; week <= now.bracketWeek; week++) {
			const odds = await getSeasonOdds(season, week);
			if (!odds.available || odds.bracketWeek !== week) continue;
			playoffWeeks.push(week);
			for (const t of odds.teams) {
				points.get(t.teamKey)?.push({
					week,
					playoffOdds: t.playoffOdds,
					byeOdds: t.byeOdds,
					titleOdds: t.titleOdds
				});
			}
		}
	}

	// The field and byes once the regular season is over: the same seeding the
	// Playoff Field card shows.
	const settled = now.phase === 'postseason' && now.throughWeek >= R;
	const seeded = settled ? [...now.teams].sort(compareSeeding).map((t) => t.teamKey) : null;

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
		result: {
			field: seeded ? seeded.slice(0, now.playoffTeams) : null,
			byes: seeded ? seeded.slice(0, now.byes) : null,
			champion: now.champion
		}
	};
	if (cache.size >= 8) cache.clear();
	cache.set(key, history);
	return history;
}
