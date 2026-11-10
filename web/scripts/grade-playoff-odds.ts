// Grade the saved playoff-odds forecasts (model.playoff_odds_snapshots)
// against what happened. Read-only.
//
// For each model version, live forecasts (taken the week they were shown) and
// rebuilt ones (filled in later) are graded separately: only the live ones
// are a true out-of-sample record. Where a week was saved more than once
// (stat corrections), the last save counts. A season is graded once its
// playoffs start (field, byes, top seed) and again once the title game is
// played; until then its forecasts wait.
//
// Usage (from web/):
//   DATABASE_URL=... npx vite-node --options.transformMode.ssr='.*' \
//     scripts/grade-playoff-odds.ts [--season 2026] [--version <hash>]

import { sql } from 'drizzle-orm';
import { db } from '$lib/server/db';
import {
	OUTCOMES,
	calibration,
	grade,
	scoreForecasts,
	seasonResult,
	type Forecast,
	type Grade,
	type SeasonGame,
	type SeasonResult
} from '$lib/playoff-odds-grading';

type Row = Record<string, unknown>;

function arg(name: string): string | undefined {
	const i = process.argv.indexOf(name);
	return i >= 0 ? process.argv[i + 1] : undefined;
}

const pct = (x: number) => `${(x * 100).toFixed(1)}%`.padStart(6);
const num = (x: number) => x.toFixed(4);

function line(label: string, g: Grade | null): string {
	if (!g) return `  ${label.padEnd(10)} not settled yet`;
	return (
		`  ${label.padEnd(10)} n ${String(g.n).padStart(5)}  brier ${num(g.brier)}` +
		`  log loss ${num(g.logLoss)}  baseline ${num(g.baselineBrier)}  skill ${pct(g.skill)}`
	);
}

async function main() {
	const season = arg('--season') ? Number(arg('--season')) : null;
	if (season != null && !Number.isInteger(season))
		throw new Error(`Bad --season: ${arg('--season')}`);
	const version = arg('--version') ?? null;

	const rows = Array.from(
		await db.execute(sql`
			WITH latest AS (
				SELECT DISTINCT ON (season_year, through_week, model_version, captured_live) *
				FROM model.playoff_odds_snapshots
				WHERE (${season}::int IS NULL OR season_year = ${season}::int)
				  AND (${version}::text IS NULL OR model_version = ${version}::text)
				ORDER BY season_year, through_week, model_version, captured_live, captured_at DESC
			),
			-- When each version was first saved, over every save, not just the
			-- latest copy of each week.
			versions AS (
				SELECT model_version, MIN(captured_at) AS first_seen
				FROM model.playoff_odds_snapshots
				GROUP BY model_version
			),
			-- Each season's regular-season length as most recently saved: early
			-- snapshots can carry an inferred length the stored settings replaced.
			lengths AS (
				SELECT DISTINCT ON (season_year) season_year, regular_season_weeks AS season_weeks
				FROM model.playoff_odds_snapshots
				ORDER BY season_year, captured_at DESC
			)
			SELECT s.season_year, s.through_week, s.playoff_teams, s.byes, l.season_weeks,
			       s.model_version, s.captured_live, v.first_seen,
			       COUNT(*) OVER (PARTITION BY s.snapshot_id) AS teams,
			       t.team_id, t.playoff_odds::float, t.bye_odds::float, t.top_seed_odds::float,
			       t.title_odds::float
			FROM latest s
			JOIN versions v USING (model_version)
			JOIN lengths l USING (season_year)
			JOIN model.playoff_odds_snapshot_teams t USING (snapshot_id)
		`)
	) as Row[];
	if (rows.length === 0) {
		console.log('No saved forecasts match.');
		return;
	}

	const seasons = [...new Set(rows.map((r) => Number(r.seasonYear)))];
	const games = Array.from(
		await db.execute(sql`
			SELECT fm.season_year, dw.week_number AS week, ta.team_id AS a, tb.team_id AS b,
			       fm.team1_points::float AS sa, fm.team2_points::float AS sb,
			       fm.is_playoffs AS po, fm.is_consolation AS cons, fm.is_championship AS champ,
			       tw.team_id AS winner
			FROM edw.fact_matchup fm
			JOIN edw.dim_week dw ON dw.week_key = fm.week_key
			JOIN edw.dim_team ta ON ta.team_key = fm.team1_key
			JOIN edw.dim_team tb ON tb.team_key = fm.team2_key
			LEFT JOIN edw.dim_team tw ON tw.team_key = fm.winner_team_key
			WHERE fm.season_year IN (${sql.join(
				seasons.map((s) => sql`${s}`),
				sql`, `
			)})
			  AND fm.team1_points IS NOT NULL AND fm.team2_points IS NOT NULL
		`)
	) as Row[];

	const regularWeeks = new Map(rows.map((r) => [Number(r.seasonYear), Number(r.seasonWeeks)]));
	const results = new Map<number, SeasonResult>();
	for (const s of seasons) {
		const seasonGames: SeasonGame[] = games
			.filter((g) => Number(g.seasonYear) === s)
			.map((g) => ({
				week: Number(g.week),
				teamA: String(g.a),
				teamB: String(g.b),
				scoreA: Number(g.sa),
				scoreB: Number(g.sb),
				isPlayoffs: Boolean(g.po),
				isConsolation: Boolean(g.cons),
				isChampionship: Boolean(g.champ),
				winner: g.winner == null ? null : String(g.winner)
			}));
		results.set(s, seasonResult(seasonGames, regularWeeks.get(s)!));
	}

	const groups = new Map<
		string,
		{ version: string; live: boolean; firstSeen: Date; forecasts: Forecast[] }
	>();
	for (const r of rows) {
		const key = `${r.modelVersion}:${r.capturedLive}`;
		if (!groups.has(key))
			groups.set(key, {
				version: String(r.modelVersion),
				live: Boolean(r.capturedLive),
				firstSeen: new Date(String(r.firstSeen)),
				forecasts: []
			});
		groups.get(key)!.forecasts.push({
			season: Number(r.seasonYear),
			throughWeek: Number(r.throughWeek),
			// The season's real length, not the one this forecast assumed:
			// whether the standings were final, and how many weeks were left,
			// are facts about the season.
			regularSeasonWeeks: Number(r.seasonWeeks),
			teams: Number(r.teams),
			playoffTeams: Number(r.playoffTeams),
			byes: Number(r.byes),
			teamId: String(r.teamId),
			p: {
				playoffs: Number(r.playoffOdds),
				bye: Number(r.byeOdds),
				topSeed: Number(r.topSeedOdds),
				title: Number(r.titleOdds)
			}
		});
	}

	const settled = seasons
		.sort((a, b) => a - b)
		.map((s) => {
			const r = results.get(s)!;
			return `${s} ${r.champion ? 'final' : r.playoffs ? 'field set' : 'in progress'}`;
		});
	console.log(`Seasons: ${settled.join(', ')}\n`);

	const ordered = [...groups.values()].sort(
		(a, b) => +a.firstSeen - +b.firstSeen || Number(b.live) - Number(a.live)
	);
	for (const g of ordered) {
		const weeks = new Set(g.forecasts.map((f) => `${f.season}:${f.throughWeek}`)).size;
		console.log(
			`Model ${g.version} (first saved ${g.firstSeen.toISOString().slice(0, 10)}), ` +
				`${g.live ? 'live' : 'rebuilt'}: ${weeks} week(s), ${g.forecasts.length} team forecasts`
		);
		const scored = scoreForecasts(g.forecasts, results);
		for (const outcome of OUTCOMES)
			console.log(line(outcome, grade(scored.filter((s) => s.outcome === outcome))));

		const playoffs = scored.filter((s) => s.outcome === 'playoffs');
		if (playoffs.length > 0) {
			console.log('\n  playoff odds calibration\n  forecast       n   mean forecast   made it');
			for (const b of calibration(playoffs))
				console.log(
					`  ${`${Math.round(b.from * 100)}-${Math.round(b.to * 100)}%`.padEnd(9)} ${String(b.n).padStart(5)}  ${pct(b.meanForecast).padStart(14)}  ${pct(b.observed).padStart(8)}`
				);
			console.log('\n  playoff odds by weeks left');
			const left = new Map<number, typeof playoffs>();
			for (const s of playoffs) {
				const k = s.forecast.regularSeasonWeeks - s.forecast.throughWeek;
				if (!left.has(k)) left.set(k, []);
				left.get(k)!.push(s);
			}
			for (const [k, list] of [...left].sort((a, b) => b[0] - a[0]))
				console.log(line(`${k} left`, grade(list)));
		}
		console.log('');
	}
}

main()
	.then(() => process.exit(0))
	.catch((e) => {
		console.error(e);
		process.exit(1);
	});
