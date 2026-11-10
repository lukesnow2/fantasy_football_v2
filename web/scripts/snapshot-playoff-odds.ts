// Save the playoff-odds forecast to model.playoff_odds_snapshots (migration
// 015) so it can be graded once the season plays out.
//
// The odds come from the same getSeasonOdds() the site calls, so a snapshot
// is exactly what the site showed. For the season (default: the latest), it
// saves:
//   - the current week, whenever its games changed since the last snapshot
//     (a new week, or a stat correction). Marked captured_live when the
//     season is the one being played.
//   - every earlier week of the season this model version has no snapshot
//     for, rebuilt from the games through that week. That fills the season
//     in on the first run, and again under each new model version, so
//     versions can be compared week for week. Marked not captured_live.
//
// The model version is a hash of the model's source, so changing the model
// starts a new version on its own. Safe to re-run: unchanged weeks are
// skipped.
//
// Usage (from web/; the ssr transform mode lets the server modules load):
//   DATABASE_URL=... npx vite-node --options.transformMode.ssr='.*' \
//     scripts/snapshot-playoff-odds.ts [--season 2026] [--dry-run]
//
// The weekly pipeline workflow runs it after each load.

import { createHash } from 'node:crypto';
import { execSync } from 'node:child_process';
import fs from 'node:fs';
import { sql } from 'drizzle-orm';
import { db } from '$lib/server/db';
import { getSeasonOdds, latestSeason, type SeasonOdds } from '$lib/server/playoff-odds-service';
import { DEFAULT_PARAMS } from '$lib/playoff-odds';

type Row = Record<string, unknown>;
type Available = Extract<SeasonOdds, { available: true }>;

const MODEL_SOURCES = ['../src/lib/playoff-odds.ts', '../src/lib/server/playoff-odds-service.ts'];

/** First 12 hex of a SHA-256 over the model's source files. */
export function modelVersion(): string {
	const h = createHash('sha256');
	for (const rel of MODEL_SOURCES) h.update(fs.readFileSync(new URL(rel, import.meta.url)));
	return h.digest('hex').slice(0, 12);
}

function gitSha(): string | null {
	if (process.env.GITHUB_SHA) return process.env.GITHUB_SHA;
	try {
		return execSync('git rev-parse HEAD', { stdio: ['ignore', 'pipe', 'ignore'] })
			.toString()
			.trim();
	} catch {
		return null;
	}
}

function arg(name: string): string | undefined {
	const i = process.argv.indexOf(name);
	return i >= 0 ? process.argv[i + 1] : undefined;
}

async function saveSnapshot(
	odds: Available,
	opts: { version: string; capturedLive: boolean; sha: string | null; teamIds: Map<number, string> }
): Promise<boolean> {
	const teams = odds.teams.map((t) => {
		const teamId = opts.teamIds.get(t.teamKey);
		// The stable key is what grading joins on; a row without it can't be
		// graded, so refuse rather than store one.
		if (!teamId) throw new Error(`No Yahoo team id for team_key ${t.teamKey} (${t.teamName})`);
		return {
			team_id: teamId,
			team_key: t.teamKey,
			team_name: t.teamName,
			manager_name: t.managerName,
			wins: t.wins,
			losses: t.losses,
			ties: t.ties,
			points_for: t.pointsFor,
			projected_points: t.projectedPoints,
			projected_wins: t.projectedWins,
			remaining_games: t.remainingGames,
			playoff_odds: t.playoffOdds,
			bye_odds: t.byeOdds,
			top_seed_odds: t.topSeedOdds,
			title_odds: t.titleOdds,
			clinched: t.clinched,
			eliminated: t.eliminated
		};
	});
	const params = { ...DEFAULT_PARAMS };

	return db.transaction(async (tx) => {
		const inserted = (await tx.execute(sql`
			INSERT INTO model.playoff_odds_snapshots (
				season_year, through_week, regular_season_weeks, playoff_teams, byes,
				format_source, sims, league_average, model_version, model_params,
				games_fingerprint, captured_live, git_sha
			) VALUES (
				${odds.season}, ${odds.throughWeek}, ${odds.regularSeasonWeeks}, ${odds.playoffTeams},
				${odds.byes}, ${odds.formatSource}, ${odds.sims}, ${odds.leagueAverage},
				${opts.version}, ${JSON.stringify(params)}::jsonb, ${odds.gamesFingerprint},
				${opts.capturedLive}, ${opts.sha}
			)
			ON CONFLICT ON CONSTRAINT playoff_odds_snapshots_once DO NOTHING
			RETURNING snapshot_id
		`)) as Row[];
		const id = inserted[0]?.snapshotId;
		if (id == null) return false;
		await tx.execute(sql`
			INSERT INTO model.playoff_odds_snapshot_teams
			SELECT ${id}::bigint, t.*
			FROM jsonb_to_recordset(${JSON.stringify(teams)}::jsonb) AS t(
				team_id text, team_key integer, team_name text, manager_name text,
				wins integer, losses integer, ties integer, points_for numeric,
				projected_points numeric, projected_wins numeric, remaining_games integer,
				playoff_odds numeric, bye_odds numeric, top_seed_odds numeric, title_odds numeric,
				clinched boolean, eliminated boolean
			)
		`);
		return true;
	});
}

async function main() {
	const dryRun = process.argv.includes('--dry-run');
	const newest = await latestSeason();
	const season = Number(arg('--season') ?? newest);
	if (!Number.isInteger(season)) throw new Error(`Bad --season: ${arg('--season')}`);
	const version = modelVersion();
	const sha = gitSha();

	const latest = await getSeasonOdds(season);
	if (!latest.available) {
		console.log(`${season}: no forecast to save (${latest.reason})`);
		return;
	}
	const current = latest.throughWeek;
	if (current < 1) {
		console.log(`${season}: no regular-season games played yet; nothing to save.`);
		return;
	}

	const [existingRows, teamRows] = await Promise.all([
		db.execute(sql`
			SELECT through_week, games_fingerprint
			FROM model.playoff_odds_snapshots
			WHERE season_year = ${season} AND model_version = ${version}
		`),
		db.execute(sql`
			SELECT dt.team_key, dt.team_id
			FROM edw.dim_team dt
			JOIN edw.dim_league dl ON dl.league_key = dt.league_key
			WHERE dl.season_year = ${season}
		`)
	]);
	const saved = new Map<number, Set<string>>();
	for (const r of Array.from(existingRows) as Row[]) {
		const w = Number(r.throughWeek);
		if (!saved.has(w)) saved.set(w, new Set());
		saved.get(w)!.add(String(r.gamesFingerprint));
	}
	const teamIds = new Map(
		(Array.from(teamRows) as Row[]).map((r) => [Number(r.teamKey), String(r.teamId)])
	);

	console.log(
		`${season}: model ${version}, through week ${current} of ${latest.regularSeasonWeeks}` +
			(dryRun ? ' (dry run)' : '')
	);
	let written = 0;
	for (let week = 1; week <= current; week++) {
		// Only the newest season's current week is what the site is showing
		// now; a past season's weeks are all rebuilt.
		const live = week === current && season === newest;
		// Earlier weeks: one rebuild per version is enough. The current week:
		// a new snapshot whenever its games changed.
		if (!live && saved.has(week)) continue;
		const odds = live ? latest : await getSeasonOdds(season, week);
		// A league week with no games (2006 and 2007 started in week 2) has
		// nothing new to forecast from.
		if (!odds.available || odds.throughWeek !== week) continue;
		if (saved.get(week)?.has(odds.gamesFingerprint)) continue;

		const line = odds.teams
			.map((t) => `${t.teamName.slice(0, 14)} ${(t.playoffOdds * 100).toFixed(0)}%`)
			.join(', ');
		console.log(`  week ${week} ${live ? 'live    ' : 'rebuilt '} ${line}`);
		if (dryRun) continue;
		if (await saveSnapshot(odds, { version, capturedLive: live, sha, teamIds })) written++;
	}
	console.log(dryRun ? 'Dry run: nothing written.' : `Saved ${written} snapshot(s).`);
}

main()
	.then(() => process.exit(0))
	.catch((e) => {
		console.error(e);
		process.exit(1);
	});
