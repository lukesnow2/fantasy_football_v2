// Backtest the playoff-odds model against every finished season.
//
// For each season 2008-2025 and each week of its regular season, run the model
// on the games played through that week and score its playoff odds against who
// actually made the playoffs. Reports Brier score and log loss per model, a
// calibration table, and an optional grid search over the model parameters.
//
// Usage (from web/):
//   npx vite-node scripts/backtest-playoff-odds.ts            # default params vs baselines
//   npx vite-node scripts/backtest-playoff-odds.ts --grid     # parameter search
//
// Reads DATABASE_URL from .env (the local warehouse copy). Read-only.

import fs from 'node:fs';
import postgres from 'postgres';
import {
	simulatePlayoffOdds,
	DEFAULT_PARAMS,
	type ModelParams,
	type PlayedGame
} from '../src/lib/playoff-odds';

const raw = fs.readFileSync(new URL('../.env', import.meta.url), 'utf8');
const url = raw
	.match(/^DATABASE_URL=(.*)$/m)?.[1]
	?.trim()
	.replace(/^"|"$/g, '');
if (!url) throw new Error('DATABASE_URL not found in .env');
const isLocal = /@(localhost|127\.0\.0\.1)[:/]/.test(url);
const sql = postgres(url, { ssl: isLocal ? false : 'require', max: 1 });

interface Season {
	season: number;
	games: PlayedGame[];
	playoff: Set<number>;
	champion: number | null;
	weeks: number;
	teams: number[];
}

async function load(): Promise<Season[]> {
	const rows = await sql`
		SELECT fm.season_year AS season, dw.week_number AS week,
		       fm.team1_key AS a, fm.team2_key AS b,
		       fm.team1_points::float AS sa, fm.team2_points::float AS sb,
		       fm.is_playoffs AS po, fm.is_consolation AS cons, fm.is_championship AS champ,
		       fm.winner_team_key AS winner
		FROM edw.fact_matchup fm
		JOIN edw.dim_week dw ON dw.week_key = fm.week_key
		WHERE fm.season_year BETWEEN 2008 AND 2025
		ORDER BY 1, 2`;
	const by = new Map<number, Season>();
	for (const r of rows) {
		const s = Number(r.season);
		if (!by.has(s))
			by.set(s, { season: s, games: [], playoff: new Set(), champion: null, weeks: 0, teams: [] });
		const x = by.get(s)!;
		if (r.po) {
			x.playoff.add(Number(r.a));
			x.playoff.add(Number(r.b));
			if (r.champ) x.champion = Number(r.winner);
		} else if (!r.cons) {
			x.games.push({
				week: Number(r.week),
				teamA: Number(r.a),
				teamB: Number(r.b),
				scoreA: Number(r.sa),
				scoreB: Number(r.sb)
			});
		}
	}
	for (const x of by.values()) {
		x.weeks = Math.max(...x.games.map((g) => g.week));
		x.teams = [...new Set(x.games.flatMap((g) => [g.teamA, g.teamB]))].sort((a, b) => a - b);
	}
	return [...by.values()];
}

interface Pred {
	season: number;
	week: number;
	weeksLeft: number;
	p: number;
	y: 0 | 1;
	title: number;
	champ: 0 | 1;
}

function run(seasons: Season[], params: Partial<ModelParams>, sims: number): Pred[] {
	const out: Pred[] = [];
	for (const s of seasons) {
		for (let k = 0; k < s.weeks; k++) {
			const res = simulatePlayoffOdds({
				teamKeys: s.teams,
				games: s.games.filter((g) => g.week <= k),
				regularSeasonWeeks: s.weeks,
				sims,
				seed: s.season * 100 + k,
				params
			});
			for (const t of res.teams) {
				out.push({
					season: s.season,
					week: k,
					weeksLeft: s.weeks - k,
					p: t.playoffOdds,
					y: s.playoff.has(t.teamKey) ? 1 : 0,
					title: t.titleOdds,
					champ: s.champion === t.teamKey ? 1 : 0
				});
			}
		}
	}
	return out;
}

const brier = (ps: Pred[]) => ps.reduce((s, x) => s + (x.p - x.y) ** 2, 0) / ps.length;
const logLoss = (ps: Pred[]) =>
	-ps.reduce((s, x) => {
		const p = Math.min(Math.max(x.p, 0.005), 0.995);
		return s + (x.y ? Math.log(p) : Math.log(1 - p));
	}, 0) / ps.length;
// Exclude week 0 (pure schedule, every model is ~60%) and final-week
// predictions where the answer is nearly settled, to see where models differ.
const mid = (ps: Pred[]) => ps.filter((x) => x.week >= 3 && x.weeksLeft >= 2);

function report(name: string, ps: Pred[]) {
	const m = mid(ps);
	console.log(
		`${name.padEnd(34)} brier ${brier(ps).toFixed(4)}  logloss ${logLoss(ps).toFixed(4)}  | wk3+ w/2+ left: brier ${brier(m).toFixed(4)} logloss ${logLoss(m).toFixed(4)}`
	);
}

function calibration(ps: Pred[]) {
	const buckets = [0, 0.05, 0.15, 0.3, 0.5, 0.7, 0.85, 0.95, 1.0001];
	console.log('\n  predicted     n    avg pred   actual');
	for (let i = 0; i < buckets.length - 1; i++) {
		const b = ps.filter((x) => x.p >= buckets[i] && x.p < buckets[i + 1]);
		if (!b.length) continue;
		const avg = b.reduce((s, x) => s + x.p, 0) / b.length;
		const act = b.reduce((s, x) => s + x.y, 0) / b.length;
		console.log(
			`  ${(buckets[i] * 100).toFixed(0).padStart(3)}-${Math.min(buckets[i + 1] * 100, 100)
				.toFixed(0)
				.padStart(
					3
				)}%  ${String(b.length).padStart(5)}    ${(avg * 100).toFixed(1).padStart(5)}%   ${(act * 100).toFixed(1).padStart(5)}%`
		);
	}
}

// "Record only": every team the same strength, so only the standings and the
// schedule matter. This is the "two wins clear = 90%" style of model.
const RECORD_ONLY: Partial<ModelParams> = { shrinkage: 1e9, drift: 0, volatilityPrior: 1e9 };
// Points with no humility: trust the season average outright.
const NAIVE_POINTS: Partial<ModelParams> = { decay: 1, shrinkage: 0.0001, drift: 0 };

const seasons = await load();
await sql.end();
console.log(`Loaded ${seasons.length} seasons (${seasons[0].season}-${seasons.at(-1)!.season})\n`);

if (process.argv.includes('--grid')) {
	const sims = 1500;
	const results: { params: ModelParams; train: number; test: number }[] = [];
	const train = seasons.filter((s) => s.season <= 2018);
	const test = seasons.filter((s) => s.season > 2018);
	for (const decay of [1, 0.95, 0.9, 0.85]) {
		for (const shrinkage of [2, 4, 7, 11]) {
			for (const drift of [0.1, 0.25, 0.4]) {
				for (const volatilityPrior of [6, 1e9]) {
					const params = { decay, shrinkage, drift, volatilityPrior };
					const tr = mid(run(train, params, sims));
					const te = mid(run(test, params, sims));
					results.push({ params, train: brier(tr), test: brier(te) });
				}
			}
		}
	}
	results.sort((a, b) => a.train - b.train);
	console.log('Top 15 by train Brier (2008-2018), with held-out 2019-2025 Brier:');
	for (const r of results.slice(0, 15))
		console.log(JSON.stringify(r.params), r.train.toFixed(4), r.test.toFixed(4));
} else {
	const sims = 4000;
	const model = run(seasons, DEFAULT_PARAMS, sims);
	report('Model (default params)', model);
	report('Baseline: record + schedule only', run(seasons, RECORD_ONLY, sims));
	report('Baseline: raw points, no humility', run(seasons, NAIVE_POINTS, sims));

	console.log('\nModel calibration, all weeks 3+ with 2+ left:');
	calibration(mid(model));

	const byWeeksLeft = new Map<number, Pred[]>();
	for (const x of model) {
		if (!byWeeksLeft.has(x.weeksLeft)) byWeeksLeft.set(x.weeksLeft, []);
		byWeeksLeft.get(x.weeksLeft)!.push(x);
	}
	console.log('\nModel Brier by weeks remaining:');
	for (const [w, ps] of [...byWeeksLeft].sort((a, b) => a[0] - b[0])) {
		console.log(`  ${String(w).padStart(2)} left: ${brier(ps).toFixed(4)}`);
	}

	const titles = model.filter((x) => x.season >= 2010);
	const titleBrier = titles.reduce((s, x) => s + (x.title - x.champ) ** 2, 0) / titles.length;
	const titleBase = titles.reduce((s, x) => s + (0.1 - x.champ) ** 2, 0) / titles.length;
	console.log(
		`\nTitle odds Brier 2010-2025: ${titleBrier.toFixed(4)} (uniform 10% baseline ${titleBase.toFixed(4)})`
	);
}
