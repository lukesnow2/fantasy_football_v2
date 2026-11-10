// Overfitting checks for the playoff-odds model.
//
//  1. Leave-one-season-out: for each season, choose the parameters that score
//     best on the other seasons, then score the held-out season with them.
//     If that out-of-sample Brier is close to the in-sample one, the tuning
//     isn't fitting noise.
//  2. Per-season edge over record-only, with a season-level bootstrap CI.
//  3. How flat the parameter surface is around the chosen point.
//
// Usage (from web/): npx vite-node scripts/backtest-overfit.ts
// Reads DATABASE_URL from .env (the local warehouse copy). Read-only.

import fs from 'node:fs';
import postgres from 'postgres';
import { simulatePlayoffOdds, DEFAULT_PARAMS, type ModelParams, type PlayedGame } from '../src/lib/playoff-odds';

const raw = fs.readFileSync(new URL('../.env', import.meta.url), 'utf8');
const url = raw.match(/^DATABASE_URL=(.*)$/m)?.[1]?.trim().replace(/^"|"$/g, '');
if (!url) throw new Error('DATABASE_URL not found in .env');
const isLocal = /@(localhost|127\.0\.0\.1)[:/]/.test(url);
const sql = postgres(url, { ssl: isLocal ? false : 'require', max: 1 });

interface Season { season: number; games: PlayedGame[]; playoff: Set<number>; weeks: number; teams: number[] }

const rows = await sql`
	SELECT fm.season_year AS season, dw.week_number AS week, fm.team1_key AS a, fm.team2_key AS b,
	       fm.team1_points::float AS sa, fm.team2_points::float AS sb,
	       fm.is_playoffs AS po, fm.is_consolation AS cons
	FROM edw.fact_matchup fm JOIN edw.dim_week dw ON dw.week_key = fm.week_key
	WHERE fm.season_year BETWEEN 2008 AND 2025 ORDER BY 1, 2`;
await sql.end();
const by = new Map<number, Season>();
for (const r of rows) {
	const s = Number(r.season);
	if (!by.has(s)) by.set(s, { season: s, games: [], playoff: new Set(), weeks: 0, teams: [] });
	const x = by.get(s)!;
	if (r.po) { x.playoff.add(Number(r.a)); x.playoff.add(Number(r.b)); }
	else if (!r.cons) x.games.push({ week: Number(r.week), teamA: Number(r.a), teamB: Number(r.b), scoreA: Number(r.sa), scoreB: Number(r.sb) });
}
const seasons = [...by.values()];
for (const x of seasons) {
	x.weeks = Math.max(...x.games.map((g) => g.week));
	x.teams = [...new Set(x.games.flatMap((g) => [g.teamA, g.teamB]))].sort((a, b) => a - b);
}

/** Sum of squared errors and count per season, weeks 3+ with 2+ left. */
function score(params: Partial<ModelParams>, sims: number): Map<number, { se: number; n: number }> {
	const out = new Map<number, { se: number; n: number }>();
	for (const s of seasons) {
		let se = 0, n = 0;
		for (let k = 3; k <= s.weeks - 2; k++) {
			const res = simulatePlayoffOdds({ teamKeys: s.teams, games: s.games.filter((g) => g.week <= k), regularSeasonWeeks: s.weeks, sims, seed: s.season * 100 + k, params });
			for (const t of res.teams) { se += (t.playoffOdds - (s.playoff.has(t.teamKey) ? 1 : 0)) ** 2; n++; }
		}
		out.set(s.season, { se, n });
	}
	return out;
}
const brierOf = (m: Map<number, { se: number; n: number }>, keep: (y: number) => boolean) => {
	let se = 0, n = 0;
	for (const [y, v] of m) if (keep(y)) { se += v.se; n += v.n; }
	return se / n;
};

const SIMS = 1500;
const grid: ModelParams[] = [];
for (const decay of [1, 0.95, 0.9])
	for (const shrinkage of [1, 2, 3, 5, 8, 12])
		for (const drift of [0, 0.15, 0.3, 0.45])
			for (const volatilityPrior of [4, 8, 1e9]) grid.push({ decay, shrinkage, drift, volatilityPrior });

console.log(`Scoring ${grid.length} parameter sets x ${seasons.length} seasons at ${SIMS} sims...`);
const t0 = Date.now();
const scored = grid.map((p) => ({ p, s: score(p, SIMS) }));
console.log(`done in ${((Date.now() - t0) / 1000).toFixed(0)}s\n`);

const def = score(DEFAULT_PARAMS, SIMS);
const rec = score({ shrinkage: 1e9, drift: 0, volatilityPrior: 1e9 }, SIMS);
const all = () => true;

// 1. Leave-one-season-out.
let loSe = 0, loN = 0;
const picks: string[] = [];
for (const s of seasons) {
	const best = scored.reduce((a, b) => (brierOf(b.s, (y) => y !== s.season) < brierOf(a.s, (y) => y !== s.season) ? b : a));
	const v = best.s.get(s.season)!;
	loSe += v.se; loN += v.n;
	picks.push(`${s.season}: ${JSON.stringify(best.p)}`);
}
const inSampleBest = scored.reduce((a, b) => (brierOf(b.s, all) < brierOf(a.s, all) ? b : a));
console.log('1. Leave-one-season-out');
console.log(`   best grid point, in-sample (all seasons):   ${brierOf(inSampleBest.s, all).toFixed(4)}  ${JSON.stringify(inSampleBest.p)}`);
console.log(`   shipped defaults, in-sample:                ${brierOf(def, all).toFixed(4)}`);
console.log(`   tuned without the scored season (honest):   ${(loSe / loN).toFixed(4)}`);
console.log(`   record + schedule only (no tuning):         ${brierOf(rec, all).toFixed(4)}`);
console.log('   params picked per held-out season:');
for (const p of picks) console.log('     ' + p);

// 2. Per-season edge and bootstrap over seasons.
console.log('\n2. Edge over record-only, by season (negative = model better)');
const diffs = seasons.map((s) => {
	const d = def.get(s.season)!, r = rec.get(s.season)!;
	return { y: s.season, d: d.se / d.n - r.se / r.n, n: d.n, seD: d.se, seR: r.se };
});
for (const x of diffs) console.log(`   ${x.y}  ${x.d >= 0 ? '+' : ''}${x.d.toFixed(4)}`);
console.log(`   model better in ${diffs.filter((x) => x.d < 0).length} of ${diffs.length} seasons`);
let seed = 12345;
const rand = () => ((seed = (seed * 1103515245 + 12345) >>> 0) / 4294967296);
const boots: number[] = [];
for (let b = 0; b < 5000; b++) {
	let sd = 0, sr = 0, n = 0;
	for (let i = 0; i < diffs.length; i++) { const x = diffs[Math.floor(rand() * diffs.length)]; sd += x.seD; sr += x.seR; n += x.n; }
	boots.push((sd - sr) / n);
}
boots.sort((a, b) => a - b);
const lo = boots[Math.floor(0.025 * boots.length)], hi = boots[Math.floor(0.975 * boots.length)];
console.log(`   Brier difference ${(brierOf(def, all) - brierOf(rec, all)).toFixed(4)}, 95% CI over seasons [${lo.toFixed(4)}, ${hi.toFixed(4)}]`);

// 3. Flatness.
const sorted = [...scored].sort((a, b) => brierOf(a.s, all) - brierOf(b.s, all));
console.log('\n3. Parameter surface (all seasons): best 8 and worst 3');
for (const x of [...sorted.slice(0, 8), ...sorted.slice(-3)]) console.log(`   ${brierOf(x.s, all).toFixed(4)}  ${JSON.stringify(x.p)}`);
const within = sorted.filter((x) => brierOf(x.s, all) - brierOf(sorted[0].s, all) < 0.001).length;
console.log(`   ${within} of ${sorted.length} settings within 0.001 of the best`);
