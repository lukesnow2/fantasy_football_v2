// Score Yahoo's own projections against the playoff-odds model.
//
// Input: a CSV of Yahoo's per-team, per-week scoreboard fields (projected
// points and win_probability), pulled read-only from the Yahoo API.
//
//  1. Single games: Yahoo's pre-game win_probability vs the model's, using
//     only games before that week.
//  2. Team strength: which better predicts a team's average score over the
//     rest of the season, relative to the league: the model's estimate, or
//     Yahoo's projection for the next week? Plus a blend, fitted
//     leave-one-season-out.
//  3. Draft day: Yahoo's projected records vs actual wins.
//
// Usage (from web/): npx vite-node scripts/compare-yahoo.ts <yahoo_weekly.csv>

import fs from 'node:fs';
import postgres from 'postgres';
import { estimateStrength, DEFAULT_PARAMS, type PlayedGame } from '../src/lib/playoff-odds';

const csvPath = process.argv[2];
if (!csvPath) throw new Error('usage: compare-yahoo.ts <yahoo_weekly.csv>');
const raw = fs.readFileSync(new URL('../.env', import.meta.url), 'utf8');
const url = raw.match(/^DATABASE_URL=(.*)$/m)?.[1]?.trim().replace(/^"|"$/g, '');
if (!url) throw new Error('DATABASE_URL not found in .env');
const sql = postgres(url, { ssl: /@(localhost|127\.0\.0\.1)[:/]/.test(url) ? false : 'require', max: 1 });

// ---- Yahoo rows
interface Y { pts: number | null; proj: number | null; wp: number | null }
const yahoo = new Map<string, Y>();
const lines = fs.readFileSync(csvPath, 'utf8').trim().split('\n').slice(1);
const num = (s: string) => (s === '' || s === 'None' ? null : Number(s));
for (const l of lines) {
	const [season, week, tk, , po, co, pts, proj, wp] = l.split(',');
	if (po === '1' || co === '1') continue;
	yahoo.set(`${season}:${week}:${tk}`, { pts: num(pts), proj: num(proj), wp: num(wp) });
}

// ---- Warehouse games, with Yahoo team keys
const rows = await sql`
	SELECT fm.season_year AS season, dw.week_number AS week,
	       fm.team1_key AS a, fm.team2_key AS b, ta.team_id AS ya, tb.team_id AS yb,
	       fm.team1_points::float AS sa, fm.team2_points::float AS sb, fm.is_playoffs AS po, fm.is_consolation AS cons
	FROM edw.fact_matchup fm
	JOIN edw.dim_week dw ON dw.week_key = fm.week_key
	JOIN edw.dim_team ta ON ta.team_key = fm.team1_key
	JOIN edw.dim_team tb ON tb.team_key = fm.team2_key
	WHERE fm.season_year BETWEEN 2008 AND 2026 ORDER BY 1, 2`;
const playoffRows = rows.filter((r) => r.po);
await sql.end();

interface G extends PlayedGame { ya: string; yb: string }
const seasons = new Map<number, G[]>();
const playoff = new Map<number, Set<number>>();
for (const r of rows) {
	const s = Number(r.season);
	if (r.po) { if (!playoff.has(s)) playoff.set(s, new Set()); playoff.get(s)!.add(Number(r.a)).add(Number(r.b)); continue; }
	if (r.cons) continue;
	if (!seasons.has(s)) seasons.set(s, []);
	seasons.get(s)!.push({ week: Number(r.week), teamA: Number(r.a), teamB: Number(r.b), scoreA: Number(r.sa), scoreB: Number(r.sb), ya: r.ya, yb: r.yb });
}
void playoffRows;

const Phi = (z: number) => 0.5 * (1 + erf(z / Math.SQRT2));
function erf(x: number) {
	const t = 1 / (1 + 0.3275911 * Math.abs(x));
	const y = 1 - (((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t) * Math.exp(-x * x);
	return x >= 0 ? y : -y;
}
const clip = (p: number) => Math.min(1 - 1e-4, Math.max(1e-4, p));

// ---- Coverage and sanity
console.log('Coverage: share of games with a Yahoo projection / a pre-game win prob (not 0 or 1)');
const usableWp = new Set<number>();
const usableProj = new Set<number>();
for (const [s, games] of [...seasons].sort((a, b) => a[0] - b[0])) {
	let proj = 0, wp = 0, n = 0, mism = 0;
	for (const g of games) {
		const A = yahoo.get(`${s}:${g.week}:${g.ya}`);
		n++;
		if (A?.proj != null && A.proj > 0) proj++;
		if (A?.wp != null && A.wp > 0 && A.wp < 1) wp++;
		if (A?.pts != null && Math.abs(A.pts - g.scoreA) > 0.05) mism++;
	}
	if (proj / n > 0.9) usableProj.add(s);
	if (wp / n > 0.8) usableWp.add(s);
	console.log(`  ${s}: proj ${((100 * proj) / n).toFixed(0)}%  pre-game wp ${((100 * wp) / n).toFixed(0)}%  score mismatches ${mism}`);
}

// ---- 1. Single games
type Rec = { s: number; w: number; y: 0 | 1; ours: number; yh: number };
const recs: Rec[] = [];
for (const [s, games] of seasons) {
	if (!usableWp.has(s)) continue;
	const keys = [...new Set(games.flatMap((g) => [g.teamA, g.teamB]))].sort((a, b) => a - b);
	const idx = new Map(keys.map((k, i) => [k, i]));
	const R = Math.max(...games.map((g) => g.week));
	for (let w = 1; w <= R; w++) {
		const before = games.filter((g) => g.week < w);
		const st = estimateStrength(keys, before, w - 1, DEFAULT_PARAMS);
		const pooled = Math.sqrt(st.noiseSd.reduce((a, x) => a + x * x, 0) / keys.length);
		const drift = DEFAULT_PARAMS.drift * pooled;
		for (const g of games.filter((x) => x.week === w)) {
			if (g.scoreA === g.scoreB) continue;
			const a = idx.get(g.teamA)!, b = idx.get(g.teamB)!;
			const v = st.meanSd[a] ** 2 + st.meanSd[b] ** 2 + st.noiseSd[a] ** 2 + st.noiseSd[b] ** 2 + 2 * drift ** 2;
			const ours = Phi((st.mean[a] - st.mean[b]) / Math.sqrt(v));
			const yh = yahoo.get(`${s}:${w}:${g.ya}`)?.wp;
			if (yh == null || yh <= 0 || yh >= 1) continue;
			recs.push({ s, w, y: g.scoreA > g.scoreB ? 1 : 0, ours, yh });
		}
	}
}
const brier = (xs: Rec[], f: (r: Rec) => number) => xs.reduce((a, r) => a + (f(r) - r.y) ** 2, 0) / xs.length;
const ll = (xs: Rec[], f: (r: Rec) => number) => -xs.reduce((a, r) => a + (r.y ? Math.log(clip(f(r))) : Math.log(1 - clip(f(r)))), 0) / xs.length;
const acc = (xs: Rec[], f: (r: Rec) => number) => xs.filter((r) => (f(r) > 0.5 ? 1 : 0) === r.y).length / xs.length;
const show = (label: string, xs: Rec[]) => {
	const fs_: [string, (r: Rec) => number][] = [
		['coin flip', () => 0.5],
		['our model', (r) => r.ours],
		['Yahoo win_probability', (r) => r.yh],
		['average of the two', (r) => (r.ours + r.yh) / 2]
	];
	console.log(`\n  ${label} (n=${xs.length})`);
	for (const [n, f] of fs_) console.log(`    ${n.padEnd(24)} brier ${brier(xs, f).toFixed(4)}  logloss ${ll(xs, f).toFixed(4)}  picks right ${(100 * acc(xs, f)).toFixed(1)}%`);
};
console.log('\n1. Single games, seasons with pre-game win probabilities: ' + [...usableWp].sort().join(', '));
show('all weeks', recs);
show('weeks 1-3', recs.filter((r) => r.w <= 3));
show('weeks 4+', recs.filter((r) => r.w >= 4));
const yhCal = [0, 0.3, 0.4, 0.5, 0.6, 0.7, 1.0001];
console.log('\n  Yahoo calibration (team A side):');
for (let i = 0; i + 1 < yhCal.length; i++) {
	const b = recs.filter((r) => r.yh >= yhCal[i] && r.yh < yhCal[i + 1]);
	if (b.length) console.log(`    ${(100 * yhCal[i]).toFixed(0)}-${Math.min(100, 100 * yhCal[i + 1]).toFixed(0)}%  n=${b.length}  avg ${(100 * b.reduce((a, r) => a + r.yh, 0) / b.length).toFixed(1)}%  won ${(100 * b.reduce((a, r) => a + r.y, 0) / b.length).toFixed(1)}%`);
}

// ---- 2. Team strength: predict rest-of-season average, centred on the league
type S = { s: number; k: number; target: number; ours: number; yNext: number; yRest: number };
const srecs: S[] = [];
for (const [s, games] of seasons) {
	if (!usableProj.has(s) || s === 2026) continue;
	const keys = [...new Set(games.flatMap((g) => [g.teamA, g.teamB]))].sort((a, b) => a - b);
	const ykey = new Map<number, string>();
	for (const g of games) { ykey.set(g.teamA, g.ya); ykey.set(g.teamB, g.yb); }
	const R = Math.max(...games.map((g) => g.week));
	for (let k = 3; k <= R - 2; k++) {
		const st = estimateStrength(keys, games.filter((g) => g.week <= k), k, DEFAULT_PARAMS);
		const per = keys.map((t, i) => {
			const fut = games.filter((g) => g.week > k && (g.teamA === t || g.teamB === t)).map((g) => (g.teamA === t ? g.scoreA : g.scoreB));
			const projs: number[] = [];
			for (let w = k + 1; w <= R; w++) { const p = yahoo.get(`${s}:${w}:${ykey.get(t)}`)?.proj; if (p != null) projs.push(p); }
			const yNext = yahoo.get(`${s}:${k + 1}:${ykey.get(t)}`)?.proj ?? NaN;
			return { target: fut.reduce((a, x) => a + x, 0) / fut.length, ours: st.mean[i], yNext, yRest: projs.reduce((a, x) => a + x, 0) / projs.length };
		});
		if (per.some((p) => !Number.isFinite(p.yNext))) continue;
		const c = (f: (p: (typeof per)[0]) => number) => { const m = per.reduce((a, p) => a + f(p), 0) / per.length; return per.map((p) => f(p) - m); };
		const T = c((p) => p.target), O = c((p) => p.ours), N = c((p) => p.yNext), Rr = c((p) => p.yRest);
		per.forEach((_, i) => srecs.push({ s, k, target: T[i], ours: O[i], yNext: N[i], yRest: Rr[i] }));
	}
}
const rmse = (xs: S[], f: (r: S) => number) => Math.sqrt(xs.reduce((a, r) => a + (f(r) - r.target) ** 2, 0) / xs.length);
const corr = (xs: S[], f: (r: S) => number) => {
	const n = xs.length, mx = xs.reduce((a, r) => a + f(r), 0) / n, my = xs.reduce((a, r) => a + r.target, 0) / n;
	let sxy = 0, sxx = 0, syy = 0;
	for (const r of xs) { sxy += (f(r) - mx) * (r.target - my); sxx += (f(r) - mx) ** 2; syy += (r.target - my) ** 2; }
	return sxy / Math.sqrt(sxx * syy);
};
// Least squares through the origin on two predictors.
const fit2 = (xs: S[], f1: (r: S) => number, f2: (r: S) => number) => {
	let a11 = 0, a12 = 0, a22 = 0, b1 = 0, b2 = 0;
	for (const r of xs) { const x1 = f1(r), x2 = f2(r); a11 += x1 * x1; a12 += x1 * x2; a22 += x2 * x2; b1 += x1 * r.target; b2 += x2 * r.target; }
	const det = a11 * a22 - a12 * a12;
	return [(b1 * a22 - b2 * a12) / det, (a11 * b2 - a12 * b1) / det];
};
const fit1 = (xs: S[], f: (r: S) => number) => xs.reduce((a, r) => a + f(r) * r.target, 0) / xs.reduce((a, r) => a + f(r) ** 2, 0);
const loso = (fitter: (tr: S[]) => (r: S) => number) => {
	let se = 0;
	for (const s of new Set(srecs.map((r) => r.s))) {
		const f = fitter(srecs.filter((r) => r.s !== s));
		for (const r of srecs.filter((x) => x.s === s)) se += (f(r) - r.target) ** 2;
	}
	return Math.sqrt(se / srecs.length);
};
console.log(`\n2. Team strength: predicting rest-of-season scoring vs the league average, weeks 3 to R-2`);
console.log(`   seasons: ${[...new Set(srecs.map((r) => r.s))].sort().join(', ')}  (n=${srecs.length} team-weeks)`);
console.log(`   "everyone average" baseline  RMSE ${rmse(srecs, () => 0).toFixed(2)}`);
console.log(`   our model                    RMSE ${rmse(srecs, (r) => r.ours).toFixed(2)}  corr ${corr(srecs, (r) => r.ours).toFixed(3)}`);
console.log(`   Yahoo next-week projection   RMSE ${rmse(srecs, (r) => r.yNext).toFixed(2)}  corr ${corr(srecs, (r) => r.yNext).toFixed(3)}  (raw)`);
console.log(`   Yahoo next week, rescaled    RMSE ${loso((tr) => { const b = fit1(tr, (r) => r.yNext); return (r) => b * r.yNext; }).toFixed(2)}  (scale fit leave-one-season-out)`);
console.log(`   blend ours + Yahoo next      RMSE ${loso((tr) => { const [a, b] = fit2(tr, (r) => r.ours, (r) => r.yNext); return (r) => a * r.ours + b * r.yNext; }).toFixed(2)}  (fit leave-one-season-out)  weights ${fit2(srecs, (r) => r.ours, (r) => r.yNext).map((x) => x.toFixed(2)).join(' / ')}`);
console.log(`   Yahoo rest-of-season proj    RMSE ${rmse(srecs, (r) => r.yRest).toFixed(2)}  corr ${corr(srecs, (r) => r.yRest).toFixed(3)}  (LOOKAHEAD: each week's projection was made just before that week; an upper bound for Yahoo)`);
const byK = new Map<number, S[]>();
for (const r of srecs) { if (!byK.has(r.k)) byK.set(r.k, []); byK.get(r.k)!.push(r); }
console.log('   by week (RMSE ours / Yahoo next raw / Yahoo next rescaled):');
const bAll = fit1(srecs, (r) => r.yNext);
for (const [k, xs] of [...byK].sort((a, b) => a[0] - b[0])) console.log(`     wk ${String(k).padStart(2)}  ${rmse(xs, (r) => r.ours).toFixed(2)} / ${rmse(xs, (r) => r.yNext).toFixed(2)} / ${rmse(xs, (r) => bAll * r.yNext).toFixed(2)}`);
