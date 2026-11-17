/**
 * The playoff odds week by week (/api/playoff-odds/history) and the grade the
 * odds get once a season is over. Pure, so vitest holds the rules; the server
 * side is $lib/server/playoff-odds-history.
 */

import { formatOdds } from '$lib/home';

/**
 * The first season with odds: the backtest's first (2008). The page's season
 * picker and both odds routes start here.
 */
export const FIRST_ODDS_SEASON = 2008;

export interface OddsHistoryPoint {
	week: number;
	playoffOdds: number;
	byeOdds: number;
	titleOdds: number;
	/** Certain to make the playoffs, not just 100% in the simulation. */
	clinched: boolean;
	/** Certain to miss them. */
	eliminated: boolean;
	/** Lost a championship-bracket game. */
	knockedOut: boolean;
}

export interface OddsHistoryTeam {
	teamKey: number;
	teamName: string;
	managerName: string | null;
	points: OddsHistoryPoint[];
}

export type OddsHistory =
	| { season: number; available: false; reason: string }
	| {
			season: number;
			available: true;
			regularSeasonWeeks: number;
			playoffTeams: number;
			byes: number;
			/** Every week with odds: regular-season weeks, then playoff weeks. */
			weeks: number[];
			/** The playoff weeks in `weeks`, where only the title odds still move. */
			playoffWeeks: number[];
			/** Weeks the site never saved, rebuilt from that week's games. */
			rebuiltWeeks: number[];
			teams: OddsHistoryTeam[];
			/** Null until it's known. */
			result: { field: number[] | null; byes: number[] | null; champion: number | null };
	  };

export type OddsMetric = 'playoffOdds' | 'byeOdds' | 'titleOdds';

type Available = Extract<OddsHistory, { available: true }>;

/** The weeks a metric's chart covers: the title keeps moving through the bracket. */
export function chartWeeks(h: Available, metric: OddsMetric): number[] {
	return metric === 'titleOdds' ? h.weeks : h.weeks.filter((w) => w <= h.regularSeasonWeeks);
}

/* ---------- The playoff field ---------- */

/**
 * The playoff field and byes from the championship-bracket games played so
 * far, as scripts/grade-playoff-odds.ts reads them: the field is every team
 * in a bracket game, and the byes are the field's teams that sat out its
 * first week. Only played weeks are stored, so after the first round the
 * teams on a bye haven't played yet: the top of `seeded` (wins, then points
 * for) not already in a game fills the field to `playoffTeams`.
 */
export function bracketField(
	games: { week: number; teamA: number; teamB: number }[],
	seeded: number[],
	playoffTeams: number
): { field: number[]; byes: number[] } {
	const field = [...new Set(games.flatMap((g) => [g.teamA, g.teamB]))];
	for (const t of seeded) {
		if (field.length >= playoffTeams) break;
		if (!field.includes(t)) field.push(t);
	}
	const first = Math.min(...games.map((g) => g.week));
	const playedFirst = new Set(
		games.filter((g) => g.week === first).flatMap((g) => [g.teamA, g.teamB])
	);
	return { field, byes: field.filter((t) => !playedFirst.has(t)) };
}

/* ---------- The grade ---------- */

export interface SeasonGrade {
	/** The first week from which every playoff team had better odds than every team that missed. */
	fieldFrom: number | null;
	halfway: { week: number; hits: number; of: number } | null;
	/** The champion's title odds when the bracket was set, and where they ranked. */
	champion: { name: string; odds: number; rank: number } | null;
	/** The title favorite when the bracket was set, when that wasn't the champion. */
	favorite: { name: string; odds: number } | null;
	/** The halfway forecast furthest from what happened, when it was on the wrong side of 50%. */
	surprise: { name: string; week: number; odds: number; madeIt: boolean } | null;
	/** Brier skill of the regular-season playoff odds against an even guess. */
	skill: number | null;
	/** That even guess: playoff spots over teams. */
	evenGuess: number;
}

const nameOf = (t: OddsHistoryTeam) => t.managerName ?? 'Unknown manager';

function at(t: OddsHistoryTeam, week: number): OddsHistoryPoint | undefined {
	return t.points.find((p) => p.week === week);
}

export function seasonGrade(h: Available): SeasonGrade | null {
	const field = h.result.field ? new Set(h.result.field) : null;
	if (!field) return null;
	const R = h.regularSeasonWeeks;
	const regular = h.weeks.filter((w) => w <= R);
	const evenGuess = h.playoffTeams / h.teams.length;

	// Field: the odds split the real field from the rest, every week from then on.
	const splits = (week: number) => {
		let lowIn = Infinity;
		let highOut = -Infinity;
		for (const t of h.teams) {
			const p = at(t, week)?.playoffOdds;
			if (p == null) return false;
			if (field.has(t.teamKey)) lowIn = Math.min(lowIn, p);
			else highOut = Math.max(highOut, p);
		}
		return lowIn > highOut;
	};
	let fieldFrom: number | null = null;
	for (let i = regular.length - 1; i >= 0 && splits(regular[i]); i--) fieldFrom = regular[i];

	// Halfway: the top of the odds against the real field.
	const half = [...regular].reverse().find((w) => w <= Math.ceil(R / 2)) ?? null;
	let halfway: SeasonGrade['halfway'] = null;
	let surprise: SeasonGrade['surprise'] = null;
	if (half != null) {
		const ranked = h.teams
			.map((t) => ({ t, p: at(t, half)?.playoffOdds ?? 0 }))
			.sort((a, b) => b.p - a.p);
		const top = ranked.slice(0, h.playoffTeams);
		halfway = {
			week: half,
			hits: top.filter((x) => field.has(x.t.teamKey)).length,
			of: h.playoffTeams
		};
		let worst = 0.5;
		for (const { t, p } of ranked) {
			const madeIt = field.has(t.teamKey);
			const miss = Math.abs((madeIt ? 1 : 0) - p);
			if (miss > worst) {
				worst = miss;
				surprise = { name: nameOf(t), week: half, odds: p, madeIt };
			}
		}
	}

	// The title, as the bracket was set: the last regular-season week.
	let champion: SeasonGrade['champion'] = null;
	let favorite: SeasonGrade['favorite'] = null;
	if (h.result.champion != null && regular.includes(R)) {
		const ranked = h.teams
			.map((t) => ({ t, p: at(t, R)?.titleOdds ?? 0 }))
			.sort((a, b) => b.p - a.p);
		const i = ranked.findIndex((x) => x.t.teamKey === h.result.champion);
		if (i >= 0) {
			// Shared odds share a rank.
			const rank = ranked.filter((x) => x.p > ranked[i].p).length + 1;
			champion = { name: nameOf(ranked[i].t), odds: ranked[i].p, rank };
			if (rank > 1) favorite = { name: nameOf(ranked[0].t), odds: ranked[0].p };
		}
	}

	// Skill: the regular season before the standings were final.
	let brier = 0;
	let base = 0;
	let n = 0;
	for (const t of h.teams) {
		const y = field.has(t.teamKey) ? 1 : 0;
		for (const p of t.points) {
			if (p.week >= R) continue;
			brier += (p.playoffOdds - y) ** 2;
			base += (evenGuess - y) ** 2;
			n++;
		}
	}
	const skill = n > 0 && base > 0 ? 1 - brier / base : null;

	return { fieldFrom, halfway, champion, favorite, surprise, skill, evenGuess };
}

/* ---------- The grade, in words ---------- */

export interface GradeLine {
	id: string;
	value: string;
	note: string;
}

// Counts in a sentence: 'none of the six', 'two of the six'.
const WORDS = [
	'none',
	'one',
	'two',
	'three',
	'four',
	'five',
	'six',
	'seven',
	'eight',
	'nine',
	'ten'
];
const word = (n: number) => WORDS[n] ?? String(n);
const pct = (p: number) => formatOdds(p, false);

export function ordinal(n: number): string {
	const s = ['th', 'st', 'nd', 'rd'];
	const v = n % 100;
	return n + (s[(v - 20) % 10] || s[v] || s[0]);
}

export function gradeLines(g: SeasonGrade, regularSeasonWeeks: number): GradeLine[] {
	const lines: GradeLine[] = [];
	if (g.champion) {
		lines.push({
			id: 'champion',
			value: `${pct(g.champion.odds)} for the champ`,
			note:
				g.champion.rank === 1
					? `${g.champion.name} had the best title odds when the bracket was set.`
					: `${g.champion.name} had the ${ordinal(g.champion.rank)}-best title odds when the bracket was set${g.favorite ? `. ${g.favorite.name} was the favorite at ${pct(g.favorite.odds)}` : ''}.`
		});
	}
	if (g.fieldFrom != null) {
		lines.push({
			id: 'field',
			value:
				g.fieldFrom >= regularSeasonWeeks
					? `Field settled in wk ${g.fieldFrom}`
					: `Field called by wk ${g.fieldFrom}`,
			note:
				g.fieldFrom >= regularSeasonWeeks
					? "The playoff teams didn't all have better odds than the rest until the final week."
					: `From week ${g.fieldFrom} on, every playoff team had better odds than every team that missed.`
		});
	}
	if (g.halfway) {
		lines.push({
			id: 'halfway',
			value: `${g.halfway.hits} of ${g.halfway.of} at halfway`,
			note: `After week ${g.halfway.week}, ${g.halfway.hits === g.halfway.of ? 'all' : word(g.halfway.hits)} of the ${word(g.halfway.of)} teams with the best odds made the playoffs.`
		});
	}
	if (g.surprise) {
		lines.push({
			id: 'surprise',
			value: `Biggest miss: ${g.surprise.name}`,
			note: `Had ${pct(g.surprise.odds)} playoff odds after week ${g.surprise.week} and ${g.surprise.madeIt ? 'made it anyway' : 'missed'}.`
		});
	}
	return lines;
}

/** The footnote on how far off the odds were overall. */
export function skillLine(g: SeasonGrade): string | null {
	if (g.skill == null) return null;
	const even = pct(g.evenGuess);
	if (g.skill <= 0)
		return `Over the regular season the odds did no better than giving every team the same ${even} chance.`;
	return `Over the regular season the odds had ${Math.round(g.skill * 100)}% less error than giving every team the same ${even} chance (Brier score).`;
}

/**
 * The note under the chart when some weeks weren't saved at the time. Seasons
 * before the snapshots began (2026) are rebuilt whole.
 */
export function rebuiltNote(h: Available): string | null {
	const rebuilt = h.rebuiltWeeks;
	const regular = h.weeks.filter((w) => w <= h.regularSeasonWeeks);
	if (rebuilt.length === 0) return null;
	if (rebuilt.length >= regular.length)
		return "These odds weren't saved at the time, so every week is rebuilt from that week's games with today's model.";
	if (rebuilt.length === 1)
		return `Week ${rebuilt[0]} wasn't saved at the time, so it's rebuilt from that week's games.`;
	const list = `${rebuilt.slice(0, -1).join(', ')} and ${rebuilt[rebuilt.length - 1]}`;
	return `Weeks ${list} weren't saved at the time, so they're rebuilt from those weeks' games.`;
}

/* ---------- The chart's rules ---------- */

/**
 * The week a team's line stops: the first week from which its odds are
 * settled for good, at a value that doesn't change again. Null while they're
 * still open. Settled means certain, not a simulated 0% or 100%: clinched or
 * eliminated, the regular season over (for the playoffs and the bye), knocked
 * out of the bracket or the champion (for the title). After a season every
 * line ends at 0% or 100%, so the end says nothing; where it stops does.
 */
export function decidedWeek(
	h: Available,
	team: OddsHistoryTeam,
	metric: OddsMetric
): number | null {
	const R = h.regularSeasonWeeks;
	const inView = new Set(chartWeeks(h, metric));
	const points = team.points.filter((p) => inView.has(p.week));
	const settled = (p: OddsHistoryPoint) => {
		const v = p[metric];
		const final = p.week >= R && (v === 0 || v === 1);
		if (metric === 'playoffOdds')
			return (p.clinched && v === 1) || (p.eliminated && v === 0) || final;
		if (metric === 'byeOdds') return (p.eliminated && v === 0) || final;
		return (
			((p.eliminated || p.knockedOut) && v === 0) || (v === 1 && h.result.champion === team.teamKey)
		);
	};
	let week: number | null = null;
	for (let i = points.length - 1; i >= 0; i--) {
		const p = points[i];
		if (!settled(p)) break;
		if (i < points.length - 1 && p[metric] !== points[i + 1][metric]) break;
		week = p.week;
	}
	return week;
}

/**
 * Spreads labels that want the same spot along one axis: each keeps its
 * wanted position when it can, and a crowd is laid out `gap` apart, centered
 * on where its members wanted to be and kept between `lo` and `hi`. Returns
 * positions in the input's order.
 */
export function dodge(wanted: number[], gap: number, lo: number, hi: number): number[] {
	type Cluster = { members: { want: number; i: number }[]; start: number };
	const place = (c: Cluster) => {
		const n = c.members.length;
		const mean = c.members.reduce((s, m) => s + m.want, 0) / n;
		const start = mean - ((n - 1) * gap) / 2;
		c.start = Math.max(lo, Math.min(start, hi - (n - 1) * gap));
	};
	const clusters: Cluster[] = wanted
		.map((want, i) => ({ want, i }))
		.sort((a, b) => a.want - b.want)
		.map((m) => ({ members: [m], start: m.want }));
	clusters.forEach(place);
	for (let k = 0; k < clusters.length - 1; ) {
		const a = clusters[k];
		const b = clusters[k + 1];
		if (a.start + a.members.length * gap > b.start + 1e-9) {
			a.members.push(...b.members);
			clusters.splice(k + 1, 1);
			place(a);
			k = Math.max(0, k - 1);
		} else k++;
	}
	const out = new Array<number>(wanted.length);
	for (const c of clusters) c.members.forEach((m, j) => (out[m.i] = c.start + j * gap));
	return out;
}

const CEILINGS = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.8];

/**
 * The y axis: 0 to 100%, except title odds before the bracket is on the
 * chart, which never get near 100% (about 30% at most in a season) and would
 * otherwise sit in the bottom quarter. Those stop at the round number above
 * the highest value. Zero always stays at the bottom: 0% is "no chance".
 */
export function oddsAxis(h: Available, metric: OddsMetric): { max: number; ticks: number[] } {
	const full = { max: 1, ticks: [0, 0.25, 0.5, 0.75, 1] };
	if (metric !== 'titleOdds' || h.playoffWeeks.length > 0) return full;
	const high = Math.max(0, ...h.teams.flatMap((t) => t.points.map((p) => p.titleOdds)));
	const max = CEILINGS.find((c) => c >= high + 0.02);
	if (max == null) return full;
	const step = max <= 0.2 ? 0.05 : max <= 0.6 ? 0.1 : 0.2;
	const ticks: number[] = [];
	for (let t = 0; t <= max + 1e-9; t += step) ticks.push(Math.round(t * 100) / 100);
	return { max, ticks };
}
