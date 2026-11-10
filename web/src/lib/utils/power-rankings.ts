/**
 * Pure rules behind the Power Rankings page and the luck card (WP5), kept out
 * of the components so tests hold what the page says (build spec G9, S5.9).
 */
import { summarizeLuck, type SeasonLuck } from '$lib/manager-luck';

/** The parts of a power-rankings row the callouts read. */
export interface CalloutRow {
	powerRank: number;
	rankChange: number | null;
	luckFactor: number | null;
}

export interface RankingCallouts<Row extends CalloutRow> {
	riser: Row | null;
	faller: Row | null;
	luckiest: Row | null;
}

/**
 * The three tiles above the table (S5.2), from the selected week's rows:
 * the largest climb, the largest drop and the largest luck. Ties go to the
 * manager ranked higher this week. Week 1 (every change null) and a week with
 * no movement leave the riser and faller empty; null luck is skipped.
 */
export function rankingCallouts<Row extends CalloutRow>(rankings: Row[]): RankingCallouts<Row> {
	const byRank = [...rankings].sort((a, b) => a.powerRank - b.powerRank);
	// The first row that beats every row above it wins, so a tie keeps the
	// higher-ranked manager.
	const best = (score: (row: Row) => number | null): Row | null => {
		let pick: Row | null = null;
		let top = -Infinity;
		for (const row of byRank) {
			const value = score(row);
			if (value == null || !Number.isFinite(value)) continue;
			if (value > top) {
				pick = row;
				top = value;
			}
		}
		return pick;
	};
	return {
		riser: best((r) => (r.rankChange != null && r.rankChange > 0 ? r.rankChange : null)),
		faller: best((r) => (r.rankChange != null && r.rankChange < 0 ? -r.rankChange : null)),
		luckiest: best((r) => r.luckFactor)
	};
}

/** The power score bar's width in percent: the score held to 0 to 1, as a whole number. */
export function powerBarPct(powerScore: number): number {
	if (!Number.isFinite(powerScore)) return 0;
	return Math.round(Math.min(1, Math.max(0, powerScore)) * 100);
}

/** A value rounded to `places`, with negative zero folded into zero. */
function rounded(value: number, places: number): number {
	const factor = 10 ** places;
	const r = Math.round(value * factor) / factor;
	return r === 0 ? 0 : r;
}

/**
 * Luck signed to `places`: "+0.39", "-0.14", "0.00". Null when there's no
 * number, which the page renders as EmptyValue "None", never a dash (G6).
 */
export function formatLuck(value: number | null | undefined, places = 2): string | null {
	if (value == null || !Number.isFinite(value)) return null;
	const r = rounded(value, places);
	const text = Math.abs(r).toFixed(places);
	return r > 0 ? `+${text}` : r < 0 ? `-${text}` : text;
}

export type LuckTone = 'up' | 'down' | 'flat';

/**
 * Which way luck leans, by the sign of the number as printed at `places`, so
 * a "0.00" is never colored as lucky. Null is flat.
 */
export function luckTone(value: number | null | undefined, places = 2): LuckTone {
	if (value == null || !Number.isFinite(value)) return 'flat';
	const r = rounded(value, places);
	return r > 0 ? 'up' : r < 0 ? 'down' : 'flat';
}

/**
 * The rank chart's lines, in order of the last week's rank (S5.5). CSS
 * variables, so the colors stay tokens (G2).
 */
export const RANK_LINE_COLORS = [
	'var(--color-field)',
	'var(--color-pennant)',
	'var(--color-pos-rb)',
	'var(--color-pos-te)',
	'var(--color-pos-def)',
	'var(--color-chart-teal)',
	'var(--color-pos-qb)',
	'var(--color-muted)',
	'var(--color-pos-k)',
	'var(--color-chart-rose)'
] as const;

/** The parts of a chart series the label reads. */
export interface RankSeries {
	managerName: string | null;
	points: { week: number; rank: number }[];
}

const SMALL_NUMBERS = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine'];

/** One to nine spelled out, as the board writes "four spots"; 10 and up as digits. */
export function spellNumber(n: number): string {
	return Number.isInteger(n) && n >= 0 && n < 10 ? SMALL_NUMBERS[n] : String(n);
}

/** "A", "A and B", "A, B and C". */
function joinNames(names: string[]): string {
	if (names.length <= 1) return names.join('');
	return `${names.slice(0, -1).join(', ')} and ${names[names.length - 1]}`;
}

/**
 * The rank chart's aria-label: the weeks it covers, then the biggest riser or
 * risers into the last week, when anyone rose. Series come in last-week rank
 * order, so tied names read in rank order.
 */
export function rankChartLabel(series: RankSeries[], weeks: number[]): string {
	if (weeks.length === 0) return 'Power rank by week.';
	const first = weeks[0];
	const last = weeks[weeks.length - 1];
	const opening = `Power rank by week for weeks ${first} to ${last}.`;

	const rises = series.flatMap((s) => {
		const end = s.points.find((p) => p.week === last);
		const before = [...s.points].reverse().find((p) => p.week < last);
		if (!end || !before) return [];
		const rise = before.rank - end.rank;
		return rise > 0 ? [{ name: s.managerName ?? 'Unknown manager', rise }] : [];
	});
	if (rises.length === 0) return opening;

	const top = Math.max(...rises.map((r) => r.rise));
	const names = rises.filter((r) => r.rise === top).map((r) => r.name);
	const spots = top === 1 ? 'spot' : 'spots';
	return `${opening} ${joinNames(names)} rose ${spellNumber(top)} ${spots} in week ${last}.`;
}

/**
 * A manager's luck for the luck card, with career totals from finished
 * seasons only (G8): summarizeLuck sums every season it's given and stays
 * frozen (G3), so the season in progress is held back from the career call.
 * It still shows on its own row, as `currentSeason`, which summarizeLuck
 * builds from the live rows alone.
 */
export function summarizeManagerLuck(seasons: SeasonLuck[], latestSeason: number | null) {
	const career = summarizeLuck(
		seasons.filter((s) => !s.inProgress),
		latestSeason
	);
	const live = seasons.filter((s) => s.inProgress);
	if (live.length === 0) return career;
	return { ...career, currentSeason: summarizeLuck(live, latestSeason).currentSeason };
}

/** "Career, 21 seasons" counts; one season reads "1 season". */
export function seasonCount(n: number): string {
	return `${n} ${n === 1 ? 'season' : 'seasons'}`;
}
