/**
 * Pure rules behind the Trade Center (WP8 S8.1 to S8.3). The trade API reports
 * each side by what it sent away: `teamAGives` is what team A traded, so what
 * team A *got* is `teamBGives`. Every "got" line on the page is the other
 * side's `...Gives`.
 */

import { numberWord } from '$lib/utils/hall-of-fame';

export interface TradeRow {
	seasonYear: number | string;
	transactionWeek: number | string | null;
	teamAManager: string;
	teamBManager: string;
	teamAGives: string | null;
	teamBGives: string | null;
	tradeType?: string | null;
	productionDifferential?: number | string | null;
	teamAProduction?: number | string | null;
	teamBProduction?: number | string | null;
	teamAFinalScore?: number | string | null;
	teamBFinalScore?: number | string | null;
	teamAMadePlayoffs?: number | boolean | null;
	teamBMadePlayoffs?: number | boolean | null;
	teamAChampion?: number | boolean | null;
	teamBChampion?: number | boolean | null;
	tradeWinner?: string | null;
	evaluationStatus?: string | null;
}

export type Side = 'A' | 'B';

const num = (v: number | string | null | undefined): number => {
	const n = Number(v);
	return Number.isFinite(n) ? n : 0;
};

/** One decimal, as every score and margin on the page reads. */
export function oneDecimal(v: number | string | null | undefined): string {
	return num(v).toFixed(1);
}

/** A margin as the board writes it: "+308.9", always the size of the swing. */
export function margin(v: number | string | null | undefined): string {
	return `+${Math.abs(num(v)).toFixed(1)}`;
}

const other = (side: Side): Side => (side === 'A' ? 'B' : 'A');

/** The manager on a side. */
export function managerOf(trade: TradeRow, side: Side): string {
	return side === 'A' ? trade.teamAManager : trade.teamBManager;
}

/** What a side got: the other side's `...Gives`. */
export function gotBy(trade: TradeRow, side: Side): string {
	return (side === 'A' ? trade.teamBGives : trade.teamAGives) ?? '';
}

export function finalScore(trade: TradeRow, side: Side): number {
	return num(side === 'A' ? trade.teamAFinalScore : trade.teamBFinalScore);
}

function production(trade: TradeRow, side: Side): number {
	return num(side === 'A' ? trade.teamAProduction : trade.teamBProduction);
}

function madePlayoffs(trade: TradeRow, side: Side): boolean {
	return Boolean(num((side === 'A' ? trade.teamAMadePlayoffs : trade.teamBMadePlayoffs) as number));
}

/* ---------- S8.1 the numbers ---------- */

/**
 * Trades per season: the API sends no average, so the page divides the total
 * by the seasons in the filter, one decimal. Null with no seasons.
 */
export function perSeason(totalTrades: number | string | null | undefined, seasons: number): string | null {
	if (!seasons) return null;
	return (num(totalTrades) / seasons).toFixed(1);
}

/* ---------- S8.2 Most lopsided trades ---------- */

/** The API's five most decisive trades, largest production swing first. */
export function lopsidedTrades<T extends TradeRow>(bestTrades: T[]): T[] {
	return [...bestTrades].sort(
		(a, b) => Math.abs(num(b.productionDifferential)) - Math.abs(num(a.productionDifferential))
	);
}

/** The winning side: the API's `tradeWinner`, else the higher final score. */
export function winnerSide(trade: TradeRow): Side {
	if (trade.tradeWinner === trade.teamAManager) return 'A';
	if (trade.tradeWinner === trade.teamBManager) return 'B';
	return finalScore(trade, 'A') >= finalScore(trade, 'B') ? 'A' : 'B';
}

export interface LopsidedRow {
	winner: string;
	winnerGot: string;
	loser: string;
	loserGot: string;
	margin: string;
	winnerScore: string;
	loserScore: string;
	share: number;
	when: string;
}

/** One lopsided row, ready to draw. */
export function lopsidedRow(trade: TradeRow): LopsidedRow {
	const w = winnerSide(trade);
	const l = other(w);
	const ws = finalScore(trade, w);
	const ls = finalScore(trade, l);
	return {
		winner: managerOf(trade, w),
		winnerGot: gotBy(trade, w),
		loser: managerOf(trade, l),
		loserGot: gotBy(trade, l),
		margin: margin(trade.productionDifferential),
		winnerScore: ws.toFixed(1),
		loserScore: ls.toFixed(1),
		share: winnerShare(ws, ls),
		when: `Week ${trade.transactionWeek}, ${trade.seasonYear}`
	};
}

/** The winner's share of the two final scores, as a whole percent. */
export function winnerShare(ws: number, ls: number): number {
	const total = ws + ls;
	return total > 0 ? Math.round((ws / total) * 100) : 0;
}

/**
 * "Omar is on the losing end of three of these five." The manager who loses
 * most often among the rows, when that's two or more; on a tie, whoever loses
 * first in the list. Null otherwise.
 */
export function repeatLoserLine(rows: TradeRow[]): string | null {
	const counts = new Map<string, number>();
	for (const t of rows) {
		const loser = managerOf(t, other(winnerSide(t)));
		counts.set(loser, (counts.get(loser) ?? 0) + 1);
	}
	let best: string | null = null;
	let bestCount = 0;
	// Map keeps first-seen order, so a strict > leaves a tie with the first.
	for (const [name, count] of counts) {
		if (count > bestCount) {
			best = name;
			bestCount = count;
		}
	}
	if (!best || bestCount < 2) return null;
	const word = (n: number) => numberWord(n).toLowerCase();
	return `${best} is on the losing end of ${word(bestCount)} of these ${word(rows.length)}.`;
}

/* ---------- S8.3 Trades that won a title ---------- */

/** The champion's side of a title trade: whichever side has the champion flag. */
export function championSide(trade: TradeRow): Side {
	return num(trade.teamAChampion as number) ? 'A' : 'B';
}

/* ---------- S8.3 Recent trades ---------- */

export const EVEN_TRADE = 'Even Trade';

export function inProgress(trade: TradeRow): boolean {
	return trade.evaluationStatus === 'In Progress';
}

export interface ScoreLine {
	text: string;
	tone: 'field' | 'muted';
}

/** A margin this size or more, won on the players' own points, is named. */
export const NAMED_MARGIN = 50;

/**
 * The 13px line under each side of a recent trade. The side with the higher
 * final score is in field and the other in muted. While the trade is in
 * progress the higher side is "leading". Once final, the higher side "won by
 * {n} pts" when its players also scored more and the swing is 50 or more,
 * else "won, made playoffs" when it made the playoffs and the other side
 * didn't, else "won". The lower side is its score alone, and an Even Trade
 * shows both scores in muted with nothing after them.
 */
export function tradeScoreLine(trade: TradeRow, side: Side): ScoreLine {
	const score = finalScore(trade, side).toFixed(1);
	if (trade.tradeWinner === EVEN_TRADE) return { text: score, tone: 'muted' };

	const mine = finalScore(trade, side);
	const theirs = finalScore(trade, other(side));
	if (mine <= theirs) return { text: score, tone: 'muted' };

	if (inProgress(trade)) return { text: `${score} · leading`, tone: 'field' };

	const swing = Math.abs(num(trade.productionDifferential));
	if (production(trade, side) > production(trade, other(side)) && swing >= NAMED_MARGIN) {
		return { text: `${score} · won by ${swing.toFixed(1)} pts`, tone: 'field' };
	}
	if (madePlayoffs(trade, side) && !madePlayoffs(trade, other(side))) {
		return { text: `${score} · won, made playoffs`, tone: 'field' };
	}
	return { text: `${score} · won`, tone: 'field' };
}

/** The three newest trades, or all of them once the list is opened. */
export function recentTrades<T>(trades: T[], expanded: boolean): T[] {
	return expanded ? trades : trades.slice(0, 3);
}
