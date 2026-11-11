/**
 * Pure rules behind Draft Central's display (WP8 S8.4 to S8.8): grade chip
 * colors, the draft value bar, the board's columns in draft order, tag counts
 * and the callouts' wording. The analysis itself stays in $lib/draft-value.
 */

import { pickVerdict, type PickVerdict } from '$lib/draft-value';
import { formatSigned } from '$lib/utils/managers';

/* ---------- Grades ---------- */

/** A color token's name, used as `var(--color-<name>)`. */
export type ColorToken =
	| 'field'
	| 'field-2'
	| 'field-tint'
	| 'clubhouse'
	| 'wash'
	| 'ink'
	| 'ink-2'
	| 'muted'
	| 'pennant'
	| 'pennant-dark'
	| 'pennant-tint'
	| 'white';

export interface ChipColors {
	bg: ColorToken;
	fg: ColorToken;
}

/**
 * A grade chip's fill and text (the career table, the board's column heads,
 * the selected pick): A field with white, B field-tint with clubhouse, C wash
 * with ink-2, D pennant-tint with pennant-dark, F pennant with white. A plus
 * follows its letter. Null for no grade.
 */
export function gradeChip(grade: string | null | undefined): ChipColors | null {
	switch (String(grade ?? '').charAt(0).toUpperCase()) {
		case 'A':
			return { bg: 'field', fg: 'white' };
		case 'B':
			return { bg: 'field-tint', fg: 'clubhouse' };
		case 'C':
			return { bg: 'wash', fg: 'ink-2' };
		case 'D':
			return { bg: 'pennant-tint', fg: 'pennant-dark' };
		case 'F':
			return { bg: 'pennant', fg: 'white' };
		default:
			return null;
	}
}

/**
 * A pick grade's text color on a board cell: A+ and A field, B+ field-2 (the
 * board draws #2E6B52, which has no token: asked in the PR), B ink-2, C+ and C
 * muted, D pennant-dark, F pennant.
 */
export function pickGradeColor(grade: string | null | undefined): ColorToken {
	switch (grade) {
		case 'A+':
		case 'A':
			return 'field';
		case 'B+':
			return 'field-2';
		case 'B':
			return 'ink-2';
		case 'C+':
		case 'C':
			return 'muted';
		case 'D':
			return 'pennant-dark';
		case 'F':
			return 'pennant';
		default:
			return 'ink-2';
	}
}

/**
 * A signed number to `places` decimals: "+0.94", "-0.37". It rounds before it
 * signs, so a value that rounds to zero reads "0.00", never "-0.00" or
 * "+0.0". One place goes through formatSigned, the managers pages' rule.
 */
export function signed(v: number, places: number): string {
	if (places === 1) return formatSigned(v);
	const r = Number(v.toFixed(places));
	const text = Math.abs(r).toFixed(places);
	return r > 0 ? `+${text}` : r < 0 ? `-${text}` : text;
}

export interface ValueBar {
	/** Where the fill starts, as a percent of the track. */
	left: number;
	/** The fill's width, as a percent of the track. */
	width: number;
	tone: 'field' | 'pennant';
	text: string;
}

/**
 * The draft value bar: the fill starts at the center tick and runs right in
 * field for a value of 0 or more, left in pennant below it, half the track
 * per average pick and never past the end.
 */
export function draftValueBar(v: number): ValueBar {
	const width = Math.min(Math.abs(v), 1) * 50;
	// A value that prints as 0.00 reads as level, in field.
	const up = Number(v.toFixed(2)) >= 0;
	return {
		left: up ? 50 : 50 - width,
		width,
		tone: up ? 'field' : 'pennant',
		text: signed(v, 2)
	};
}

/* ---------- Positions ---------- */

export type PositionKey = 'qb' | 'rb' | 'wr' | 'te' | 'k' | 'def';

/** The position's token suffix, or null for one outside the six (the 2005 IDP picks). */
export function positionKey(position: string | null | undefined): PositionKey | null {
	const p = String(position ?? '').trim().toUpperCase();
	switch (p) {
		case 'QB':
		case 'RB':
		case 'WR':
		case 'TE':
		case 'K':
			return p.toLowerCase() as PositionKey;
		case 'DEF':
		case 'D/ST':
		case 'DEFENSE':
			return 'def';
		default:
			return null;
	}
}

/* ---------- The board's tags ---------- */

export type BoardTag = 'steal' | 'value' | 'letdown' | 'bust';

export const BOARD_TAGS: { key: BoardTag; label: string }[] = [
	{ key: 'steal', label: 'Steal' },
	{ key: 'value', label: 'Value' },
	{ key: 'letdown', label: 'Letdown' },
	{ key: 'bust', label: 'Bust' }
];

/** A verdict's tag on the board; a fair pick (or no verdict) gets none. */
export function boardTag(verdict: PickVerdict | null): BoardTag | null {
	switch (verdict) {
		case 'steal':
			return 'steal';
		case 'good':
			return 'value';
		case 'disappointing':
			return 'letdown';
		case 'bust':
			return 'bust';
		default:
			return null;
	}
}

export interface BoardPick {
	overallPick: number;
	roundNumber: number;
	managerName: string;
	playerName?: string;
	primaryPosition?: string | null;
	seasonPoints?: number | string | null;
	expectedPoints?: number | null;
	valueOverExpected?: number | null;
	highExpectation?: boolean;
}

export const pointsOf = (pick: BoardPick): number => Number(pick.seasonPoints ?? 0) || 0;

/** A pick's tag, from the same verdict as every count (so K and DEF never get one). */
export function pickTag(pick: BoardPick): BoardTag | null {
	return boardTag(
		pickVerdict(pointsOf(pick), pick.expectedPoints, pick.highExpectation ?? false, pick.primaryPosition)
	);
}

/** How many picks carry each tag. Kickers and defenses are never counted. */
export function tagCounts(picks: BoardPick[]): Record<BoardTag, number> {
	const counts: Record<BoardTag, number> = { steal: 0, value: 0, letdown: 0, bust: 0 };
	for (const p of picks) {
		const tag = pickTag(p);
		if (tag) counts[tag] += 1;
	}
	return counts;
}

/* ---------- The board's columns ---------- */

export interface BoardLayout<T extends BoardPick> {
	/** The managers, in round 1's pick order. */
	columns: string[];
	/** One row per round, its cells in column order (null where a slot is empty). */
	rows: { round: number; cells: (T | null)[] }[];
}

/**
 * The draft as a grid: one column per manager in round 1's pick order, each
 * round's picks under their manager. In a snake draft that puts round 2's
 * picks 20 down to 11 left to right. A pick that can't sit under its own
 * manager (a traded pick leaves one manager two picks in a round) fills the
 * round's empty slots in pick order, so no pick is dropped.
 */
export function boardColumns<T extends BoardPick>(picks: T[]): BoardLayout<T> {
	const sorted = [...picks].sort((a, b) => a.overallPick - b.overallPick);
	const columns: string[] = [];
	for (const p of sorted) {
		if (p.roundNumber === 1 && !columns.includes(p.managerName)) columns.push(p.managerName);
	}
	const rounds = [...new Set(sorted.map((p) => p.roundNumber))].sort((a, b) => a - b);
	const rows = rounds.map((round) => {
		const inRound = sorted.filter((p) => p.roundNumber === round);
		const cells: (T | null)[] = columns.map(() => null);
		const left: T[] = [];
		for (const p of inRound) {
			const i = columns.indexOf(p.managerName);
			if (i >= 0 && cells[i] == null) cells[i] = p;
			else left.push(p);
		}
		for (const p of left) {
			const i = cells.indexOf(null);
			if (i >= 0) cells[i] = p;
			else cells.push(p);
		}
		return { round, cells };
	});
	return { columns, rows };
}

/* ---------- The callouts ---------- */

export interface BustRate {
	name: string;
	busts: number;
	picks: number;
}

/** Whether `name` has the lowest bust rate (busts / picks) of `managers`; a tie counts. */
export function hasLowestBustRate(name: string, managers: BustRate[]): boolean {
	const rate = (m: BustRate) => (m.picks > 0 ? m.busts / m.picks : Infinity);
	const mine = managers.find((m) => m.name === name);
	if (!mine || !(mine.picks > 0)) return false;
	return managers.every((m) => rate(mine) <= rate(m));
}

/** "64% hit rate, lowest bust rate", the second part only when it's true. */
export function consistencyDetail(hitRate: number, lowestBustRate: boolean): string {
	return lowestBustRate ? `${hitRate}% hit rate, lowest bust rate` : `${hitRate}% hit rate`;
}
