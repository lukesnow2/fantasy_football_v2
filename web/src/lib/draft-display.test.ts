import { describe, expect, it } from 'vitest';
import {
	boardCell,
	boardColumns,
	cellTagCounts,
	pickMeta,
	boardTag,
	consistencyDetail,
	draftValueBar,
	gradeChip,
	hasLowestBustRate,
	pickGradeColor,
	pickTag,
	signed,
	positionKey,
	tagCounts,
	type BoardPick
} from './draft-display';

describe('gradeChip', () => {
	it('colors each letter as the board does', () => {
		expect(gradeChip('A')).toEqual({ bg: 'field', fg: 'white' });
		expect(gradeChip('B')).toEqual({ bg: 'field-tint', fg: 'clubhouse' });
		expect(gradeChip('C')).toEqual({ bg: 'wash', fg: 'ink-2' });
		expect(gradeChip('D')).toEqual({ bg: 'pennant-tint', fg: 'pennant-dark' });
		expect(gradeChip('F')).toEqual({ bg: 'pennant', fg: 'white' });
	});

	it('gives a plus its letter, and nothing for no grade', () => {
		expect(gradeChip('A+')).toEqual(gradeChip('A'));
		expect(gradeChip('B+')).toEqual(gradeChip('B'));
		expect(gradeChip('C+')).toEqual(gradeChip('C'));
		expect(gradeChip(null)).toBeNull();
		expect(gradeChip('—')).toBeNull();
	});
});

describe('pickGradeColor', () => {
	it('follows the board cell colors', () => {
		expect(pickGradeColor('A+')).toBe('field');
		expect(pickGradeColor('B+')).toBe('field-2');
		expect(pickGradeColor('B')).toBe('ink-2');
		expect(pickGradeColor('C+')).toBe('muted');
		expect(pickGradeColor('D')).toBe('pennant-dark');
		expect(pickGradeColor('F')).toBe('pennant');
	});
});

describe('draftValueBar', () => {
	it('runs right in field from the center for +0.94', () => {
		expect(draftValueBar(0.94)).toEqual({ left: 50, width: 47, tone: 'field', text: '+0.94' });
	});

	it('runs left in pennant for -0.37', () => {
		const bar = draftValueBar(-0.37);
		expect(bar.tone).toBe('pennant');
		expect(bar.text).toBe('-0.37');
		expect(bar.width).toBeCloseTo(18.5, 9);
		expect(bar.left).toBeCloseTo(31.5, 9);
	});

	it('caps values beyond 1 at half the track', () => {
		expect(draftValueBar(2.6)).toMatchObject({ left: 50, width: 50 });
		expect(draftValueBar(-1.4)).toMatchObject({ left: 0, width: 50 });
	});

	it('reads zero as field with no sign', () => {
		expect(draftValueBar(0)).toEqual({ left: 50, width: 0, tone: 'field', text: '0.00' });
	});
});

/** A 10-team, 3-round snake draft. */
function snake(teams = 10, rounds = 3): BoardPick[] {
	const managers = Array.from({ length: teams }, (_, i) => `M${i + 1}`);
	const picks: BoardPick[] = [];
	for (let r = 1; r <= rounds; r++) {
		const order = r % 2 === 1 ? managers : [...managers].reverse();
		order.forEach((m, i) => {
			picks.push({ overallPick: (r - 1) * teams + i + 1, roundNumber: r, managerName: m });
		});
	}
	return picks;
}

describe('boardColumns', () => {
	it("orders the columns by round 1's picks", () => {
		const { columns } = boardColumns(snake().reverse());
		expect(columns).toEqual(['M1', 'M2', 'M3', 'M4', 'M5', 'M6', 'M7', 'M8', 'M9', 'M10']);
	});

	it('runs round 2 of a 10-team snake from 20 down to 11, left to right', () => {
		const { rows } = boardColumns(snake());
		expect(rows[1].round).toBe(2);
		expect(rows[1].cells.map((c) => c?.overallPick)).toEqual([20, 19, 18, 17, 16, 15, 14, 13, 12, 11]);
		expect(rows[2].cells.map((c) => c?.overallPick)).toEqual([21, 22, 23, 24, 25, 26, 27, 28, 29, 30]);
	});

	it('keeps a traded pick on the board', () => {
		const picks = snake(4, 2);
		// M1 traded their round 2 pick (8) to M2, who now picks 7 and 8.
		picks.find((p) => p.overallPick === 8)!.managerName = 'M2';
		const { rows } = boardColumns(picks);
		expect(rows[1].cells.map((c) => c?.overallPick).sort()).toEqual([5, 6, 7, 8]);
	});
});

describe('tags', () => {
	it('maps verdicts to the four tags, and fair to none', () => {
		expect(boardTag('steal')).toBe('steal');
		expect(boardTag('good')).toBe('value');
		expect(boardTag('disappointing')).toBe('letdown');
		expect(boardTag('bust')).toBe('bust');
		expect(boardTag('fair')).toBeNull();
		expect(boardTag(null)).toBeNull();
	});

	const pick = (position: string, seasonPoints: number, highExpectation = true): BoardPick => ({
		overallPick: 1,
		roundNumber: 1,
		managerName: 'M1',
		primaryPosition: position,
		seasonPoints,
		expectedPoints: 100,
		highExpectation
	});

	it('counts each tag and skips kickers and defenses', () => {
		const picks = [
			pick('RB', 200),
			pick('K', 200),
			pick('WR', 120),
			pick('DEF', 120),
			pick('QB', 70),
			pick('TE', 40),
			pick('DEF', 0),
			pick('K', 0),
			pick('WR', 100)
		];
		expect(tagCounts(picks)).toEqual({ steal: 1, value: 1, letdown: 1, bust: 1 });
		expect(pickTag(pick('K', 300))).toBeNull();
		expect(pickTag(pick('D/ST', 0))).toBeNull();
	});
});

describe('positionKey', () => {
	it('maps the six positions and every defense spelling', () => {
		expect(positionKey('QB')).toBe('qb');
		expect(positionKey('DEF')).toBe('def');
		expect(positionKey('D/ST')).toBe('def');
		expect(positionKey('DEFENSE')).toBe('def');
		expect(positionKey('DL')).toBeNull();
	});
});

describe('the lowest bust rate check', () => {
	const managers = [
		{ name: 'Gabe the Younger', busts: 7, picks: 240 },
		{ name: 'Troy Colvin', busts: 7, picks: 240 },
		{ name: 'Gabe Flores', busts: 19, picks: 300 }
	];

	it('keeps "lowest bust rate" only when it is', () => {
		expect(hasLowestBustRate('Gabe the Younger', managers)).toBe(true);
		expect(hasLowestBustRate('Gabe Flores', managers)).toBe(false);
		expect(consistencyDetail(64, true)).toBe('64% hit rate, lowest bust rate');
		expect(consistencyDetail(60, false)).toBe('60% hit rate');
	});

	it('is false for a manager not in the list', () => {
		expect(hasLowestBustRate('Bobby', managers)).toBe(false);
	});
});

describe('signed', () => {
	it('signs to the places asked', () => {
		expect(signed(0.94, 2)).toBe('+0.94');
		expect(signed(-0.37, 2)).toBe('-0.37');
		expect(signed(199.94, 1)).toBe('+199.9');
		expect(signed(-173.7, 1)).toBe('-173.7');
	});

	it('reads a value that rounds to zero as level, never "-0.0"', () => {
		expect(signed(-0.003, 2)).toBe('0.00');
		expect(signed(0.004, 2)).toBe('0.00');
		expect(signed(-0.04, 1)).toBe('0.0');
		expect(draftValueBar(-0.003)).toMatchObject({ tone: 'field', text: '0.00' });
	});
});

describe('boardCell', () => {
	const base = { overallPick: 87, roundNumber: 9, managerName: 'Craig', playerName: 'Dak Prescott' };

	it('works out the grade, tag, colors and text once', () => {
		const c = boardCell({ ...base, primaryPosition: 'QB', seasonPoints: 466.9, expectedPoints: 267, highExpectation: false });
		expect(c).toEqual({
			grade: 'A+',
			tag: 'steal',
			tagLabel: 'Steal',
			bg: 'var(--color-pos-qb-tint)',
			bar: 'var(--color-pos-qb)',
			slot: '87 · QB',
			label: 'Pick 87, Dak Prescott, QB, grade A+'
		});
	});

	it('leaves out a missing position instead of printing "null"', () => {
		const c = boardCell({ ...base, primaryPosition: null, seasonPoints: 100, expectedPoints: null });
		expect(c.slot).toBe('87');
		expect(c.label).toBe('Pick 87, Dak Prescott, grade none');
		expect(c.bg).toBe('var(--color-wash)');
		expect(pickMeta({ ...base, primaryPosition: null })).toBe('R9 · #87 · Craig');
		expect(pickMeta({ ...base, primaryPosition: 'QB' })).toBe('R9 · #87 · QB · Craig');
	});

	it('counts tags from the cells the same as from the picks', () => {
		const picks = [
			{ ...base, primaryPosition: 'RB', seasonPoints: 200, expectedPoints: 100, highExpectation: true },
			{ ...base, overallPick: 88, primaryPosition: 'K', seasonPoints: 200, expectedPoints: 100, highExpectation: true },
			{ ...base, overallPick: 89, primaryPosition: 'TE', seasonPoints: 40, expectedPoints: 100, highExpectation: true }
		];
		expect(cellTagCounts(picks.map(boardCell))).toEqual(tagCounts(picks));
	});
});
