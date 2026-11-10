import { describe, expect, it } from 'vitest';
import { countWord, startHistory, type StartHistoryRow } from './start-history';

function row(
	season: number,
	wins: number,
	losses: number,
	ties = 0,
	made = false
): StartHistoryRow {
	return { season, managerName: 'Someone', wins, losses, ties, madePlayoffs: made };
}

const rows: StartHistoryRow[] = [
	row(2005, 3, 0, 0, true),
	row(2006, 3, 0, 0, true),
	row(2007, 3, 0, 0, false),
	row(2008, 3, 0, 1, true),
	row(2009, 2, 1),
	row(2024, 0, 3)
];

describe('startHistory', () => {
	it('counts an exact record match only: 3-0 is not 3-0-1', () => {
		expect(startHistory(rows, { wins: 3, losses: 0, ties: 0 })).toMatchObject({
			count: 3,
			madePlayoffs: 2
		});
		expect(startHistory(rows, { wins: 3, losses: 0, ties: 1 }).count).toBe(1);
	});

	it('reports the first season the rows cover, matched or not', () => {
		expect(startHistory(rows, { wins: 3, losses: 0, ties: 0 })).toMatchObject({
			firstSeason: 2005
		});
	});

	it('reports nothing for no rows', () => {
		expect(startHistory([], { wins: 1, losses: 0, ties: 0 })).toEqual({
			count: 0,
			madePlayoffs: 0,
			firstSeason: null
		});
	});
});

describe('countWord', () => {
	it('writes one to nine as words and 10 up as numerals', () => {
		expect([1, 4, 9, 10, 32].map(countWord)).toEqual(['one', 'four', 'nine', '10', '32']);
	});
});
