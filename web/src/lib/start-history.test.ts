import { describe, expect, it } from 'vitest';
import { countWord, historyTile, startHistory, type StartHistoryRow } from './start-history';

function row(season: number, wins: number, losses: number, ties = 0, made = false, won = false): StartHistoryRow {
	return { season, managerName: 'Someone', wins, losses, ties, madePlayoffs: made, wonTitle: won };
}

const rows: StartHistoryRow[] = [
	row(2005, 3, 0, 0, true, true),
	row(2006, 3, 0, 0, true),
	row(2007, 3, 0, 0, false),
	row(2008, 3, 0, 1, true, true),
	row(2009, 2, 1),
	row(2024, 0, 3)
];

describe('startHistory', () => {
	it('counts an exact record match only: 3-0 is not 3-0-1', () => {
		expect(startHistory(rows, { wins: 3, losses: 0, ties: 0 })).toMatchObject({
			count: 3,
			madePlayoffs: 2,
			wonTitle: 1
		});
		expect(startHistory(rows, { wins: 3, losses: 0, ties: 1 }).count).toBe(1);
	});

	it('reports the first and last seasons the rows cover, matched or not', () => {
		expect(startHistory(rows, { wins: 3, losses: 0, ties: 0 })).toMatchObject({
			firstSeason: 2005,
			lastSeason: 2024
		});
	});

	it('reports nothing for no rows', () => {
		expect(startHistory([], { wins: 1, losses: 0, ties: 0 })).toEqual({
			count: 0,
			madePlayoffs: 0,
			wonTitle: 0,
			firstSeason: null,
			lastSeason: null
		});
	});
});

describe('historyTile', () => {
	const record = { wins: 3, losses: 0, ties: 0 };

	it("prints the board's tile for many matches", () => {
		const result = { count: 32, madePlayoffs: 30, wonTitle: 4, firstSeason: 2005, lastSeason: 2025 };
		expect(historyTile(result, record)).toEqual({
			label: '3-0 starts that made the playoffs',
			value: '30 of 32',
			sub: 'Since 2005. Four won the title.'
		});
	});

	it('words one title, no titles and ten or more', () => {
		const tile = (wonTitle: number) =>
			historyTile({ count: 40, madePlayoffs: 30, wonTitle, firstSeason: 2005, lastSeason: 2025 }, record)!.sub;
		expect(tile(1)).toBe('Since 2005. One won the title.');
		expect(tile(0)).toBe('Since 2005. None won the title.');
		expect(tile(12)).toBe('Since 2005. 12 won the title.');
	});

	it('says so when no team had the record', () => {
		const result = { count: 0, madePlayoffs: 0, wonTitle: 0, firstSeason: 2005, lastSeason: 2025 };
		expect(historyTile(result, { wins: 0, losses: 3, ties: 1 })).toEqual({
			label: '0-3-1 starts since 2005',
			value: 'None',
			sub: 'No team started 0-3-1 from 2005 to 2025.'
		});
	});

	it('draws nothing without finished seasons', () => {
		const result = { count: 0, madePlayoffs: 0, wonTitle: 0, firstSeason: null, lastSeason: null };
		expect(historyTile(result, record)).toBeNull();
	});
});

describe('countWord', () => {
	it('writes one to nine as words and 10 up as numerals', () => {
		expect([1, 4, 9, 10, 32].map(countWord)).toEqual(['one', 'four', 'nine', '10', '32']);
	});
});
