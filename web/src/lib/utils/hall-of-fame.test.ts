import { describe, expect, it } from 'vitest';
import {
	fixed,
	groupByTier,
	hofTier,
	indexBarPct,
	managerCount,
	numberWord,
	podiumStyle,
	tierKey,
	toNumber
} from './hall-of-fame';

describe('hofTier', () => {
	it('puts each boundary rank on the right side of its line', () => {
		const at = (rank: number) => hofTier(rank);
		expect(at(1)).toEqual({ name: 'Hall of Fame Elite', color: 'pennant-dark' });
		expect(at(3)).toEqual({ name: 'Hall of Fame Elite', color: 'pennant-dark' });
		expect(at(4)).toEqual({ name: 'Hall of Fame', color: 'field' });
		expect(at(6)).toEqual({ name: 'Hall of Fame', color: 'field' });
		expect(at(7)).toEqual({ name: 'Hall of Very Good', color: 'ink-2' });
		expect(at(10)).toEqual({ name: 'Hall of Very Good', color: 'ink-2' });
		expect(at(11)).toEqual({ name: 'Solid Contributor', color: 'muted' });
		expect(at(15)).toEqual({ name: 'Solid Contributor', color: 'muted' });
		expect(at(16)).toEqual({ name: 'Developing Legacy', color: 'muted' });
		expect(at(40)).toEqual({ name: 'Developing Legacy', color: 'muted' });
	});
});

describe('tierKey', () => {
	it('lists all five tiers in order with the board copy', () => {
		expect(tierKey()).toEqual([
			{ name: 'Hall of Fame Elite', ranks: 'Top 3', color: 'pennant-dark' },
			{ name: 'Hall of Fame', ranks: 'Ranks 4 to 6', color: 'field' },
			{ name: 'Hall of Very Good', ranks: 'Ranks 7 to 10', color: 'ink-2' },
			{ name: 'Solid Contributor', ranks: 'Ranks 11 to 15', color: 'muted' },
			{ name: 'Developing Legacy', ranks: '16 and below', color: 'muted' }
		]);
	});
});

describe('indexBarPct', () => {
	it('draws the top manager full', () => {
		expect(indexBarPct(0.825, 0.825)).toBe(100);
	});

	it('rounds to a whole percent, as the board does', () => {
		expect(indexBarPct(0.508, 0.825)).toBe(62);
		expect(indexBarPct(0.172, 0.825)).toBe(21);
		expect(indexBarPct(0.344, 0.825)).toBe(42);
	});

	it('draws nothing when the top index is zero', () => {
		expect(indexBarPct(0, 0)).toBe(0);
		expect(indexBarPct(0.3, 0)).toBe(0);
	});
});

describe('numberWord', () => {
	it('spells one to twenty with a capital and uses digits above', () => {
		expect(numberWord(1)).toBe('One');
		expect(numberWord(12)).toBe('Twelve');
		expect(numberWord(20)).toBe('Twenty');
		expect(numberWord(21)).toBe('21');
	});
});

describe('podiumStyle', () => {
	it('draws first as the clubhouse card', () => {
		expect(podiumStyle(1)).toEqual({
			bg: 'clubhouse',
			fg: 'white',
			border: 'field-2',
			accent: 'pennant-light',
			ring: 'pennant-light',
			muted: 'on-clubhouse-2',
			pole: 'white',
			flag: 'pennant-light',
			padY: 28,
			rankSize: 72,
			avatar: 116,
			nameSize: 44,
			statSize: 34
		});
	});

	it('draws second and third white, each a size down', () => {
		const white = {
			bg: 'card',
			fg: 'ink',
			border: 'line',
			accent: 'field',
			ring: 'field-tint',
			muted: 'muted',
			pole: 'ink',
			flag: 'pennant'
		};
		expect(podiumStyle(2)).toEqual({
			...white,
			padY: 20,
			rankSize: 52,
			avatar: 80,
			nameSize: 32,
			statSize: 26
		});
		expect(podiumStyle(3)).toEqual({
			...white,
			padY: 16,
			rankSize: 44,
			avatar: 64,
			nameSize: 28,
			statSize: 24
		});
	});

	it('takes the phone board sizes and keeps the colors', () => {
		expect(podiumStyle(1, true)).toMatchObject({
			bg: 'clubhouse',
			padY: 20,
			rankSize: 48,
			avatar: 76,
			nameSize: 30,
			statSize: 24
		});
		expect(podiumStyle(2, true)).toMatchObject({
			bg: 'card',
			padY: 16,
			rankSize: 36,
			avatar: 56,
			nameSize: 24,
			statSize: 20
		});
		expect(podiumStyle(3, true)).toMatchObject({
			bg: 'card',
			padY: 14,
			rankSize: 32,
			avatar: 48,
			nameSize: 22,
			statSize: 18
		});
	});
});

describe('groupByTier', () => {
	const row = (name: string, hallOfFameRank: number) => ({ name, hallOfFameRank });

	it('groups rows by tier, best first, and skips empty tiers', () => {
		const groups = groupByTier([row('A', 1), row('B', 2), row('C', 3), row('D', 4), row('E', 11)]);
		expect(groups.map((g) => [g.name, g.color, g.rows.map((r) => r.name)])).toEqual([
			['Hall of Fame Elite', 'pennant-dark', ['A', 'B', 'C']],
			['Hall of Fame', 'field', ['D']],
			['Solid Contributor', 'muted', ['E']]
		]);
	});

	it('keeps a rank tie in one tier', () => {
		const groups = groupByTier([row('A', 1), row('B', 2), row('C', 3), row('D', 3), row('E', 5)]);
		expect(groups[0].rows.map((r) => r.name)).toEqual(['A', 'B', 'C', 'D']);
		expect(groups[1].rows.map((r) => r.name)).toEqual(['E']);
	});

	it('returns nothing for no rows', () => {
		expect(groupByTier([])).toEqual([]);
	});
});

describe('formatters', () => {
	it('fixes places', () => {
		expect(fixed('0.35225', 3)).toBe('0.352');
		expect(fixed(1.75, 1)).toBe('1.8');
		expect(fixed('', 1)).toBeNull();
	});

	it('reads numbers and treats missing ones as null', () => {
		expect(toNumber('0.429')).toBe(0.429);
		expect(toNumber(0)).toBe(0);
		expect(toNumber(null)).toBeNull();
		expect(toNumber('')).toBeNull();
		expect(toNumber('x')).toBeNull();
	});

	it('counts managers', () => {
		expect(managerCount(1)).toBe('1 manager');
		expect(managerCount(3)).toBe('3 managers');
	});
});
