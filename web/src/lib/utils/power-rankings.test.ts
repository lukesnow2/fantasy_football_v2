import { describe, expect, it } from 'vitest';
import {
	formatLuck,
	luckTone,
	powerBarPct,
	RANK_LINE_COLORS,
	rankChartLabel,
	rankingCallouts,
	seasonCount,
	spellNumber,
	type CalloutRow
} from './power-rankings';

type Row = CalloutRow & { name: string };

function row(
	name: string,
	powerRank: number,
	rankChange: number | null,
	luckFactor: number | null
): Row {
	return { name, powerRank, rankChange, luckFactor };
}

describe('rankingCallouts', () => {
	it('picks the biggest riser, faller and luck in a normal week', () => {
		const c = rankingCallouts([
			row('Israel', 1, 0, 1.01),
			row('Craig', 2, 0, -0.14),
			row('Gabe Flores', 3, 4, 0.21),
			row('Nick', 5, -2, -0.69),
			row('Gabe the Younger', 10, -5, -0.44)
		]);
		expect(c.riser?.name).toBe('Gabe Flores');
		expect(c.faller?.name).toBe('Gabe the Younger');
		expect(c.luckiest?.name).toBe('Israel');
	});

	it("breaks ties by this week's rank, whatever order the rows arrive in", () => {
		const c = rankingCallouts([
			row('Trevor', 4, 4, 0.5),
			row('Omar', 6, -2, 0.1),
			row('Gabe Flores', 3, 4, 0.5),
			row('Nick', 5, -2, 0.1)
		]);
		expect(c.riser?.name).toBe('Gabe Flores');
		expect(c.faller?.name).toBe('Nick');
		expect(c.luckiest?.name).toBe('Gabe Flores');
	});

	it('leaves the riser and faller empty in week 1, where every change is null', () => {
		const c = rankingCallouts([row('Israel', 1, null, 0.4), row('Craig', 2, null, -0.4)]);
		expect(c.riser).toBeNull();
		expect(c.faller).toBeNull();
		expect(c.luckiest?.name).toBe('Israel');
	});

	it('leaves the riser and faller empty in a week with no movement', () => {
		const c = rankingCallouts([row('Israel', 1, 0, 0), row('Craig', 2, 0, 0)]);
		expect(c.riser).toBeNull();
		expect(c.faller).toBeNull();
	});

	it('skips null luck, and has no luckiest when every luck is null', () => {
		expect(rankingCallouts([row('A', 1, 0, null), row('B', 2, 0, -0.3)]).luckiest?.name).toBe('B');
		expect(rankingCallouts([row('A', 1, 0, null)]).luckiest).toBeNull();
	});

	it('handles an empty week', () => {
		expect(rankingCallouts([])).toEqual({ riser: null, faller: null, luckiest: null });
	});
});

describe('powerBarPct', () => {
	it('rounds to a whole percent', () => {
		expect(powerBarPct(0.946)).toBe(95);
		expect(powerBarPct(0.544)).toBe(54);
		expect(powerBarPct(0.545)).toBe(55);
	});

	it('clamps below 0 and above 1', () => {
		expect(powerBarPct(-0.2)).toBe(0);
		expect(powerBarPct(1.3)).toBe(100);
		expect(powerBarPct(Number.NaN)).toBe(0);
	});
});

describe('formatLuck', () => {
	it('signs positive and negative values', () => {
		expect(formatLuck(0.39)).toBe('+0.39');
		expect(formatLuck(-0.14)).toBe('-0.14');
		expect(formatLuck(1.006)).toBe('+1.01');
	});

	it('prints zero, and anything that rounds to it, without a sign', () => {
		expect(formatLuck(0)).toBe('0.00');
		expect(formatLuck(-0.001)).toBe('0.00');
		expect(formatLuck(0.04, 1)).toBe('0.0');
	});

	it('returns null for no number, never a dash', () => {
		expect(formatLuck(null)).toBeNull();
		expect(formatLuck(undefined)).toBeNull();
		expect(formatLuck(Number.NaN)).toBeNull();
	});

	it('takes one or two places', () => {
		expect(formatLuck(2.04, 1)).toBe('+2.0');
		expect(formatLuck(-0.44, 1)).toBe('-0.4');
		expect(formatLuck(-2.5, 1)).toBe('-2.5');
		expect(formatLuck(0.6, 2)).toBe('+0.60');
	});
});

describe('luckTone', () => {
	it('follows the sign', () => {
		expect(luckTone(0.39)).toBe('up');
		expect(luckTone(-0.14)).toBe('down');
		expect(luckTone(0)).toBe('flat');
	});

	it('is flat for null and for values that print as zero', () => {
		expect(luckTone(null)).toBe('flat');
		expect(luckTone(0.004)).toBe('flat');
		expect(luckTone(0.04, 1)).toBe('flat');
		expect(luckTone(0.04, 2)).toBe('up');
	});
});

describe('rankChartLabel', () => {
	const weeks = [1, 2, 3];
	const s = (managerName: string, ranks: number[]) => ({
		managerName,
		points: ranks.map((rank, i) => ({ week: i + 1, rank }))
	});

	it('names one riser', () => {
		expect(rankChartLabel([s('Israel', [2, 1, 1]), s('Gabe Flores', [5, 7, 3])], weeks)).toBe(
			'Power rank by week for weeks 1 to 3. Gabe Flores rose four spots in week 3.'
		);
	});

	it('names two tied risers, joined with "and", as the board does', () => {
		expect(
			rankChartLabel(
				[
					s('Israel', [2, 1, 1]),
					s('Gabe Flores', [5, 7, 3]),
					s('Trevor', [8, 8, 4]),
					s('Luke S', [10, 9, 8])
				],
				weeks
			)
		).toBe(
			'Power rank by week for weeks 1 to 3. Gabe Flores and Trevor rose four spots in week 3.'
		);
	});

	it('says only the weeks when nobody rose', () => {
		expect(rankChartLabel([s('Israel', [1, 1, 1]), s('Craig', [2, 2, 2])], weeks)).toBe(
			'Power rank by week for weeks 1 to 3.'
		);
	});

	it('spells out one to nine and keeps 10 and up as digits', () => {
		expect(rankChartLabel([s('Nick', [3, 2, 1])], weeks)).toBe(
			'Power rank by week for weeks 1 to 3. Nick rose one spot in week 3.'
		);
		const long = Array.from({ length: 12 }, (_, i) => i + 1);
		const climber = {
			managerName: 'Omar',
			points: [
				{ week: 11, rank: 12 },
				{ week: 12, rank: 1 }
			]
		};
		expect(rankChartLabel([climber], long)).toBe(
			'Power rank by week for weeks 1 to 12. Omar rose 11 spots in week 12.'
		);
		expect(spellNumber(9)).toBe('nine');
		expect(spellNumber(10)).toBe('10');
	});
});

describe('RANK_LINE_COLORS', () => {
	it('has ten token colors in the board order', () => {
		expect(RANK_LINE_COLORS).toHaveLength(10);
		expect(RANK_LINE_COLORS[0]).toBe('var(--color-field)');
		expect(RANK_LINE_COLORS[9]).toBe('var(--color-chart-rose)');
		for (const c of RANK_LINE_COLORS) expect(c).toMatch(/^var\(--color-[a-z-]+\)$/);
	});
});

describe('seasonCount', () => {
	it('reads one season in the singular', () => {
		expect(seasonCount(1)).toBe('1 season');
		expect(seasonCount(21)).toBe('21 seasons');
	});
});
