import { describe, expect, it } from 'vitest';
import {
	chartWeeks,
	gradeLines,
	ordinal,
	rebuiltNote,
	seasonGrade,
	skillLine,
	type OddsHistory,
	type OddsHistoryPoint
} from './odds-history';

type Available = Extract<OddsHistory, { available: true }>;

/**
 * Four teams, two make it, four regular weeks and two playoff weeks. A and B
 * make the field; C leads at halfway and misses; B wins the title.
 */
function history(over: Partial<Available> = {}): Available {
	const odds: Record<string, [number, number, number][]> = {
		// [playoff, bye, title] for weeks 1-4, then title after weeks 5 and 6.
		A: [
			[0.6, 0.3, 0.3],
			[0.7, 0.4, 0.35],
			[0.9, 0.6, 0.4],
			[1, 1, 0.45]
		],
		B: [
			[0.5, 0.2, 0.2],
			[0.4, 0.2, 0.2],
			[0.8, 0.3, 0.3],
			[1, 0, 0.35]
		],
		C: [
			[0.5, 0.2, 0.25],
			[0.8, 0.3, 0.3],
			[0.2, 0.1, 0.2],
			[0, 0, 0.1]
		],
		D: [
			[0.4, 0.1, 0.25],
			[0.1, 0.1, 0.15],
			[0.1, 0, 0.1],
			[0, 0, 0.1]
		]
	};
	const after = { A: [0.5, 0], B: [0.5, 1], C: [0, 0], D: [0, 0] } as Record<string, number[]>;
	const teams = Object.entries(odds).map(([name, weeks], i) => {
		const points: OddsHistoryPoint[] = weeks.map(([playoffOdds, byeOdds, titleOdds], w) => ({
			week: w + 1,
			playoffOdds,
			byeOdds,
			titleOdds
		}));
		after[name].forEach((titleOdds, j) =>
			points.push({ week: 5 + j, playoffOdds: 0, byeOdds: 0, titleOdds })
		);
		return { teamKey: i + 1, teamName: `Team ${name}`, managerName: name, points };
	});
	return {
		season: 2025,
		available: true,
		regularSeasonWeeks: 4,
		playoffTeams: 2,
		byes: 1,
		weeks: [1, 2, 3, 4, 5, 6],
		playoffWeeks: [5, 6],
		rebuiltWeeks: [],
		teams,
		result: { field: [1, 2], byes: [1], champion: 2 },
		...over
	};
}

describe('chartWeeks', () => {
	it('keeps the playoff weeks for the title only', () => {
		expect(chartWeeks(history(), 'titleOdds')).toEqual([1, 2, 3, 4, 5, 6]);
		expect(chartWeeks(history(), 'playoffOdds')).toEqual([1, 2, 3, 4]);
	});
});

describe('seasonGrade', () => {
	it('is null until the field is known', () => {
		expect(
			seasonGrade(history({ result: { field: null, byes: null, champion: null } }))
		).toBeNull();
	});

	it('finds the week the odds split the field from the rest for good', () => {
		// Week 1 has B level with C, week 2 has C ahead of B, weeks 3 and 4 split.
		expect(seasonGrade(history())!.fieldFrom).toBe(3);
	});

	it('scores halfway against the field and names the biggest miss', () => {
		const g = seasonGrade(history())!;
		expect(g.halfway).toEqual({ week: 2, hits: 1, of: 2 });
		// C at 80% missed (0.8 off); B at 40% made it (0.6 off).
		expect(g.surprise).toEqual({ name: 'C', week: 2, odds: 0.8, madeIt: false });
	});

	it("rates the champion's odds as the bracket was set", () => {
		const g = seasonGrade(history())!;
		expect(g.champion).toEqual({ name: 'B', odds: 0.35, rank: 2 });
		expect(g.favorite).toEqual({ name: 'A', odds: 0.45 });
	});

	it('compares the regular season with an even guess', () => {
		const g = seasonGrade(history())!;
		expect(g.evenGuess).toBe(0.5);
		// Weeks 1-3 only: week 4's standings are final.
		const sq = (x: number) => x * x;
		const brier =
			[0.6, 0.7, 0.9, 0.5, 0.4, 0.8].reduce((s, p) => s + sq(1 - p), 0) +
			[0.5, 0.8, 0.2, 0.4, 0.1, 0.1].reduce((s, p) => s + sq(p), 0);
		expect(g.skill).toBeCloseTo(1 - brier / (12 * 0.25), 10);
	});
});

describe('gradeLines', () => {
	it('words the grade', () => {
		const lines = gradeLines(seasonGrade(history())!, 4);
		expect(lines).toEqual([
			{
				id: 'champion',
				value: '35% for the champ',
				note: 'B had the 2nd-best title odds when the bracket was set. A was the favorite at 45%.'
			},
			{
				id: 'field',
				value: 'Field called by wk 3',
				note: 'From week 3 on, every playoff team had better odds than every team that missed.'
			},
			{
				id: 'halfway',
				value: '1 of 2 at halfway',
				note: 'After week 2, one of the two teams with the best odds made the playoffs.'
			},
			{
				id: 'surprise',
				value: 'Biggest miss: C',
				note: 'Had 80% playoff odds after week 2 and missed.'
			}
		]);
	});

	it('says when the field only settled in the last week', () => {
		const g = { ...seasonGrade(history())!, fieldFrom: 4 };
		expect(gradeLines(g, 4).find((l) => l.id === 'field')?.value).toBe('Field settled in wk 4');
	});

	it('praises a favorite that won', () => {
		const g = { ...seasonGrade(history())!, champion: { name: 'A', odds: 0.45, rank: 1 } };
		expect(gradeLines(g, 4)[0].note).toBe('A had the best title odds when the bracket was set.');
	});
});

describe('skillLine', () => {
	it('prints the skill against the even guess', () => {
		const g = seasonGrade(history())!;
		expect(skillLine({ ...g, skill: 0.384 })).toBe(
			'Over the regular season the odds had 38% less error than giving every team the same 50% chance (Brier score).'
		);
		expect(skillLine({ ...g, skill: -0.1 })).toBe(
			'Over the regular season the odds did no better than giving every team the same 50% chance.'
		);
	});
});

describe('ordinal', () => {
	it.each([
		[1, '1st'],
		[2, '2nd'],
		[3, '3rd'],
		[4, '4th'],
		[11, '11th'],
		[22, '22nd']
	])('%i is %s', (n, s) => expect(ordinal(n)).toBe(s));
});

describe('rebuiltNote', () => {
	it('says nothing when every week was saved', () => {
		expect(rebuiltNote(history())).toBeNull();
	});

	it('names one or a few rebuilt weeks', () => {
		expect(rebuiltNote(history({ rebuiltWeeks: [2] }))).toBe(
			"Week 2 wasn't saved at the time, so it's rebuilt from that week's games."
		);
		expect(rebuiltNote(history({ rebuiltWeeks: [1, 2, 3] }))).toBe(
			"Weeks 1, 2 and 3 weren't saved at the time, so they're rebuilt from those weeks' games."
		);
	});

	it('says so plainly when the whole season is rebuilt', () => {
		expect(rebuiltNote(history({ rebuiltWeeks: [1, 2, 3, 4] }))).toBe(
			"These odds weren't saved at the time, so every week is rebuilt from that week's games with today's model."
		);
	});
});
