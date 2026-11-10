import { describe, expect, it } from 'vitest';
import { buildWeekNotes, TEMPLATES, type NoteManager, type WeekNotesInput } from './week-notes';
import type { StartHistoryRow } from './start-history';

/** The This Season board's week 3: name, W, L, PF, PA, odds, last week's odds, power rank change. */
const BOARD: [string, number, number, number, number, number, number, number][] = [
	['Israel', 3, 0, 424.1, 380.5, 85, 79, 0],
	['Craig', 2, 1, 465.5, 402.2, 86, 89, 0],
	['Gabe Flores', 2, 1, 368.35, 346.3, 63, 46, 4],
	['Trevor', 2, 1, 368.2, 359.95, 61, 55, 4],
	['Nick', 1, 2, 450.8, 432.65, 75, 83, -2],
	['Omar', 1, 2, 372.45, 377.9, 51, 58, -2],
	['Erik Snow', 1, 2, 363.15, 357.6, 48, 59, -1],
	['Gabe the Younger', 1, 2, 357.4, 361.8, 45, 54, -5],
	['Luke S', 1, 2, 353.25, 381.35, 45, 40, 1],
	['Troy Colvin', 1, 2, 337.05, 460.0, 40, 39, 1]
];

/** The board's power order, for each manager's current rank. */
const POWER = ['Israel', 'Craig', 'Gabe Flores', 'Trevor', 'Nick', 'Omar', 'Erik Snow', 'Luke S', 'Troy Colvin', 'Gabe the Younger'];

const boardManagers: NoteManager[] = BOARD.map(([name, wins, losses, pf, pa, now, prev, change]) => ({
	name,
	wins,
	losses,
	ties: 0,
	pointsFor: pf,
	pointsAgainst: pa,
	playoffOdds: now / 100,
	previousOdds: prev / 100,
	powerRank: POWER.indexOf(name) + 1,
	rankChange: change
}));

const board: WeekNotesInput = { week: 3, managers: boardManagers, history: null };

function m(name: string, over: Partial<NoteManager> = {}): NoteManager {
	return {
		name,
		wins: 1,
		losses: 1,
		ties: 0,
		pointsFor: 200,
		pointsAgainst: 200,
		playoffOdds: 0.5,
		previousOdds: 0.5,
		powerRank: null,
		rankChange: 0,
		...over
	};
}

/** A plain league of six where nothing stands out. */
function league(over: Record<string, Partial<NoteManager>> = {}): NoteManager[] {
	return ['A', 'B', 'C', 'D', 'E', 'F'].map((name, i) =>
		m(name, { pointsFor: 200 + i, pointsAgainst: 200 + i, powerRank: i + 1, ...over[name] })
	);
}

function only(id: string, input: WeekNotesInput) {
	const template = TEMPLATES.find((t) => t.id === id)!;
	return template.build(input);
}

describe('buildWeekNotes on the board', () => {
	it('gives the three notes the board draws, in its order', () => {
		const notes = buildWeekNotes(board);
		expect(notes.map((n) => n.text)).toEqual([
			'Nick is 1-2 with the second-most points in the league. The schedule owes them one.',
			'Troy Colvin has given up 460.0 points in three weeks, the most by 27.',
			'Gabe Flores and Trevor both climbed four spots in the power rankings.'
		]);
		expect(notes.map((n) => Math.round(n.score * 10) / 10)).toEqual([75, 72.4, 68]);
	});

	it('keeps one note per subject and at most three', () => {
		const all = TEMPLATES.flatMap((t) => (t.build(board) ? [t.id] : []));
		// Gabe Flores is the subject of both the climb and the odds swing.
		expect(all).toContain('odds-swing');
		const notes = buildWeekNotes(board);
		expect(new Set(notes.map((n) => n.subject)).size).toBe(notes.length);
		expect(notes).toHaveLength(3);
	});

	it('never uses he or she for a manager', () => {
		const words = buildWeekNotes(board)
			.concat(TEMPLATES.flatMap((t) => (t.build(board) ? [{ text: t.build(board)!.text }] : [])) as never)
			.map((n) => n.text)
			.join(' ');
		expect(words).not.toMatch(/\b(him|her|his|hers)\b/i);
	});

	it('gives nothing when nothing applies', () => {
		expect(buildWeekNotes({ week: 2, managers: league(), history: null })).toEqual([]);
	});

	it('shows one or two notes when only that many apply', () => {
		const one = league({ F: { pointsAgainst: 260 } });
		expect(buildWeekNotes({ week: 2, managers: one, history: null })).toHaveLength(1);
	});
});

describe('losing-but-scoring', () => {
	it('applies to a losing team with a top-three points rank', () => {
		const managers = league({ F: { wins: 0, losses: 2 } });
		expect(only('losing-but-scoring', { week: 2, managers, history: null })).toEqual({
			text: 'F is 0-2 with the most points in the league. The schedule owes them one.',
			score: 80 + 10,
			subject: 'F'
		});
	});
	it("doesn't apply to a losing team lower in points", () => {
		const managers = league({ A: { wins: 0, losses: 2 } });
		expect(only('losing-but-scoring', { week: 2, managers, history: null })).toBeNull();
	});
});

describe('points-against-leader', () => {
	it('applies 15 points clear, and says one week in week 1', () => {
		const managers = league({ A: { wins: 0, losses: 1, ties: 0, pointsAgainst: 230 } });
		expect(only('points-against-leader', { week: 1, managers, history: null })).toEqual({
			text: 'A has given up 230.0 points in one week, the most by 25.',
			score: 70,
			subject: 'A'
		});
	});
	it("doesn't apply under 15 points clear", () => {
		expect(only('points-against-leader', { week: 2, managers: league(), history: null })).toBeNull();
	});
});

describe('power-climb', () => {
	it('names one climber', () => {
		const managers = league({ C: { rankChange: 3 } });
		expect(only('power-climb', { week: 2, managers, history: null })?.text).toBe(
			'C climbed three spots in the power rankings.'
		);
	});
	it('names three climbers in power-rank order', () => {
		const managers = league({ E: { rankChange: 5 }, B: { rankChange: 5 }, C: { rankChange: 5 } });
		expect(only('power-climb', { week: 2, managers, history: null })).toEqual({
			text: 'B, C and E all climbed five spots in the power rankings.',
			score: 76,
			subject: 'B'
		});
	});
	it("doesn't apply under three spots or with four sharing it", () => {
		expect(only('power-climb', { week: 2, managers: league({ C: { rankChange: 2 } }), history: null })).toBeNull();
		const four = league({ A: { rankChange: 3 }, B: { rankChange: 3 }, C: { rankChange: 3 }, D: { rankChange: 3 } });
		expect(only('power-climb', { week: 2, managers: four, history: null })).toBeNull();
	});
});

describe('power-fall', () => {
	it('names one or two fallers', () => {
		const one = league({ D: { rankChange: -4 } });
		expect(only('power-fall', { week: 2, managers: one, history: null })).toEqual({
			text: 'D fell four spots in the power rankings.',
			score: 54,
			subject: 'D'
		});
		const two = league({ D: { rankChange: -3 }, A: { rankChange: -3 } });
		expect(only('power-fall', { week: 2, managers: two, history: null })?.text).toBe(
			'A and D both fell three spots in the power rankings.'
		);
	});
	it("doesn't apply with three sharing the drop", () => {
		const three = league({ A: { rankChange: -3 }, B: { rankChange: -3 }, C: { rankChange: -3 } });
		expect(only('power-fall', { week: 2, managers: three, history: null })).toBeNull();
	});
});

describe('winning-but-not-scoring', () => {
	it('applies to a winning team with a bottom-three points rank', () => {
		const managers = league({ B: { wins: 2, losses: 0 } });
		expect(only('winning-but-not-scoring', { week: 2, managers, history: null })).toEqual({
			text: 'B is 2-0 with the second-fewest points in the league.',
			score: 70 + 10 - 5,
			subject: 'B'
		});
	});
	it("doesn't apply to a winning team higher in points", () => {
		const managers = league({ F: { wins: 2, losses: 0 } });
		expect(only('winning-but-not-scoring', { week: 2, managers, history: null })).toBeNull();
	});
});

describe('odds-swing', () => {
	it('applies to a jump of 15 points or more', () => {
		const managers = league({ C: { previousOdds: 0.3, playoffOdds: 0.52 } });
		expect(only('odds-swing', { week: 2, managers, history: null })).toEqual({
			text: 'Playoff odds for C went from 30% to 52%, the biggest jump in the league.',
			score: 62,
			subject: 'C'
		});
	});
	it('words a drop', () => {
		const managers = league({ C: { previousOdds: 0.6, playoffOdds: 0.4 } });
		expect(only('odds-swing', { week: 2, managers, history: null })?.text).toBe(
			'Playoff odds for C went from 60% to 40%, the biggest drop in the league.'
		);
	});
	it("doesn't apply under 15 points or in week 1", () => {
		expect(only('odds-swing', { week: 2, managers: league({ C: { previousOdds: 0.4 } }), history: null })).toBeNull();
		const week1 = league().map((x) => ({ ...x, previousOdds: null }));
		expect(only('odds-swing', { week: 1, managers: week1, history: null })).toBeNull();
	});
});

describe('rough-start-history', () => {
	const history = (made: number, count: number): StartHistoryRow[] =>
		Array.from({ length: count }, (_, i) => ({
			season: 2005 + i,
			managerName: 'Someone',
			wins: 0,
			losses: 2,
			ties: 0,
			madePlayoffs: i < made,
			wonTitle: false
		}));
	const managers = league({ F: { wins: 0, losses: 2 }, A: { wins: 2, losses: 0 } });

	it('applies with five or more past teams', () => {
		expect(only('rough-start-history', { week: 2, managers, history: history(3, 12) })).toEqual({
			text: 'F is 0-2. Since 2005, three of 12 teams that started 0-2 made the playoffs.',
			score: 50 + 35 * (1 - 3 / 12),
			subject: 'F'
		});
	});
	it('words none and two managers', () => {
		const two = league({ F: { wins: 0, losses: 2 }, E: { wins: 0, losses: 2 }, A: { wins: 2, losses: 0 } });
		expect(only('rough-start-history', { week: 2, managers: two, history: history(0, 6) })?.text).toBe(
			'E and F are 0-2. Since 2005, none of the six teams that started 0-2 made the playoffs.'
		);
	});
	it("doesn't apply under five past teams, with three sharing, or with no history", () => {
		expect(only('rough-start-history', { week: 2, managers, history: history(1, 4) })).toBeNull();
		const three = league({
			F: { wins: 0, losses: 2 },
			E: { wins: 0, losses: 2 },
			D: { wins: 0, losses: 2 },
			A: { wins: 2, losses: 0 }
		});
		expect(only('rough-start-history', { week: 2, managers: three, history: history(1, 9) })).toBeNull();
		expect(only('rough-start-history', { week: 2, managers, history: null })).toBeNull();
	});
	it("doesn't apply when the worst record is the leader's", () => {
		expect(only('rough-start-history', { week: 2, managers: league(), history: history(1, 9) })).toBeNull();
	});
});
