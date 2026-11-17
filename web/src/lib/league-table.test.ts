import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import {
	COLUMNS,
	DEFAULT_STATE,
	GROUPS,
	buildRows,
	caption,
	clickSort,
	filterRows,
	firstDirection,
	formatLuck,
	formatRecord,
	groupById,
	leaders,
	parseState,
	phoneColumns,
	rankRows,
	recordPct,
	seasonSpan,
	signTone,
	signed,
	sortLine,
	sortRows,
	stateQuery,
	switchGroup,
	tableMinWidth,
	tierLabel,
	type LeagueRow
} from './league-table';

const f = JSON.parse(
	readFileSync(new URL('../../e2e/fixtures/league-table.json', import.meta.url), 'utf8')
);
const grades = f.draft.data.draftGrades;
const rows = buildRows(f.all.data.performance, grades, f.hallOfFame.hallOfFame);
const byName = (name: string) => rows.find((r) => r.name === name) as LeagueRow;

/** A row with every field blank but the ones given. */
function row(over: Partial<LeagueRow>): LeagueRow {
	return {
		name: 'X',
		firstSeason: 2005,
		lastSeason: 2026,
		alum: false,
		seasons: null,
		wins: 0,
		losses: 0,
		ties: 0,
		winPct: null,
		titles: null,
		titleGameWins: null,
		titleGameLosses: null,
		trips: null,
		playoffWinPct: null,
		ppg: null,
		ppSeason: null,
		points: null,
		pointsAgainst: null,
		pointDiff: null,
		regWins: null,
		expectedWins: null,
		luck: null,
		faab: null,
		moves: null,
		movesPerSeason: null,
		draftGrade: null,
		draftValue: null,
		bestDraft: null,
		worstDraft: null,
		hofIndex: null,
		hofRank: null,
		hofKnown: true,
		swing: null,
		bestSeason: null,
		worstSeason: null,
		winStreak: null,
		lossStreak: null,
		...over
	};
}

const ranked = (key: string, dir = firstDirection(COLUMNS[key]), list = rows) =>
	rankRows(sortRows(list, COLUMNS[key], dir), COLUMNS[key]);

const rankText = (r: { rank: number | null; tied: boolean }) =>
	r.rank == null ? '' : `${r.tied ? 'T' : ''}${r.rank}`;

describe('buildRows', () => {
	it('joins the three APIs by name, with win % counting a tie as half (D13.1)', () => {
		const trevor = byName('Trevor');
		expect(trevor.winPct).toBeCloseTo((188 + 0.5) / 334, 4);
		expect(formatRecord(trevor)).toBe('188-145-1');
		expect(trevor.pointsAgainst).toBeCloseTo(41221.44, 1);
		expect(trevor.ppSeason).toBeCloseTo(2028.87, 1);
		expect(trevor.winStreak).toBe(10);
		expect(trevor.lossStreak).toBe(6);
		expect(trevor.pointDiff).toBeCloseTo(43118.79 - 41221.44, 1);
		expect(trevor.titleGameWins).toBe(7);
		expect(trevor.titleGameLosses).toBe(3);
		expect(trevor.hofRank).toBe(1);
		expect(trevor.draftGrade).toBe('C');
	});

	it('marks managers whose last season is before the latest as alumni', () => {
		expect(byName('Bobby').alum).toBe(true);
		expect(rows.filter((r) => r.alum).map((r) => r.name)).toEqual(['Bobby']);
		expect(filterRows(rows, 'active').some((r) => r.name === 'Bobby')).toBe(false);
		expect(filterRows(rows, 'all')).toHaveLength(rows.length);
	});

	it('takes best and worst draft from finished-season scores, as the Managers page does (D7)', () => {
		const bySeason = [
			{ managerName: 'A', seasonYear: 2024, score: 1, inProgress: false },
			{ managerName: 'A', seasonYear: 2025, score: -2, inProgress: false },
			{ managerName: 'A', seasonYear: 2026, score: 9, inProgress: true }
		];
		const [a] = buildRows([{ managerName: 'A' }], { byManager: [], bySeason }, []);
		expect(a.bestDraft).toBe(2024);
		expect(a.worstDraft).toBe(2025);
	});

	it('spans the seasons for the kicker', () => {
		expect(seasonSpan(rows)).toEqual({ first: 2005, latest: 2026 });
		expect(seasonSpan([])).toBeNull();
	});
});

describe('columns', () => {
	it('each column sorts in its better direction first', () => {
		const lows = Object.values(COLUMNS)
			.filter((c) => c.better === 'low')
			.map((c) => c.key);
		expect(lows.sort()).toEqual(['lossstreak', 'pa', 'swing']);
		expect(COLUMNS.luck.better).toBe('neutral');
		expect(firstDirection(COLUMNS.luck)).toBe('desc');
		expect(firstDirection(COLUMNS.pa)).toBe('asc');
		expect(firstDirection(COLUMNS.titles)).toBe('desc');
		expect(COLUMNS.bestdraft.better).toBeNull();
		expect(COLUMNS.worstdraft.better).toBeNull();
	});

	it('every group lists real columns and sorts by one of them by default', () => {
		expect(GROUPS.map((g) => g.label)).toEqual([
			'Overview',
			'Winning',
			'Playoffs',
			'Scoring',
			'Luck',
			'Waiver wire',
			'Draft',
			'Legacy'
		]);
		for (const g of GROUPS) {
			for (const key of g.columns) expect(COLUMNS[key], key).toBeDefined();
			expect(g.columns).toContain(g.defaultSort);
		}
	});

	it('prints the signed formats, with zero judged on the rounded value', () => {
		expect(formatLuck(-1.84)).toBe('-1.8 W');
		expect(formatLuck(1.91)).toBe('+1.9 W');
		expect(formatLuck(0.03)).toBe('0.0 W');
		expect(signTone({ text: formatLuck(0.03), tone: 'signed' })).toBe('zero');
		expect(signTone({ text: formatLuck(-0.06), tone: 'signed' })).toBe('down');
		expect(signed(1897.35, 0)).toBe('+1,897');
		expect(signed(-1149.1, 0)).toBe('-1,149');
		expect(signed(0.22, 2)).toBe('+0.22');
		expect(signed(-0.004, 2)).toBe('0.00');
		// A half rounds away from zero, as the rest of the site prints it.
		expect(signed(-639.5, 0)).toBe('-640');
		expect(signed(29.25, 1)).toBe('+29.3');
	});

	it('prints each kind of value as the board does', () => {
		const t = byName('Trevor');
		expect(COLUMNS.winpct.display(t).text).toBe('.564');
		expect(COLUMNS.ppct.display(t).text).toBe('66.7%');
		expect(COLUMNS.points.display(t).text).toBe('43,119');
		expect(COLUMNS.ppseason.display(t).text).toBe('2,028.9');
		expect(COLUMNS.faab.display(t).text).toBe('3.13 pts/$');
		expect(COLUMNS.swing.display(t).text).toBe('±.160');
		expect(COLUMNS.hof.display(t).text).toBe('0.825');
		expect(COLUMNS.titlegames.display(t).text).toBe('7-3');
		expect(formatRecord({ wins: 158, losses: 139, ties: 0 })).toBe('158-139');
	});

	it('blank values read as words, never a dash', () => {
		expect(COLUMNS.faab.display(byName('Bobby'))).toEqual({
			text: 'No bids',
			tone: 'plain',
			blank: true
		});
		const newcomer = row({ name: 'New' });
		expect(COLUMNS.hof.display(newcomer).text).toBe('Under 3 seasons');
		expect(COLUMNS.tier.display(newcomer).text).toBe('Under 3 seasons');
		expect(COLUMNS.ppg.display(newcomer).text).toBe('None');
		// The Hall of Fame didn't load: missing, not "under 3 seasons".
		const [noHof] = buildRows([{ managerName: 'Trevor', totalSeasons: 22 }], undefined, null);
		expect(noHof.hofKnown).toBe(false);
		expect(COLUMNS.hof.display(noHof).text).toBe('None');
		expect(COLUMNS.tier.display(noHof).text).toBe('None');
	});

	it('names Hall of Fame tiers short, in their tier colors', () => {
		expect(tierLabel(1)).toEqual({ text: 'HoF Elite', color: 'pennant-dark' });
		expect(tierLabel(5)).toEqual({ text: 'Hall of Fame', color: 'field' });
		expect(tierLabel(7)).toEqual({ text: 'Very Good', color: 'ink-2' });
		expect(tierLabel(12)).toEqual({ text: 'Solid', color: 'muted' });
	});

	it('reads a season record as a win rate', () => {
		expect(recordPct('13-2-0')).toBeCloseTo(13 / 15, 5);
		expect(recordPct('9-6-1')).toBeCloseTo(9.5 / 16, 5);
		expect(recordPct('')).toBeNull();
		expect(recordPct('bad')).toBeNull();
	});
});

describe('sorting and ranks', () => {
	it('ranks Titles with ties sharing a rank and the next rank skipping', () => {
		expect(ranked('titles').map(rankText)).toEqual([
			'1',
			'T2',
			'T2',
			'T2',
			'5',
			'T6',
			'T6',
			'T8',
			'T8',
			'T8',
			'T8'
		]);
	});

	it('orders a tie by win %, highest first', () => {
		const ties = ranked('titles').filter((r) => r.rank === 2);
		const pcts = ties.map((r) => r.row.winPct as number);
		expect(pcts).toEqual([...pcts].sort((a, b) => b - a));
		expect(ties.map((r) => r.row.name)).toEqual(['Gabe the Younger', 'Israel', 'Nick']);
	});

	it('puts a blank value last in both directions, with no rank', () => {
		for (const dir of ['desc', 'asc'] as const) {
			const list = ranked('faab', dir);
			expect(list.at(-1)?.row.name).toBe('Bobby');
			expect(list.at(-1)?.rank).toBeNull();
		}
	});

	it('sorts Record by win %', () => {
		expect(sortRows(rows, COLUMNS.record, 'desc').map((r) => r.name)).toEqual(
			sortRows(rows, COLUMNS.winpct, 'desc').map((r) => r.name)
		);
	});

	it('sorts Title games by wins, then fewer losses', () => {
		const list = [
			row({ name: 'A', titleGameWins: 3, titleGameLosses: 4 }),
			row({ name: 'B', titleGameWins: 3, titleGameLosses: 0 }),
			row({ name: 'C', titleGameWins: 7, titleGameLosses: 3 })
		];
		expect(sortRows(list, COLUMNS.titlegames, 'desc').map((r) => r.name)).toEqual(['C', 'B', 'A']);
		expect([...leaders(list, COLUMNS.titlegames)]).toEqual(['C']);
	});

	it('sorts the draft letter by the score behind it', () => {
		const list = sortRows(rows, COLUMNS.grade, 'desc');
		expect(list[0].name).toBe(sortRows(rows, COLUMNS.dval, 'desc')[0].name);
	});

	it('puts Points against lowest first on the first click', () => {
		const list = ranked('pa');
		const pa = list.map((r) => r.row.pointsAgainst as number);
		expect(pa).toEqual([...pa].sort((a, b) => a - b));
		expect(list[0].row.name).toBe('Bobby');
	});

	it('ties on the printed value, not the raw one', () => {
		const list = [row({ name: 'A', ppg: 124.94 }), row({ name: 'B', ppg: 124.86 })];
		expect(ranked('ppg', 'desc', list).map(rankText)).toEqual(['T1', 'T1']);
	});
});

describe('leaders', () => {
	it('chips every manager tied for the best value', () => {
		expect([...leaders(rows, COLUMNS.seasons)].sort()).toEqual(['Luke S', 'Trevor']);
		expect([...leaders(rows, COLUMNS.pa)]).toEqual(['Bobby']);
	});

	it('gives no leader to Luck, the draft years, the HoF tier or Record', () => {
		for (const key of ['luck', 'bestdraft', 'worstdraft', 'tier', 'record']) {
			expect(leaders(rows, COLUMNS[key]).size, key).toBe(0);
		}
	});
});

describe('state', () => {
	it('parses the URL, falling back on anything unknown', () => {
		expect(parseState(new URLSearchParams(''))).toEqual(DEFAULT_STATE);
		expect(parseState(new URLSearchParams('group=scoring&sort=pa&dir=asc&who=active'))).toEqual({
			group: 'scoring',
			sort: 'pa',
			dir: 'asc',
			who: 'active'
		});
		expect(parseState(new URLSearchParams('group=nope'))).toEqual(DEFAULT_STATE);
		// A sort the group doesn't have falls back to its default.
		expect(parseState(new URLSearchParams('group=luck&sort=pa'))).toMatchObject({
			sort: 'luck',
			dir: 'desc'
		});
		// Years aren't sortable.
		expect(parseState(new URLSearchParams('group=draft&sort=bestdraft')).sort).toBe('dval');
		// Avg draft grade was the average pick number, not a grade, so it's gone (Luke, 11-16).
		expect(parseState(new URLSearchParams('group=draft&sort=adg')).sort).toBe('dval');
		expect(parseState(new URLSearchParams('dir=sideways')).dir).toBe('desc');
		expect(parseState(new URLSearchParams('who=everyone')).who).toBe('all');
	});

	it('leaves defaults out of the URL and round-trips the rest', () => {
		expect(stateQuery(DEFAULT_STATE)).toBe('');
		const pa = { group: 'scoring', sort: 'pa', dir: 'asc', who: 'active' } as const;
		expect(stateQuery(pa)).toBe('?group=scoring&sort=pa&who=active');
		expect(parseState(new URLSearchParams(stateQuery(pa)))).toEqual(pa);
		const flipped = { ...DEFAULT_STATE, dir: 'asc' } as const;
		expect(stateQuery(flipped)).toBe('?sort=titles&dir=asc');
		expect(parseState(new URLSearchParams(stateQuery(flipped)))).toEqual(flipped);
	});

	it('a first click sorts the better way, a second flips it', () => {
		const first = clickSort(DEFAULT_STATE, 'luck');
		expect(first).toMatchObject({ sort: 'luck', dir: 'desc' });
		expect(clickSort(first, 'luck').dir).toBe('asc');
		expect(clickSort(DEFAULT_STATE, 'swing').dir).toBe('asc');
		expect(clickSort(DEFAULT_STATE, 'bestdraft')).toBe(DEFAULT_STATE);
	});

	it('switching groups keeps the sort when the new group has it', () => {
		const byTitlesAsc = { ...DEFAULT_STATE, dir: 'asc' as const };
		expect(switchGroup(byTitlesAsc, 'playoffs')).toMatchObject({ sort: 'titles', dir: 'asc' });
		expect(switchGroup(byTitlesAsc, 'scoring')).toMatchObject({ sort: 'ppg', dir: 'desc' });
		expect(switchGroup(DEFAULT_STATE, 'legacy')).toMatchObject({ group: 'legacy', sort: 'hof' });
	});
});

describe('copy and layout', () => {
	it('writes the sort line and caption', () => {
		expect(sortLine(11, COLUMNS.titles, 'desc')).toBe(
			'11 managers · sorted by titles, highest first'
		);
		expect(sortLine(10, COLUMNS.pa, 'asc')).toBe(
			'10 managers · sorted by points against, lowest first'
		);
		expect(sortLine(11, COLUMNS.luck, 'desc')).toBe('11 managers · sorted by luck, luckiest first');
		expect(sortLine(11, COLUMNS.luck, 'asc')).toBe(
			'11 managers · sorted by luck, unluckiest first'
		);
		expect(caption(groupById('overview'), COLUMNS.titles)).toBe(
			'League table, Overview, sorted by Titles'
		);
	});

	it('moves the sorted column next to Manager on phones', () => {
		expect(phoneColumns(groupById('overview'), 'titles').map((c) => c.key)).toEqual([
			'titles',
			'seasons',
			'trips',
			'winpct',
			'ppg',
			'luck',
			'hof'
		]);
	});

	it('sizes the table as the boards do', () => {
		const overview = phoneColumns(groupById('overview'), 'titles');
		expect(tableMinWidth(overview, false)).toBe(938);
		expect(tableMinWidth(overview, true)).toBe(804);
		const scoring = phoneColumns(groupById('scoring'), 'ppg');
		expect(tableMinWidth(scoring, false)).toBe(806);
		const draft = groupById('draft');
		expect(draft.columns).toEqual(['grade', 'dval', 'bestdraft', 'worstdraft']);
		expect(COLUMNS.adg).toBeUndefined();
	});
});
