import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import {
	COLUMNS,
	DEFAULT_STATE,
	GROUPS,
	buildRangeRows,
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
	pickSeason,
	rangeLabel,
	rankRows,
	recordPct,
	seasonRange,
	seasonSpan,
	signTone,
	signed,
	sortLine,
	sortRows,
	stateQuery,
	switchGroup,
	tableMinWidth,
	tierLabel,
	type LeagueRow,
	type SeasonsPayload,
	type TeamSeasonRow
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
		playoffWins: null,
		playoffLosses: null,
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

	it('sorts Playoff record by wins, then fewer losses, and rates from the record', () => {
		const list = [
			row({ name: 'A', playoffWins: 9, playoffLosses: 13 }),
			row({ name: 'B', playoffWins: 9, playoffLosses: 10 }),
			row({ name: 'C', playoffWins: 30, playoffLosses: 15 })
		];
		expect(sortRows(list, COLUMNS.precord, 'desc').map((r) => r.name)).toEqual(['C', 'B', 'A']);
		expect([...leaders(list, COLUMNS.precord)]).toEqual(['C']);
		expect(COLUMNS.precord.display(list[2]).text).toBe('30-15');
		// 12-10 is 54.5%; the mart's 0.5455 would print 54.6%.
		const [g] = buildRows(
			[{ managerName: 'G', playoffWins: 12, playoffLosses: 10, playoffWinPercentage: 0.5455 }],
			undefined,
			[]
		);
		expect(COLUMNS.ppct.display(g).text).toBe('54.5%');
		// No record: the API's rate.
		const [h] = buildRows([{ managerName: 'H', playoffWinPercentage: 0.25 }], undefined, []);
		expect(COLUMNS.ppct.display(h).text).toBe('25.0%');
	});

	it('has no playoff win rate without bracket games, whatever the mart stored', () => {
		// 0-0 in the bracket: the mart's old fallback rate was 0, which printed 0.0%.
		const [none] = buildRows(
			[{ managerName: 'N', playoffWins: 0, playoffLosses: 0, playoffWinPercentage: 0 }],
			undefined,
			[]
		);
		expect(none.playoffWinPct).toBeNull();
		expect(COLUMNS.ppct.display(none)).toMatchObject({ text: 'None', blank: true });
		expect(COLUMNS.ppct.sortKey(none)).toBeNull();
		// A 0-3 record is a real 0.0%.
		const [swept] = buildRows(
			[{ managerName: 'S', playoffWins: 0, playoffLosses: 3, playoffWinPercentage: 0 }],
			undefined,
			[]
		);
		expect(COLUMNS.ppct.display(swept).text).toBe('0.0%');
	});

	// The mart stores no swing with one finished season (its own test is
	// tests/test_manager_performance_mart.py); the table prints that as None.
	it('prints a missing swing as None, not ±.000', () => {
		const [one] = buildRows([{ managerName: 'O', seasonConsistencyScore: null }], undefined, []);
		expect(COLUMNS.swing.display(one)).toMatchObject({ text: 'None', blank: true });
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
			who: 'active',
			from: null,
			to: null
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
		const pa = {
			group: 'scoring',
			sort: 'pa',
			dir: 'asc',
			who: 'active',
			from: null,
			to: null
		} as const;
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

describe('season range state', () => {
	const span = { first: 2005, latest: 2026 };

	it('reads from and to from the URL, swapping them if they are backwards', () => {
		expect(parseState(new URLSearchParams('from=2010&to=2020'))).toMatchObject({
			from: 2010,
			to: 2020
		});
		expect(parseState(new URLSearchParams('from=2020&to=2010'))).toMatchObject({
			from: 2010,
			to: 2020
		});
		expect(parseState(new URLSearchParams('from=soon&to=20201'))).toMatchObject({
			from: null,
			to: null
		});
	});

	it('writes only the ends that are set', () => {
		expect(stateQuery({ ...DEFAULT_STATE, from: 2010, to: 2020 })).toBe('?from=2010&to=2020');
		expect(stateQuery({ ...DEFAULT_STATE, from: 2020 })).toBe('?from=2020');
		const state = { ...DEFAULT_STATE, group: 'luck' as const, sort: 'luck', from: 2015 };
		expect(parseState(new URLSearchParams(stateQuery(state)))).toEqual(state);
	});

	it('fills an open end with the league span and pulls a year outside it in', () => {
		expect(seasonRange(DEFAULT_STATE, span)).toEqual({ from: 2005, to: 2026, all: true });
		expect(seasonRange({ from: 2010, to: null }, span)).toEqual({
			from: 2010,
			to: 2026,
			all: false
		});
		expect(seasonRange({ from: 1999, to: 2031 }, span)).toEqual({
			from: 2005,
			to: 2026,
			all: true
		});
	});

	it('leaves the league edges out of the state and never makes the span empty', () => {
		const picked = pickSeason(DEFAULT_STATE, 'from', 2010, span);
		expect(picked).toMatchObject({ from: 2010, to: null });
		// A first season after the last moves the last with it, and back.
		expect(pickSeason({ ...DEFAULT_STATE, to: 2012 }, 'from', 2018, span)).toMatchObject({
			from: 2018,
			to: 2018
		});
		expect(pickSeason({ ...DEFAULT_STATE, from: 2018 }, 'to', 2012, span)).toMatchObject({
			from: 2012,
			to: 2012
		});
		// Picking the league's own first season clears the end.
		expect(pickSeason(picked, 'from', 2005, span)).toMatchObject({ from: null, to: null });
	});

	it('labels the span', () => {
		expect(rangeLabel({ from: 2005, to: 2026, all: true })).toBe('all time');
		expect(rangeLabel({ from: 2010, to: 2020, all: false })).toBe('2010 to 2020');
		expect(rangeLabel({ from: 2020, to: 2020, all: false })).toBe('2020');
	});
});

describe('buildRangeRows', () => {
	/**
	 * Dev-database data from 2026-11-19: /api/managers/performance,
	 * /api/draft, /api/hall-of-fame and /api/managers/seasons, captured
	 * together so the career rows and the seasons agree.
	 */
	const s = JSON.parse(
		readFileSync(new URL('../../e2e/fixtures/league-table-seasons.json', import.meta.url), 'utf8')
	);
	const g = s.draft.data.draftGrades;
	const career = buildRows(s.all.data.performance, g, s.hallOfFame.hallOfFame);
	const seasons = s.seasons.data as SeasonsPayload;
	const span = seasonSpan(career)!;
	const draft = { bySeason: g.bySeason, spread: g.spread };

	it('gives back every career number when the span is every season', () => {
		const full = buildRangeRows(career, seasons, draft, { from: span.first, to: span.latest });
		expect(full.map((r) => r.name)).toEqual(career.map((r) => r.name));
		for (const row of career) {
			const ranged = full.find((r) => r.name === row.name)!;
			for (const column of Object.values(COLUMNS)) {
				// The Hall of Fame index is whole-career only.
				if (column.key === 'hof' || column.key === 'tier') continue;
				expect
					.soft([row.name, column.key, column.display(ranged).text], `${row.name} ${column.label}`)
					.toEqual([row.name, column.key, column.display(row).text]);
			}
		}
	});

	it('counts only the span, and reads the Hall of Fame index as All time only', () => {
		const range = { from: 2015, to: 2016 };
		const rows = buildRangeRows(career, seasons, draft, range);
		for (const row of rows) {
			const mine = seasons.teamSeasons.filter(
				(t) => t.managerName === row.name && t.seasonYear >= 2015 && t.seasonYear <= 2016
			);
			expect(row.wins).toBe(mine.reduce((a, t) => a + t.wins, 0));
			expect(row.seasons).toBe(new Set(mine.map((t) => t.seasonYear)).size);
			expect(row.firstSeason! >= 2015 && row.lastSeason! <= 2016).toBe(true);
			expect(COLUMNS.hof.display(row)).toMatchObject({ text: 'All time only', blank: true });
			expect(COLUMNS.tier.display(row).text).toBe('All time only');
		}
		// Nobody who sat out both seasons gets a row.
		const played = new Set(
			seasons.teamSeasons
				.filter((t) => t.seasonYear >= 2015 && t.seasonYear <= 2016)
				.map((t) => t.managerName)
		);
		expect(new Set(rows.map((r) => r.name))).toEqual(
			new Set(career.map((r) => r.name).filter((n) => played.has(n)))
		);
	});

	it('keeps league alum from the career, whatever the span', () => {
		const rows = buildRangeRows(career, seasons, draft, { from: 2005, to: 2008 });
		for (const row of rows) expect(row.alum).toBe(career.find((c) => c.name === row.name)!.alum);
	});

	it('runs streaks across seasons in the span and finished-season fields skip the live one', () => {
		const team = (over: Partial<TeamSeasonRow>): TeamSeasonRow => ({
			managerName: 'X',
			seasonYear: 2020,
			teamKey: 1,
			finished: true,
			wins: 0,
			losses: 0,
			ties: 0,
			games: 0,
			points: 0,
			pointsFor: 0,
			pointsAgainst: 0,
			titles: 0,
			titleGames: 0,
			playoffWins: 0,
			playoffLosses: 0,
			madePlayoffs: false,
			results: '',
			...over
		});
		const payload: SeasonsPayload = {
			teamSeasons: [
				team({
					seasonYear: 2020,
					teamKey: 1,
					wins: 2,
					losses: 1,
					games: 3,
					results: 'LWW',
					pointsFor: 300
				}),
				team({
					seasonYear: 2021,
					teamKey: 2,
					wins: 3,
					losses: 0,
					games: 3,
					results: 'WWW',
					pointsFor: 360
				}),
				team({
					seasonYear: 2022,
					teamKey: 3,
					finished: false,
					wins: 0,
					losses: 2,
					games: 2,
					results: 'LL',
					pointsFor: 100
				})
			],
			luck: [],
			moves: []
		};
		const [r] = buildRangeRows(
			[row({ name: 'X' })],
			payload,
			{ bySeason: [], spread: null },
			{
				from: 2020,
				to: 2022
			}
		);
		expect(r.winStreak).toBe(5);
		expect(r.lossStreak).toBe(2);
		expect(r.seasons).toBe(3);
		// Points per season: the two finished seasons only.
		expect(r.ppSeason).toBe(330);
		expect(r.bestSeason).toBe('3-0-0');
		expect(r.worstSeason).toBe('2-1-0');
		// No luck rows, no paid claims, no drafts: blank, not zero.
		expect(r.luck).toBeNull();
		expect(r.faab).toBeNull();
		expect(r.draftGrade).toBeNull();
	});

	it('takes the earlier season on a tie and leaves missing points against blank', () => {
		const team = (seasonYear: number, wins: number, losses: number): TeamSeasonRow => ({
			managerName: 'X',
			seasonYear,
			teamKey: seasonYear,
			finished: true,
			wins,
			losses,
			ties: 0,
			games: wins + losses,
			points: 100,
			pointsFor: null,
			pointsAgainst: null,
			titles: 0,
			titleGames: 0,
			playoffWins: 0,
			playoffLosses: 0,
			madePlayoffs: false,
			results: ''
		});
		const payload: SeasonsPayload = {
			// Same rates, different records, so the pick shows which season won the tie.
			teamSeasons: [team(2018, 2, 4), team(2019, 3, 0), team(2020, 1, 2), team(2021, 6, 0)],
			luck: [],
			moves: []
		};
		const [r] = buildRangeRows(
			[row({ name: 'X' })],
			payload,
			{ bySeason: [], spread: null },
			{ from: 2018, to: 2021 }
		);
		expect([r.bestSeason, r.worstSeason]).toEqual(['3-0-0', '2-4-0']);
		// No weekly rows: blank, as the career SUM would be, not 0.
		expect(r.pointsAgainst).toBeNull();
		expect(r.pointDiff).toBeNull();
		expect(r.ppSeason).toBeNull();
		expect(COLUMNS.pa.display(r)).toMatchObject({ blank: true });
	});
});
