import { describe, expect, it } from 'vitest';
import {
	computeEras,
	defaultPair,
	eraButton,
	filterRivalries,
	findings,
	findRivalry,
	historyTab,
	luckier,
	managerSlug,
	parseRecordValue,
	recordBook,
	recordChips,
	recordFootnote,
	recordLabel,
	recordWho,
	recordWhen,
	resolveManager,
	seriesShare,
	spanYears,
	winShare,
	type ApiRecord,
	type Headline
} from './history-display';

const headline = (over: Partial<Headline> = {}): Headline => ({
	recordPersistence: { r: -0.0084, n: 162 },
	skillShare: 0.319,
	championTopThree: { count: 14, seasons: 21 },
	topScorerTitles: 8,
	draftedShare: 0.7506,
	seasons: 21,
	...over
});

describe('tabs and the URL (S10.1)', () => {
	it('reads the tab, defaulting to overview', () => {
		expect(historyTab(new URLSearchParams(''))).toBe('overview');
		expect(historyTab(new URLSearchParams('tab=head-to-head'))).toBe('head-to-head');
		expect(historyTab(new URLSearchParams('tab=records'))).toBe('records');
		expect(historyTab(new URLSearchParams('tab=nope'))).toBe('overview');
	});

	it('slugs a name the way the server does', () => {
		expect(managerSlug('Gabe the Younger')).toBe('gabe-the-younger');
		expect(managerSlug('Luke S')).toBe('luke-s');
	});

	it('resolves a manager from a slug or a name, ignoring unknowns', () => {
		const names = ['Luke S', 'Trevor', 'Gabe the Younger'];
		expect(resolveManager('luke-s', names)).toBe('Luke S');
		expect(resolveManager('trevor', names)).toBe('Trevor');
		expect(resolveManager('Gabe the Younger', names)).toBe('Gabe the Younger');
		expect(resolveManager('nobody', names)).toBeNull();
		expect(resolveManager(null, names)).toBeNull();
	});
});

describe('findings (S10.2)', () => {
	it('signs r with two decimals and reads each threshold', () => {
		expect(findings(headline())[0]).toMatchObject({
			value: '-0.01',
			head: "Last season's record tells you nothing about this one.",
			sub: 'Correlation across 162 manager-seasons.'
		});
		expect(findings(headline({ recordPersistence: { r: 0.0999, n: 39 } }))[0].head).toBe(
			"Last season's record tells you nothing about this one."
		);
		expect(findings(headline({ recordPersistence: { r: 0.1, n: 39 } }))[0].head).toBe(
			"Last season's record says little about this one."
		);
		expect(findings(headline({ recordPersistence: { r: -0.29, n: 39 } }))[0].head).toBe(
			"Last season's record says little about this one."
		);
		const strong = findings(headline({ recordPersistence: { r: 0.3, n: 39 } }))[0];
		expect(strong.head).toBe("Last season's record carries over into this one.");
		expect(strong.value).toBe('+0.30');
	});

	it('rounds the shares to whole percents', () => {
		const f = findings(headline({ skillShare: 0.425, draftedShare: 0.7506 }));
		expect(f[1]).toMatchObject({ value: '43%', tone: 'field' });
		expect(f[1].sub).toBe("The rest is luck, using Tango's method over 21 seasons.");
		expect(f[3]).toMatchObject({ value: '75%', tone: 'ink' });
	});

	it('counts top-3 champions and says once for one title', () => {
		const f = findings(headline());
		expect(f[2]).toMatchObject({ value: '14 of 21', tone: 'pennant', sub: 'The top scorer won it 8 times.' });
		expect(findings(headline({ topScorerTitles: 1 }))[2].sub).toBe('The top scorer won it once.');
	});

	it('draws nothing without a headline', () => {
		expect(findings(null)).toEqual([]);
	});

	it('names an era button with its two-digit years', () => {
		expect(eraButton({ name: 'Early Era', start: 2005, end: 2012 })).toEqual({ name: 'Early', years: '05 to 12' });
	});
});

describe('three eras (S10.2)', () => {
	const years = Array.from({ length: 6 }, (_, i) => 2005 + i);
	const champs = ['Trevor', 'Israel', 'Trevor', 'Nick', 'Israel', 'Omar'];
	const ov = {
		leagueEvolution: years.map((y, i) => ({ seasonYear: y, championManager: champs[i], totalTransactions: '1200' })),
		tradeActivity: years.map((y) => ({ seasonYear: y, totalTrades: '10' })),
		scoringPatterns: years.map((y) => ({ seasonYear: y, avgWeeklyScore: '120.04' }))
	};

	it('builds "to" ranges with thousands commas and sentence-case names', () => {
		const eras = computeEras(ov);
		expect(eras.map((e) => [e.name, e.span])).toEqual([
			['Early era', '2005 to 2006'],
			['Middle era', '2007 to 2008'],
			['Modern era', '2009 to 2010']
		]);
		expect(eras[0].rows).toEqual([
			{ label: 'Seasons', value: '2' },
			{ label: 'Avg weekly score', value: '120.0' },
			{ label: 'Trades', value: '20' },
			{ label: 'Transactions', value: '2,400' },
			{ label: 'Different champions', value: '2' }
		]);
	});

	it('joins a tie for most titles with "and"', () => {
		const eras = computeEras(ov);
		expect(eras[0].titles).toBe('Israel and Trevor (1)');
		expect(eras[0].leaders).toEqual(['Israel', 'Trevor']);
	});
});

describe('head to head (S10.5 to S10.7)', () => {
	const row = {
		managerAName: 'Luke S',
		managerBName: 'Trevor',
		managerAWins: 21,
		managerBWins: 19,
		seriesRecord: '21-19-0',
		totalMatchups: 40,
		seasonsPlayedTogether: 21,
		firstMatchupDate: '2005-W1',
		lastMatchupDate: '2025-W5',
		managerAPythagoreanWins: '18.77',
		managerBPythagoreanWins: '21.23',
		managerALuckFactor: '2.2300',
		managerBLuckFactor: '-2.2300',
		championshipMatchups: 2,
		playoffMatchups: 5
	};

	it('reads a span in years', () => {
		expect(spanYears('2005-W1', '2025-W5')).toBe('2005 to 2025');
		expect(spanYears('2011-W3', '2011-W9')).toBe('2011');
	});

	it('flips a pair stored the other way round', () => {
		const flipped = findRivalry([row], 'Trevor', 'Luke S')!;
		expect(flipped).toMatchObject({ a: 'Trevor', b: 'Luke S', aWins: 19, bWins: 21, aPythag: 21.23, aLuck: -2.23 });
		expect(findRivalry([row], 'Luke S', 'Trevor')).toMatchObject({ a: 'Luke S', aWins: 21, span: '2005 to 2025' });
		expect(findRivalry([row], 'Luke S', 'Nick')).toBeNull();
	});

	it('puts the series leader on the left of the default pair, unless the URL names one', () => {
		expect(defaultPair([row], null, null)).toEqual(['Luke S', 'Trevor']);
		expect(defaultPair([{ ...row, managerAWins: 15, managerBWins: 25 }], null, null)).toEqual(['Trevor', 'Luke S']);
		expect(defaultPair([row], 'Nick', 'Omar')).toEqual(['Nick', 'Omar']);
		expect(defaultPair([row], 'Nick', 'Nick')).toEqual(['Luke S', 'Trevor']);
	});

	it('leaves track showing for ties in the share bar', () => {
		expect(seriesShare(21, 19, 40)).toEqual({ a: 52.5, b: 47.5 });
		const tied = seriesShare(15, 14, 30);
		expect(tied.a + tied.b).toBeCloseTo((29 / 30) * 100);
		expect(winShare(16, 20).a).toBeCloseTo(44.444, 2);
	});

	it('names the luckier manager, or Even', () => {
		expect(luckier(row)).toEqual({ name: 'Luke S', luck: '+2.23' });
		expect(luckier({ ...row, managerALuckFactor: '-0.4600', managerBLuckFactor: '-1.1' })).toEqual({ name: 'Luke S', luck: '-0.46' });
		expect(luckier({ ...row, managerALuckFactor: '0.00', managerBLuckFactor: '0.0000' })).toBeNull();
	});

	it('filters the table only after a name is picked (D10)', () => {
		const rows = [row, { ...row, managerAName: 'Nick', managerBName: 'Omar' }];
		expect(filterRivalries(rows, false, 'Luke S', 'Trevor')).toHaveLength(2);
		expect(filterRivalries(rows, true, 'Luke S', 'Nick')).toHaveLength(2);
		expect(filterRivalries(rows, true, 'Trevor', 'Israel')).toHaveLength(1);
	});
});

describe('record book (S10.8)', () => {
	// Every record_name the view returns today.
	const VIEW_NAMES: [string, string][] = [
		['Highest Single Game Score', 'Highest single-game score'],
		['Most Points Scored in a Season', 'Most points in a season'],
		['Most Career Points', 'Most career points'],
		['Largest Point Differential in a Season', 'Largest point differential in a season'],
		['Most Weeks as Top Scorer in a Season', 'Most weeks as top scorer in a season'],
		['Highest Scoring Game (Combined Points)', 'Highest-scoring game, both teams'],
		['Lowest Single Game Score', 'Lowest single-game score'],
		['Lowest Scoring Game (Combined Points)', 'Lowest-scoring game, both teams'],
		['Worst Point Differential in a Season', 'Worst point differential in a season'],
		['Most Points Allowed in a Season (Unluckiest Team)', 'Most points allowed in a season'],
		['Most Career Points Allowed (Unluckiest Manager)', 'Most career points allowed'],
		['Fewest Points Allowed in a Season (Luckiest Team)', 'Fewest points allowed in a season'],
		['Fewest Points Scored in a Season', 'Fewest points scored in a season'],
		['Best Regular-Season Record', 'Best regular-season record'],
		['Worst Regular-Season Record', 'Worst regular-season record'],
		['Best Career Win Percentage (Min 3 Seasons)', 'Best career win percentage, 3+ seasons'],
		['Closest Win (Single Game)', 'Closest win'],
		['Most Career Transactions', 'Most career transactions'],
		['Most Trades in a Season', 'Most trades in a season'],
		['Biggest Blowout (Single Game)', 'Biggest blowout'],
		['Most Championships (Career)', 'Most championships'],
		['Most Championship Appearances (Career)', 'Most title-game appearances'],
		['Most Playoff Appearances (Career)', 'Most playoff appearances']
	];

	it('maps every view name to a board label, and falls back to sentence case', () => {
		for (const [name, label] of VIEW_NAMES) expect(recordLabel(name)).toBe(label);
		expect(recordLabel('Most FAAB Spent in a Season')).toBe('Most faab spent in a season');
	});

	it('splits values, units and weeks', () => {
		expect(parseRecordValue('220.7 points (Week 17)')).toEqual({ value: '220.7', unit: 'points', week: 17 });
		expect(parseRecordValue('2405.0 points')).toEqual({ value: '2,405.0', unit: 'points', week: null });
		expect(parseRecordValue('43118.8 points').value).toBe('43,118.8');
		expect(parseRecordValue('13-2 (86.7%)')).toEqual({ value: '13-2', unit: '86.7%', week: null });
		expect(parseRecordValue('+605.0')).toEqual({ value: '+605.0', unit: '', week: null });
		expect(parseRecordValue('-602.4').value).toBe('-602.4');
		expect(parseRecordValue('62.3%')).toEqual({ value: '62.3%', unit: '', week: null });
		expect(parseRecordValue('0.1 point margin (Week 16)')).toEqual({ value: '0.1', unit: 'point margin', week: 16 });
	});

	it('renames units as drawn', () => {
		expect(parseRecordValue('2345.5 points allowed')).toMatchObject({ value: '2,345.5', unit: 'allowed' });
		expect(parseRecordValue('395.0 total points (Week 2)')).toMatchObject({ unit: 'points', week: 2 });
		expect(parseRecordValue('776 transactions').unit).toBe('moves');
		expect(parseRecordValue('7 championships').unit).toBe('titles');
		expect(parseRecordValue('10 appearances').unit).toBe('appearances');
	});

	it('names holders by manager, with a hidden manager last', () => {
		expect(recordWho('--hidden-- vs Luke S')).toEqual({ who: 'Luke S vs. hidden manager', face: 'Luke S', names: ['Luke S'] });
		expect(recordWho('Luke S vs Jesse Brown')).toMatchObject({ who: 'Luke S vs. Jesse Brown', face: 'Luke S' });
		expect(recordWho('Nick')).toMatchObject({ who: 'Nick', face: 'Nick' });
	});

	it('says when a record was set', () => {
		expect(recordWhen('game', '2024', 17)).toBe(' · Week 17, 2024');
		expect(recordWhen('season', '2024', null)).toBe(' · 2024');
		expect(recordWhen('career', '', null)).toBe('');
	});

	const rec = (record: string, category: string, value = '1 points', year = '2010', manager = 'Nick'): ApiRecord => ({
		record,
		value,
		year,
		manager,
		category
	});
	const all = [
		rec('Most Championships (Career)', 'Championship Records', '7 championships', ''),
		rec('Lowest Single Game Score', 'Scoring Records', '39.2 points (Week 10)', '2026'),
		rec('Highest Single Game Score', 'Scoring Records', '220.7 points (Week 17)', '2024'),
		rec('Best Regular-Season Record', 'Season Records', '13-2 (86.7%)'),
		rec('Biggest Blowout (Single Game)', 'Other Records', '127.5 point margin (Week 10)', '2008'),
		rec('Most Career Transactions', 'Transaction Records', '776 transactions', '')
	];

	it('counts each chip, leaving out empty categories', () => {
		expect(recordChips(all)).toEqual([
			{ id: 'all', label: 'All', count: 6 },
			{ id: 'Championship Records', label: 'Championships', count: 1 },
			{ id: 'Scoring Records', label: 'Scoring', count: 2 },
			{ id: 'Season Records', label: 'Season', count: 1 },
			{ id: 'Transaction Records', label: 'Transactions', count: 1 },
			{ id: 'Other Records', label: 'Other', count: 1 }
		]);
	});

	it('groups in board order and filters by chip', () => {
		const book = recordBook(all, 'all', 2026);
		expect(book.champion.map((r) => r.label)).toEqual(['Most championships']);
		expect(book.groups.map((g) => [g.name, g.count])).toEqual([
			['Scoring records', '2 records'],
			['Season records', '1 record'],
			['Transactions and other', '2 records']
		]);
		expect(book.groups[0].rows.map((r) => r.label)).toEqual(['Highest single-game score', 'Lowest single-game score']);
		expect(book.groups[2].rows.map((r) => r.label)).toEqual(['Most career transactions', 'Biggest blowout']);

		const scoring = recordBook(all, 'Scoring Records', 2026);
		expect(scoring.champion).toEqual([]);
		expect(scoring.groups.map((g) => g.name)).toEqual(['Scoring records']);
	});

	it('tags a single game set in the season in progress, and colors records against the holder', () => {
		const rows = recordBook(all, 'Scoring Records', 2026).groups[0].rows;
		expect(rows[1]).toMatchObject({ live: '2026 in progress', tone: 'pennant', when: ' · Week 10, 2026' });
		expect(rows[0]).toMatchObject({ live: null, tone: 'ink' });
		expect(recordBook(all, 'Scoring Records', null).groups[0].rows[1].live).toBeNull();
	});

	it('writes the footnote, dropping the season clause with no season in progress', () => {
		expect(recordFootnote(2026, '2026-11-07T18:00:00Z')).toBe(
			'Single-game records count as soon as the week is final. Season and career records only count finished seasons, so 2026 joins them after the championship. Last updated Nov 7, 2026.'
		);
		expect(recordFootnote(null, '2026-11-07T18:00:00Z')).toBe(
			'Single-game records count as soon as the week is final. Season and career records only count finished seasons. Last updated Nov 7, 2026.'
		);
	});
});
