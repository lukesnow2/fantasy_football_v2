import { describe, expect, it } from 'vitest';
import {
	badgeTone,
	bestWorstSeason,
	careerMarks,
	draftRange,
	formatOdds,
	formatPct3,
	formatSigned,
	h2hCaption,
	h2hRows,
	h2hTiles,
	heroLine,
	ordinal,
	profileHref,
	profileKicker,
	profileTab,
	rankMedal,
	rankMedals,
	rankPhrase,
	railOrder,
	rivalryHref,
	rosterCounts,
	seasonOutcome,
	sentenceCase,
	shortRecord,
	tenureLine,
	titleGamesSummary,
	titleTripsLine,
	winsBars,
	winsChartLabel,
	type H2HApiRow,
	type SeasonLike,
	type TitleGame
} from './managers';
import { hofTier } from './hall-of-fame';

const mgr = (managerName: string, lastSeason: number, titles: number, pct: number) => ({
	managerName,
	lastSeason,
	totalChampionships: titles,
	winPercentage: pct
});

describe('rosterCounts', () => {
	it('counts named managers, and the active ones by the latest last season', () => {
		const rankings = [
			mgr('Trevor', 2026, 7, 0.56),
			mgr('Bobby', 2018, 0, 0.43),
			mgr('Omar', 2026, 0, 0.46),
			{ managerName: null, lastSeason: 2026, totalChampionships: 0, winPercentage: 0 }
		];
		expect(rosterCounts(rankings)).toEqual({ active: 2, allTime: 3 });
	});

	it('reads last seasons sent as strings', () => {
		expect(rosterCounts([{ ...mgr('A', 0, 0, 0), lastSeason: '2026' }])).toEqual({
			active: 1,
			allTime: 1
		});
	});

	it('is zero for an empty list', () => {
		expect(rosterCounts([])).toEqual({ active: 0, allTime: 0 });
	});
});

describe('railOrder', () => {
	it('lists active managers by titles then win rate, then alumni the same way', () => {
		const order = railOrder([
			mgr('Gabe the Younger', 2026, 3, 0.623),
			mgr('Trevor', 2026, 7, 0.563),
			mgr('Israel', 2026, 3, 0.532),
			mgr('Nick', 2026, 3, 0.486),
			mgr('Erik Snow', 2026, 0, 0.5),
			mgr('Bobby', 2018, 0, 0.429),
			mgr('Justin', 2009, 1, 0.55),
			mgr('Craig', 2026, 0, 0.459)
		]).map((e) => [e.manager.managerName, e.alum]);
		expect(order).toEqual([
			['Trevor', false],
			['Gabe the Younger', false],
			['Israel', false],
			['Nick', false],
			['Erik Snow', false],
			['Craig', false],
			['Justin', true],
			['Bobby', true]
		]);
	});

	it('is empty with no seasons', () => {
		expect(railOrder([])).toEqual([]);
	});
});

describe('profileTab and profileHref', () => {
	it('reads each tab', () => {
		expect(profileTab('/managers/Trevor?tab=seasons')).toBe('seasons');
		expect(profileTab('/managers/Trevor?tab=head-to-head')).toBe('head-to-head');
		expect(profileTab('/managers/Trevor?tab=achievements')).toBe('achievements');
		expect(profileTab(new URLSearchParams('tab=overview'))).toBe('overview');
		expect(profileTab(new URL('http://site/managers/Trevor?tab=seasons'))).toBe('seasons');
	});

	it('falls back to overview for a missing or unknown tab', () => {
		expect(profileTab('/managers/Trevor')).toBe('overview');
		expect(profileTab('/managers/Trevor?tab=draft')).toBe('overview');
	});

	it('builds links, overview with no tab', () => {
		expect(profileHref('Gabe the Younger')).toBe('/managers/Gabe%20the%20Younger');
		expect(profileHref('Luke S', 'head-to-head')).toBe('/managers/Luke%20S?tab=head-to-head');
	});
});

describe('rankMedals and rankMedal', () => {
	it('sorts by rank and keeps the given order on ties', () => {
		const sorted = rankMedals([
			{ label: 'Championships', rank: 2 },
			{ label: 'Win rate', rank: 1 },
			{ label: 'Scoring', rank: 1 },
			{ label: 'Playoff win rate', rank: 3 }
		]).map((m) => m.label);
		expect(sorted).toEqual(['Win rate', 'Scoring', 'Championships', 'Playoff win rate']);
	});

	it('puts a missing rank last', () => {
		expect(
			rankMedals([
				{ label: 'A', rank: null },
				{ label: 'B', rank: 4 }
			]).map((m) => m.label)
		).toEqual(['B', 'A']);
	});

	it('colors 1, 2, 3 and everything below', () => {
		expect(rankMedal(1)).toEqual({ medal: 'pennant', text: 'white', tile: 'pennant-tint' });
		expect(rankMedal(2)).toEqual({ medal: 'field', text: 'white', tile: 'field-tint' });
		expect(rankMedal(3)).toEqual({ medal: 'sage', text: 'white', tile: 'field-wash' });
		expect(rankMedal(4)).toEqual({ medal: 'line-strong', text: 'ink', tile: 'ground' });
	});
});

describe('rankPhrase and ordinal', () => {
	it('calls rank 1 the best', () => {
		expect(rankPhrase(1)).toBe('best in the league');
	});

	it('uses ordinals for the rest', () => {
		expect([2, 3, 4, 11, 12, 13, 21, 22, 23, 111].map(ordinal)).toEqual([
			'2nd',
			'3rd',
			'4th',
			'11th',
			'12th',
			'13th',
			'21st',
			'22nd',
			'23rd',
			'111th'
		]);
		expect(rankPhrase(4)).toBe('4th in the league');
	});
});

describe('words and numbers', () => {
	it('sentence-cases API labels', () => {
		expect(sentenceCase('League Legend')).toBe('League legend');
		expect(sentenceCase('Consistent Winner')).toBe('Consistent winner');
		expect(sentenceCase('best in the league')).toBe('Best in the league');
		expect(sentenceCase('')).toBe('');
	});

	it('prints win rates without the leading zero', () => {
		expect(formatPct3(0.6251)).toBe('.625');
		expect(formatPct3(1)).toBe('1.000');
		expect(formatPct3(0)).toBe('.000');
	});

	it('prints playoff odds to one place, and 100% or 0% only when certain', () => {
		expect(formatOdds(0.8019)).toBe('80.2%');
		expect(formatOdds(0.614)).toBe('61.4%');
		expect(formatOdds(0.5)).toBe('50.0%');
		expect(formatOdds(0.9996)).toBe('99.9%');
		expect(formatOdds(0.0004)).toBe('0.1%');
		expect(formatOdds(1)).toBe('100%');
		expect(formatOdds(0)).toBe('0%');
	});

	it('signs a differential with thousands', () => {
		expect(formatSigned(1893.71)).toBe('+1,893.7');
		expect(formatSigned(-12)).toBe('-12.0');
		expect(formatSigned(-0.04)).toBe('0.0');
	});

	it('shows ties only when there are some', () => {
		expect(shortRecord(13, 2, 0)).toBe('13-2');
		expect(shortRecord(9, 6, 1)).toBe('9-6-1');
	});

	it('builds the phone hero line', () => {
		expect(heroLine('147-87-1', 0.6251, 3)).toBe('147-87-1 · .625 · 3 titles');
		expect(heroLine('12-10-0', 0.545, 1)).toBe('12-10-0 · .545 · 1 title');
		expect(heroLine('12-10-0', 0.545, 0)).toBe('12-10-0 · .545');
	});

	it('says when a manager never missed a season', () => {
		expect(tenureLine(2005, 2026, 22)).toBe('2005 to 2026 · 22 seasons · never missed one');
		expect(tenureLine(2005, 2018, 9)).toBe('2005 to 2018 · 9 seasons');
		expect(tenureLine(2026, 2026, 1)).toBe('2026 to 2026 · 1 season · never missed one');
	});

	it('builds the kicker from the Hall of Fame rank', () => {
		expect(profileKicker(1, 2005)).toBe('Hall of Fame Elite · No. 1 all-time');
		expect(profileKicker(8, 2008)).toBe('Hall of Very Good · No. 8 all-time');
		expect(profileKicker(null, 2011)).toBe('Manager since 2011');
	});

	it('names Hall of Fame tiers by rank', () => {
		expect([1, 3, 4, 6, 7, 10, 11, 15, 16].map(hofTier)).toEqual([
			'Hall of Fame Elite',
			'Hall of Fame Elite',
			'Hall of Fame',
			'Hall of Fame',
			'Hall of Very Good',
			'Hall of Very Good',
			'Solid Contributor',
			'Solid Contributor',
			'Developing Legacy'
		]);
	});
});

describe('badgeTone', () => {
	it('tones each chip', () => {
		expect(badgeTone('Champion')).toBe('champion-strong');
		expect(badgeTone('Consistent Winner')).toBe('clinched');
		expect(badgeTone('High Scorer')).toBe('bye');
		expect(badgeTone('Long Timer')).toBe('eliminated');
		expect(badgeTone('Established')).toBe('eliminated');
		expect(badgeTone('Newcomer')).toBe('eliminated');
	});
});

describe('draftRange', () => {
	const bySeason = [
		{ managerName: 'Gabe', seasonYear: 2026, score: 9, inProgress: true },
		{ managerName: 'Gabe', seasonYear: 2025, score: -0.29, inProgress: false },
		{ managerName: 'Gabe', seasonYear: 2024, score: 2.66, inProgress: false },
		{ managerName: 'Gabe', seasonYear: 2012, score: -1.46, inProgress: false },
		{ managerName: 'Gabe', seasonYear: 2010, score: 2.66, inProgress: false },
		{ managerName: 'Omar', seasonYear: 2024, score: 5, inProgress: false }
	];

	it('picks the best and worst finished drafts, the newer on a tie, and the career grade', () => {
		expect(draftRange(bySeason, [{ managerName: 'Gabe', grade: 'B' }], 'Gabe')).toEqual({
			bestYear: 2024,
			worstYear: 2012,
			grade: 'B'
		});
	});

	it('is empty for a manager with no drafts', () => {
		expect(draftRange(bySeason, [], 'Bobby')).toEqual({
			bestYear: null,
			worstYear: null,
			grade: null
		});
	});
});

const season = (
	year: number,
	w: number,
	l: number,
	t = 0,
	extra: Partial<SeasonLike> = {}
): SeasonLike => ({
	seasonYear: year,
	wins: String(w),
	losses: String(l),
	ties: String(t),
	pointsFor: 2000,
	madePlayoffs: false,
	inProgress: false,
	...extra
});

describe('winsBars', () => {
	it('sums a two-team year and sizes bars by the most games in a year', () => {
		const bars = winsBars([
			season(2008, 7, 8),
			season(2008, 8, 7, 0, { madePlayoffs: true }),
			season(2009, 10, 5)
		]);
		expect(bars.map((b) => [b.year, b.wins, b.height, b.tone])).toEqual([
			[2008, 15, 50, 'playoffs'],
			[2009, 10, 33, 'missed']
		]);
	});

	it('orders oldest first, marks titles and the live season', () => {
		const bars = winsBars(
			[
				season(2026, 2, 1, 0, { inProgress: true }),
				season(2017, 13, 2),
				season(2016, 4, 11, 0, { madePlayoffs: false })
			],
			[2017]
		);
		expect(bars.map((b) => [b.label, b.tone, b.height])).toEqual([
			["'16", 'missed', 27],
			["'17", 'title', 87],
			["'26", 'progress', 13]
		]);
	});

	it('reads isChampion over the title years when the API sends it', () => {
		const [bar] = winsBars([season(2007, 12, 3, 0, { isChampion: false })], [2007]);
		expect(bar.tone).toBe('missed');
	});

	it('labels the chart with its span and titles', () => {
		const bars = winsBars(
			[season(2005, 12, 4), season(2006, 10, 5), season(2007, 12, 3)],
			[2006, 2007]
		);
		expect(winsChartLabel(bars)).toBe('Wins per season, 2005 to 2007. Titles in 2006 and 2007.');
		expect(winsChartLabel(winsBars([season(2011, 5, 9)]))).toBe(
			'Wins per season, 2011. No titles yet.'
		);
		expect(winsChartLabel(winsBars([season(2011, 9, 5), season(2012, 5, 9)], [2011]))).toBe(
			'Wins per season, 2011 to 2012. Title in 2011.'
		);
	});
});

describe('bestWorstSeason', () => {
	it('uses finished seasons only', () => {
		const { best, worst } = bestWorstSeason([
			season(2026, 4, 0, 0, { inProgress: true }),
			season(2017, 13, 2, 0, { madePlayoffs: true }),
			season(2016, 4, 11)
		]);
		expect([best?.seasonYear, worst?.seasonYear]).toEqual([2017, 2016]);
	});

	it('breaks ties by points for, higher for best and lower for worst', () => {
		const { best, worst } = bestWorstSeason([
			season(2010, 10, 5, 0, { pointsFor: 1900 }),
			season(2011, 10, 5, 0, { pointsFor: 2100 }),
			season(2012, 5, 10, 0, { pointsFor: 1800 }),
			season(2013, 5, 10, 0, { pointsFor: 1700 })
		]);
		expect([best?.seasonYear, worst?.seasonYear]).toEqual([2011, 2013]);
	});

	it('is empty with no finished season', () => {
		expect(bestWorstSeason([season(2026, 1, 0, 0, { inProgress: true })])).toEqual({
			best: null,
			worst: null
		});
	});
});

describe('seasonOutcome', () => {
	it('names a title, from isChampion or the title years', () => {
		expect(seasonOutcome(season(2017, 13, 2, 0, { isChampion: true }))).toMatchObject({
			label: 'Champion',
			tag: 'champion',
			champion: true
		});
		expect(seasonOutcome(season(2019, 11, 5), [2019]).label).toBe('Champion');
	});

	it('reads a finished season by its playoff place', () => {
		expect(seasonOutcome(season(2025, 8, 8, 0, { madePlayoffs: true }))).toMatchObject({
			label: 'Playoffs',
			tag: 'playoffs'
		});
		expect(seasonOutcome(season(2023, 5, 11))).toMatchObject({
			label: 'Missed playoffs',
			tag: 'missed'
		});
	});

	it('shows odds while the regular season runs', () => {
		expect(
			seasonOutcome(season(2026, 2, 1, 0, { inProgress: true, playoffOdds: 0.614 }))
		).toMatchObject({ label: 'In progress · 61.4% playoff odds', tag: 'in-progress' });
		expect(seasonOutcome(season(2026, 2, 1, 0, { inProgress: true })).label).toBe('In progress');
	});

	it('shows the bracket place once it is set', () => {
		const live = { inProgress: true, bracketSet: true, playoffOdds: 0.9 };
		expect(seasonOutcome(season(2026, 9, 5, 0, { ...live, madePlayoffs: true }))).toMatchObject({
			label: 'Playoffs · in progress',
			tag: 'playoffs'
		});
		expect(seasonOutcome(season(2026, 4, 10, 0, { ...live }))).toMatchObject({
			label: 'Missed playoffs',
			tag: 'missed'
		});
	});
});

const h2h = (
	opponent: string,
	games: number,
	wins: number,
	losses: number,
	extra: Partial<H2HApiRow> = {}
): H2HApiRow => ({
	opponent,
	totalMatchups: games,
	wins,
	losses,
	ties: games - wins - losses,
	avgPoints: 130,
	opponentAvgPoints: 120,
	currentStreak: 1,
	playoffWins: 0,
	playoffLosses: 0,
	opponentLastSeason: 2026,
	...extra
});

describe('h2hRows', () => {
	it('shows ties as a third number and counts them as half a win', () => {
		const [row] = h2hRows([h2h('Gabe the Younger', 30, 14, 15)]);
		expect(row.record).toBe('14-15-1');
		expect(row.pctText).toBe('.483');
		expect(row.pctTone).toBe('down');
	});

	it('places the bar right of center at .500 or better and left below', () => {
		const [a] = h2hRows([h2h('Troy Colvin', 23, 17, 6)]);
		expect(a.pctText).toBe('.739');
		expect(a.bar.left).toBe(50);
		expect(a.bar.width).toBeCloseTo(47.83, 2);
		expect(a.bar.tone).toBe('up');
		expect(a.pctTone).toBe('up');

		const [even] = h2hRows([h2h('Omar', 28, 14, 14)]);
		expect(even.bar).toEqual({ left: 50, width: 0, tone: 'up' });
		expect(even.pctTone).toBe('even');

		const [b] = h2hRows([h2h('Luke S', 40, 19, 21)]);
		expect(b.pctText).toBe('.475');
		expect(b.bar.width).toBeCloseTo(5, 5);
		expect(b.bar.left).toBeCloseTo(45, 5);
		expect(b.bar.tone).toBe('down');
	});

	it('caps the bar at the edge past .250 from center', () => {
		const [hi, lo] = h2hRows([h2h('A', 10, 9, 1), h2h('B', 10, 1, 9)]);
		expect(hi.bar).toEqual({ left: 50, width: 50, tone: 'up' });
		expect(lo.bar).toEqual({ left: 0, width: 50, tone: 'down' });
	});

	it('signs the streak and calls zero a tie', () => {
		const rows = h2hRows([
			h2h('A', 10, 6, 4, { currentStreak: 3 }),
			h2h('B', 10, 5, 5, { currentStreak: -7 }),
			h2h('C', 10, 4, 6, { currentStreak: 0 })
		]);
		expect(rows.map((r) => r.streak)).toEqual([
			{ text: 'W3', tone: 'up' },
			{ text: 'L7', tone: 'down' },
			{ text: 'Tie', tone: 'even' }
		]);
	});

	it('shows playoffs as a record, or null with no bracket games', () => {
		const [a, b] = h2hRows([
			h2h('A', 10, 6, 4, { playoffWins: 2, playoffLosses: 1 }),
			h2h('B', 10, 5, 5)
		]);
		expect(a.playoffs).toBe('2-1');
		expect(b.playoffs).toBeNull();
	});

	it('sorts by win rate, then games', () => {
		const rows = h2hRows([h2h('A', 10, 5, 5), h2h('B', 20, 10, 10), h2h('C', 10, 8, 2)]);
		expect(rows.map((r) => r.opponent)).toEqual(['C', 'B', 'A']);
	});
});

const game = (seasonYear: number, won: boolean): TitleGame => ({
	seasonYear,
	won,
	points: 130,
	opponentPoints: 120,
	opponent: 'Nick'
});

describe('h2hTiles', () => {
	const board = h2hRows([
		h2h('Troy Colvin', 23, 17, 6),
		h2h('Nick', 24, 16, 8),
		h2h('Omar', 28, 14, 14, { currentStreak: -2 }),
		h2h('Luke S', 40, 19, 21, { currentStreak: -7 }),
		h2h('Gabe the Younger', 30, 14, 15),
		h2h('Short', 6, 0, 6)
	]);
	const finals = [true, true, false, true, true, true, false, true, true, false].map((w, i) =>
		game(2006 + i, w)
	);

	it("picks the board's four tiles from opponents with 10 or more games", () => {
		const tiles = h2hTiles(board, finals);
		expect(tiles.map((t) => [t.label, t.value, t.sub])).toEqual([
			['Best against', '17-6', 'Troy Colvin, .739'],
			['Toughest', '19-21', 'Luke S, who has won the last 7'],
			['Dead even', '14-14', 'Omar, over 28 games'],
			['Title games', '7-3', 'Seven titles in 10 trips']
		]);
		expect(tiles.map((t) => t.tone)).toEqual(['field', 'pennant', 'ink', 'pennant-dark']);
	});

	it('names a streak of two or more on the toughest tile', () => {
		const tiles = h2hTiles(
			h2hRows([h2h('A', 20, 15, 5), h2h('Luke S', 40, 19, 21, { currentStreak: -7 })]),
			[]
		);
		expect(tiles[1].sub).toBe('Luke S, who has won the last 7');
		const one = h2hTiles(
			h2hRows([h2h('A', 20, 15, 5), h2h('B', 20, 8, 12, { currentStreak: -1 })]),
			[]
		);
		expect(one[1].sub).toBe('B, .400');
	});

	it('falls back to every opponent when none has 10 games', () => {
		const tiles = h2hTiles(h2hRows([h2h('A', 6, 5, 1), h2h('B', 6, 1, 5)]), []);
		expect(tiles[0].opponent).toBe('A');
		expect(tiles[1].opponent).toBe('B');
	});

	it('never picks a manager twice, and an empty tile says so', () => {
		const tiles = h2hTiles(h2hRows([h2h('Only', 12, 6, 6)]), []);
		expect(tiles[0].opponent).toBe('Only');
		expect(tiles[1]).toMatchObject({ opponent: null, value: null, sub: 'Not enough games yet' });
		expect(tiles[2]).toMatchObject({ opponent: null, value: null, sub: 'Not enough games yet' });
	});

	it('breaks ties by games, then name', () => {
		const tiles = h2hTiles(
			h2hRows([h2h('Zed', 10, 7, 3), h2h('Amy', 10, 7, 3), h2h('Big', 20, 14, 6)]),
			[]
		);
		expect(tiles[0].opponent).toBe('Big');
		expect(tiles[1].opponent).toBe('Amy');
	});

	it('writes every Title games line', () => {
		expect(titleTripsLine(7, 10)).toBe('Seven titles in 10 trips');
		expect(titleTripsLine(1, 3)).toBe('One title in 3 trips');
		expect(titleTripsLine(0, 2)).toBe('No titles in 2 trips');
		expect(titleTripsLine(1, 1)).toBe('One title in 1 trip');
		expect(titleTripsLine(12, 15)).toBe('12 titles in 15 trips');
		const none = h2hTiles(board, [])[3];
		expect(none).toMatchObject({ value: null, sub: 'No title games yet' });
	});
});

describe('the head to head links and caption', () => {
	it('opens the rivalry with both names', () => {
		expect(rivalryHref('Trevor', 'Gabe the Younger')).toBe(
			'/historical?tab=head-to-head&a=Trevor&b=Gabe%20the%20Younger'
		);
	});

	it('says how far the season has run', () => {
		expect(h2hCaption({ seasonYear: 2026, throughWeek: 3, final: false })).toBe(
			'Best record first · through week 3 of 2026'
		);
		expect(h2hCaption({ seasonYear: 2025, throughWeek: 14, final: true })).toBe(
			'Best record first · through the 2025 season'
		);
		expect(h2hCaption(null)).toBe('Best record first');
	});
});

describe('titleGamesSummary', () => {
	const finals = [game(2019, true), game(2017, true), game(2015, false)];

	it('says most in the league at rank 1 alone, and tied when shared', () => {
		expect(titleGamesSummary(finals, 1, false)).toEqual({
			wins: '2 wins',
			rest: ' in 3 trips, most in the league'
		});
		expect(titleGamesSummary(finals, 1, true)?.rest).toBe(
			' in 3 trips, tied for the most in the league'
		);
		expect(titleGamesSummary(finals, 2, false)?.rest).toBe(' in 3 trips');
	});

	it('says 1 win and 1 trip', () => {
		expect(titleGamesSummary([game(2020, true)], 3, false)).toEqual({
			wins: '1 win',
			rest: ' in 1 trip'
		});
	});

	it('is null with no title games', () => {
		expect(titleGamesSummary([], 5, false)).toBeNull();
	});
});

describe('careerMarks', () => {
	it('reads finished seasons only', () => {
		const marks = careerMarks({
			streaks: { longestWinStreak: 10, longestLossStreak: 6 },
			seasons: [
				season(2026, 3, 1, 0, { inProgress: true, madePlayoffs: true }),
				season(2017, 13, 2, 0, { madePlayoffs: true }),
				season(2016, 4, 11),
				season(2015, 10, 5, 0, { madePlayoffs: true })
			],
			championshipSeasons: [2017],
			playoffWinPercentage: 0.6667,
			playoffRank: 1
		});
		expect(marks.map((m) => [m.label, m.value, m.sub, m.tone])).toEqual([
			['Longest win streak', '10', 'Games in a row', 'field'],
			['Longest losing streak', '6', 'Games in a row', 'pennant'],
			['Best season', '13-2', '2017, won the title', 'ink'],
			['Worst season', '4-11', '2016, missed the playoffs', 'ink'],
			['Playoff trips', '2', 'In 3 finished seasons', 'ink'],
			['Playoff win rate', '66.7%', 'Best in the league', 'ink']
		]);
	});

	it('says made the playoffs, and ranks below first', () => {
		const marks = careerMarks({
			streaks: null,
			seasons: [season(2015, 10, 5, 0, { madePlayoffs: true }), season(2016, 2, 12)],
			playoffWinPercentage: 0.5,
			playoffRank: 3
		});
		expect(marks[0].value).toBeNull();
		expect(marks[2].sub).toBe('2015, made the playoffs');
		expect(marks[5].sub).toBe('3rd in the league');
	});
});
