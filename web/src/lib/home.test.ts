import { describe, expect, it } from 'vitest';
import {
	bannerWall,
	countWord,
	featuredRecords,
	finishTag,
	formatCount,
	formatOdds,
	formatRecord,
	joinNames,
	parseRecord,
	rivalry,
	rivalryLine,
	statStrip,
	thisWeekMode,
	titlesLabel,
	tradeTag,
	type H2HRow,
	type HallOfFameRow
} from './home';

// The Home board's renderVals, as the fixtures.

const HALL_OF_FAME: HallOfFameRow[] = [
	{ managerName: 'Trevor', championshipsWon: 7, hallOfFameRank: 1 },
	{ managerName: 'Gabe the Younger', championshipsWon: 3, hallOfFameRank: 2 },
	{ managerName: 'Israel', championshipsWon: 3, hallOfFameRank: 3 },
	{ managerName: 'Nick', championshipsWon: 3, hallOfFameRank: 4 },
	{ managerName: 'Luke S', championshipsWon: 2, hallOfFameRank: 5 },
	{ managerName: 'Justin', championshipsWon: 1, hallOfFameRank: 6 },
	{ managerName: 'Troy Colvin', championshipsWon: 1, hallOfFameRank: 7 },
	{ managerName: 'Gabe Flores', championshipsWon: 1, hallOfFameRank: 8 },
	{ managerName: 'Erik Snow', championshipsWon: null, hallOfFameRank: 9 },
	{ managerName: 'Omar', hallOfFameRank: 10 },
	{ managerName: 'Craig', championshipsWon: 0, hallOfFameRank: 11 },
	{ managerName: 'Bobby', championshipsWon: null, hallOfFameRank: 12 }
];

const CURRENT = [
	'Israel',
	'Craig',
	'Gabe Flores',
	'Trevor',
	'Luke S',
	'Nick',
	'Omar',
	'Erik Snow',
	'Gabe the Younger',
	'Troy Colvin'
];

const h2h = (a: string, b: string, aWins: number, bWins: number, first = '2005-W1'): H2HRow => ({
	managerAName: a,
	managerBName: b,
	managerAWins: aWins,
	managerBWins: bWins,
	totalMatchups: aWins + bWins,
	firstMatchupDate: first
});

const PAIR = h2h('Luke S', 'Trevor', 21, 19);

// Trevor trails Luke S (the rival) and Gabe the Younger among current
// managers; Bobby leads Trevor too but isn't a current manager.
const H2H: H2HRow[] = [
	PAIR,
	h2h('Israel', 'Trevor', 16, 20, '2005-W13'),
	h2h('Craig', 'Luke S', 21, 15, '2008-W7'),
	h2h('Trevor', 'Gabe the Younger', 14, 16, '2006-W2'),
	h2h('Bobby', 'Trevor', 9, 5, '2005-W3'),
	h2h('Nick', 'Trevor', 10, 12, '2009-W1')
];

describe('formatOdds', () => {
	it('prints whole percents and never claims certainty the sim has not earned', () => {
		expect(formatOdds(0.857, false)).toBe('86%');
		expect(formatOdds(0.996, false)).toBe('>99%');
		expect(formatOdds(0.004, false)).toBe('<1%');
	});

	it('prints 100% or 0% for a locked team', () => {
		expect(formatOdds(1, true)).toBe('100%');
		expect(formatOdds(0, true)).toBe('0%');
		expect(formatOdds(0.996, true)).toBe('0%');
	});
});

describe('formatRecord', () => {
	it('adds ties as a third number only when there are any', () => {
		expect(formatRecord(3, 0)).toBe('3-0');
		expect(formatRecord(2, 1, 0)).toBe('2-1');
		expect(formatRecord(2, 1, 1)).toBe('2-1-1');
	});
});

describe('thisWeekMode', () => {
	it('is the regular season until the odds say postseason, then final once complete', () => {
		expect(thisWeekMode(false, 'regular')).toBe('regular');
		expect(thisWeekMode(false, undefined)).toBe('regular');
		expect(thisWeekMode(false, 'postseason')).toBe('playoffs');
		expect(thisWeekMode(true, 'postseason')).toBe('final');
	});
});

describe('finishTag', () => {
	it("maps the API's tiers to the board's words and colors", () => {
		expect(finishTag('Champion')).toEqual({ text: 'Champion', variant: 'champion-strong' });
		expect(finishTag('Runner-up')).toEqual({ text: 'Runner-up', variant: 'champion' });
		expect(finishTag('Semifinalist')).toEqual({ text: 'Semifinalist', variant: 'bye' });
		expect(finishTag('Consolation Winner').text).toBe('Consolation winner');
		expect(finishTag('Consolation 4th (Last Place)')).toEqual({
			text: 'Last Place Trophy',
			variant: 'last-place'
		});
		expect(finishTag('Regular Season')).toEqual({ text: 'Missed playoffs', variant: 'eliminated' });
	});
});

describe('statStrip', () => {
	it("gives the board's four tiles with thousands separators", () => {
		expect(
			statStrip({
				totalSeasons: 22,
				firstSeason: '2005',
				lastSeason: '2026',
				totalManagers: 20,
				totalTransactions: 10612,
				totalMatchups: 1595,
				championshipGames: 21
			})
		).toEqual([
			{ value: '22', label: 'seasons, 2005 to 2026' },
			{ value: '20', label: 'managers all-time' },
			{ value: '10,612', label: 'transactions' },
			{ value: '1,595', label: 'matchups played' }
		]);
		expect(formatCount('1600')).toBe('1,600');
	});
});

describe('bannerWall', () => {
	it('drops managers with no titles and orders ties by Hall of Fame rank', () => {
		expect(bannerWall(HALL_OF_FAME)).toEqual([
			{ name: 'Trevor', count: 7 },
			{ name: 'Gabe the Younger', count: 3 },
			{ name: 'Israel', count: 3 },
			{ name: 'Nick', count: 3 },
			{ name: 'Luke S', count: 2 },
			{ name: 'Justin', count: 1 },
			{ name: 'Troy Colvin', count: 1 },
			{ name: 'Gabe Flores', count: 1 }
		]);
	});

	it('orders ties by rank even when the API hands them over out of order', () => {
		const shuffled = [HALL_OF_FAME[7], HALL_OF_FAME[5], HALL_OF_FAME[6], HALL_OF_FAME[0]];
		expect(bannerWall(shuffled).map((b) => b.name)).toEqual([
			'Trevor',
			'Justin',
			'Troy Colvin',
			'Gabe Flores'
		]);
	});

	it('names the pennants for a screen reader', () => {
		expect(titlesLabel(1)).toBe('1 title');
		expect(titlesLabel(7)).toBe('7 titles');
	});
});

describe('parseRecord', () => {
	it('takes the leading number and the week with the season', () => {
		expect(parseRecord('220.7 points (Week 17)', 2024)).toEqual({
			value: '220.7',
			when: 'Week 17, 2024'
		});
		expect(parseRecord('127.5 point margin (Week 10)', '2008')).toEqual({
			value: '127.5',
			when: 'Week 10, 2008'
		});
	});

	it('gives the season alone when the value names no week', () => {
		expect(parseRecord('2405.0 points', '2024')).toEqual({ value: '2405.0', when: '2024' });
	});
});

describe('featuredRecords', () => {
	it("picks the three single-game records in the board's order with manager names", () => {
		const records = [
			{
				record: 'Closest Win (Single Game)',
				value: '0.1 point margin (Week 6)',
				holder: 'Israel (Shoulda Kept Carolyn Rostered)',
				year: '2017',
				manager: 'Israel'
			},
			{
				record: 'Most Career Points',
				value: '43118.8 points',
				holder: 'Trevor',
				year: '',
				manager: 'Trevor'
			},
			{
				record: 'Highest Single Game Score',
				value: '220.7 points (Week 17)',
				holder: 'Nick (Omar’s Mental Faculties)',
				year: '2024'
			},
			{
				record: 'Biggest Blowout (Single Game)',
				value: '127.5 point margin (Week 10)',
				holder: 'Bobby (Casa de la carne)',
				year: '2008',
				manager: 'Bobby'
			}
		];
		expect(featuredRecords(records)).toEqual([
			{ label: 'Highest single-game score', value: '220.7', who: 'Nick', when: 'Week 17, 2024' },
			{ label: 'Biggest blowout', value: '127.5', who: 'Bobby', when: 'Week 10, 2008' },
			{ label: 'Closest win', value: '0.1', who: 'Israel', when: 'Week 6, 2017' }
		]);
	});
});

describe('rivalry', () => {
	it("puts the leader on the left with the board's score, start year and bar share", () => {
		expect(rivalry(PAIR)).toMatchObject({
			left: 'Luke S',
			right: 'Trevor',
			score: '21-19',
			meetings: 40,
			since: '2005',
			leftShare: 52.5,
			level: false
		});
	});

	it('flips the sides when manager B leads, and keeps A on the left when level', () => {
		expect(rivalry(h2h('Trevor', 'Luke S', 19, 21))).toMatchObject({
			left: 'Luke S',
			score: '21-19'
		});
		expect(rivalry(h2h('Israel', 'Omar', 10, 10))).toMatchObject({
			left: 'Israel',
			level: true,
			leftShare: 50
		});
	});
});

describe('rivalryLine', () => {
	it("gives the board's line on the board's data", () => {
		expect(rivalryLine(PAIR, HALL_OF_FAME, H2H, CURRENT)).toBe(
			'Seven titles, and Trevor still trails two current managers head to head: Luke S and Gabe the Younger.'
		);
	});

	it('says "One title" for a one-time champion', () => {
		const hof = [{ managerName: 'Trevor', championshipsWon: 1 }];
		expect(rivalryLine(PAIR, hof, H2H, CURRENT)).toBe(
			'One title, and Trevor still trails two current managers head to head: Luke S and Gabe the Younger.'
		);
	});

	it('drops the titles clause for a manager with none', () => {
		expect(rivalryLine(PAIR, [], H2H, CURRENT)).toBe(
			'Trevor trails two current managers head to head: Luke S and Gabe the Younger.'
		);
	});

	it('uses the singular for one name', () => {
		expect(rivalryLine(PAIR, HALL_OF_FAME, [PAIR], CURRENT)).toBe(
			'Seven titles, and Trevor still trails one current manager head to head: Luke S.'
		);
	});

	it('lists three names, the rival first then by meetings, and counts the rest', () => {
		const rows = [
			PAIR,
			h2h('Israel', 'Trevor', 20, 16),
			h2h('Craig', 'Trevor', 8, 6),
			h2h('Trevor', 'Nick', 9, 12),
			h2h('Omar', 'Trevor', 6, 5),
			h2h('Erik Snow', 'Trevor', 4, 3)
		];
		expect(rivalryLine(PAIR, [], rows, CURRENT)).toBe(
			'Trevor trails six current managers head to head: Luke S, Israel, Nick and three others.'
		);
		expect(rivalryLine(PAIR, [], rows.slice(0, 5), CURRENT)).toBe(
			'Trevor trails five current managers head to head: Luke S, Israel, Nick and two others.'
		);
		expect(rivalryLine(PAIR, [], rows.slice(0, 4), CURRENT)).toBe(
			'Trevor trails four current managers head to head: Luke S, Israel, Nick and one other.'
		);
	});

	it('writes counts of 10 and up as numerals', () => {
		const others = Array.from({ length: 11 }, (_, i) => `M${i}`);
		const rows = [PAIR, ...others.map((m) => h2h(m, 'Trevor', 3, 1))];
		expect(rivalryLine(PAIR, [], rows, [...CURRENT, ...others])).toBe(
			'Trevor trails 12 current managers head to head: Luke S, M0, M1 and nine others.'
		);
	});

	it('says the subject trails no one when no current manager leads them', () => {
		const formerRival = h2h('Bobby', 'Trevor', 21, 19);
		expect(rivalryLine(formerRival, HALL_OF_FAME, [formerRival], CURRENT)).toBe(
			"Seven titles, and Trevor doesn't trail any current manager head to head."
		);
	});

	it('says the series is split when it is level', () => {
		expect(rivalryLine(h2h('Luke S', 'Trevor', 20, 20), HALL_OF_FAME, H2H, CURRENT)).toBe(
			'Luke S and Trevor have split 40 meetings since 2005.'
		);
		expect(rivalryLine(h2h('Israel', 'Omar', 4, 4), HALL_OF_FAME, H2H, CURRENT)).toBe(
			'Israel and Omar have split eight meetings since 2005.'
		);
	});
});

describe('tradeTag', () => {
	const trade = {
		teamAManager: 'Trevor',
		teamBManager: 'Nick',
		teamAFinalScore: '61.4',
		teamBFinalScore: '39.6'
	};

	it("puts the winner's score first", () => {
		expect(tradeTag({ ...trade, tradeWinner: 'Trevor' })).toEqual({
			text: 'Trevor wins 61.4-39.6',
			even: false
		});
		expect(
			tradeTag({ ...trade, teamAFinalScore: '45.3', teamBFinalScore: '54.7', tradeWinner: 'Nick' })
		).toEqual({ text: 'Nick wins 54.7-45.3', even: false });
	});

	it('prints each score to one decimal', () => {
		expect(
			tradeTag({ ...trade, teamAFinalScore: 61, teamBFinalScore: 39, tradeWinner: 'Trevor' })?.text
		).toBe('Trevor wins 61.0-39.0');
	});

	it('reads "Even trade" on the wash', () => {
		expect(tradeTag({ ...trade, tradeWinner: 'Even Trade' })).toEqual({
			text: 'Even trade',
			even: true
		});
	});
});

describe('wording', () => {
	it('writes one to nine as words and joins names with a final "and"', () => {
		expect(countWord(2)).toBe('two');
		expect(countWord(10)).toBe('10');
		expect(joinNames(['Luke S'])).toBe('Luke S');
		expect(joinNames(['Luke S', 'Gabe the Younger'])).toBe('Luke S and Gabe the Younger');
		expect(joinNames(['A', 'B', 'C'])).toBe('A, B and C');
	});
});
