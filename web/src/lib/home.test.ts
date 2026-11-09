import { describe, expect, it } from 'vitest';
import {
	bannerWall,
	featuredRecords,
	finishTag,
	formatCount,
	formatOdds,
	formatRecord,
	meetingsSince,
	parseRecord,
	rivalry,
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

const h2h = (a: string, b: string, aWins: number, bWins: number, first = '2005-W1'): H2HRow => ({
	managerAName: a,
	managerBName: b,
	managerAWins: aWins,
	managerBWins: bWins,
	totalMatchups: aWins + bWins,
	firstMatchupDate: first
});

const PAIR = h2h('Luke S', 'Trevor', 21, 19);

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

	it("falls back to the standings' weeks when the odds API didn't answer", () => {
		expect(thisWeekMode(false, undefined, 14, 14)).toBe('playoffs');
		expect(thisWeekMode(false, undefined, 8, 14)).toBe('regular');
		expect(thisWeekMode(false, 'regular', 14, 14)).toBe('regular');
	});
});

describe('finishTag', () => {
	it("maps the API's tiers to the board's words and colors", () => {
		expect(finishTag('Champion')).toEqual({ text: 'Champion', variant: 'champion-strong' });
		expect(finishTag('Runner-up')).toEqual({ text: 'Runner-up', variant: 'champion' });
		expect(finishTag('Semifinalist')).toEqual({ text: 'Semifinalist', variant: 'bye' });
		expect(finishTag('Consolation Winner')?.text).toBe('Consolation winner');
		expect(finishTag('Consolation 4th (Last Place)')).toEqual({
			text: 'Last Place Trophy',
			variant: 'last-place'
		});
		expect(finishTag('Regular Season')).toEqual({ text: 'Missed playoffs', variant: 'eliminated' });
	});

	it('gives no tag when the API sends no tier', () => {
		expect(finishTag(null)).toBeNull();
		expect(finishTag(undefined)).toBeNull();
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

describe('meetingsSince', () => {
	it('leaves the year out when the series has no start date', () => {
		expect(meetingsSince(40, '2005')).toBe('40 meetings since 2005');
		expect(meetingsSince(40, '')).toBe('40 meetings');
		expect(rivalry({ ...PAIR, firstMatchupDate: null }).since).toBe('');
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
