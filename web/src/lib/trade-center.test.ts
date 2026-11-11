import { describe, expect, it } from 'vitest';
import {
	championSide,
	gotBy,
	lopsidedRow,
	lopsidedTrades,
	perSeason,
	recentTrades,
	tradeMargin,
	tradeScoreLine,
	winnerShare,
	type TradeRow
} from './trade-center';

// The board's rows. teamAGives is what team A sent away.
const israelOmar2020: TradeRow = {
	seasonYear: 2020,
	transactionWeek: 3,
	teamAManager: 'Omar',
	teamAGives: 'Kyler Murray',
	teamBManager: 'Israel',
	teamBGives: 'Henry Ruggs III',
	productionDifferential: -308.9,
	teamAFinalScore: 18.5,
	teamBFinalScore: 81.5,
	tradeWinner: 'Israel',
	evaluationStatus: 'Complete'
};

const craigOmar2019: TradeRow = {
	seasonYear: 2019,
	transactionWeek: 3,
	teamAManager: 'Craig',
	teamAGives: 'Tom Brady, Damien Williams, Devin Singletary, Sammy Watkins',
	teamBManager: 'Omar',
	teamBGives: 'Lamar Jackson, Sony Michel, Tevin Coleman, Zach Ertz',
	productionDifferential: 291.2,
	teamAFinalScore: 71.1,
	teamBFinalScore: 28.9,
	tradeWinner: 'Craig'
};

const lukeOmar2014: TradeRow = {
	seasonYear: 2014,
	transactionWeek: 10,
	teamAManager: 'Luke S',
	teamAGives: 'Ben Tate, Fred Jackson, Percy Harvin',
	teamBManager: 'Omar',
	teamBGives: 'Demaryius Thomas, Odell Beckham Jr.',
	productionDifferential: 194.1,
	teamAFinalScore: 78.9,
	teamBFinalScore: 28.1,
	tradeWinner: 'Luke S',
	teamAChampion: 1
};

const nickIsrael2020: TradeRow = {
	seasonYear: 2020,
	transactionWeek: 5,
	teamAManager: 'Nick',
	teamAGives: 'DeSean Jackson, Raheem Mostert',
	teamBManager: 'Israel',
	teamBGives: 'Antonio Gibson, Myles Gaskin',
	productionDifferential: 182.8,
	teamAFinalScore: 68.5,
	teamBFinalScore: 31.5,
	tradeWinner: 'Nick'
};

const justinIsrael2005: TradeRow = {
	seasonYear: 2005,
	transactionWeek: 7,
	teamAManager: 'Israel',
	teamAGives: 'Shaun Alexander',
	teamBManager: 'Justin',
	teamBGives: 'Marc Bulger',
	productionDifferential: -181.0,
	teamAFinalScore: 32.6,
	teamBFinalScore: 78.9,
	tradeWinner: 'Justin',
	teamBChampion: 1
};

// Trevor and Nick, 2025: Trevor won on the final score and made the playoffs,
// though Nick's player scored slightly more.
const trevorNick2025: TradeRow = {
	seasonYear: 2025,
	transactionWeek: 7,
	teamAManager: 'Trevor',
	teamAGives: 'George Kittle',
	teamBManager: 'Nick',
	teamBGives: 'Breece Hall',
	tradeType: '1-for-1',
	teamAProduction: 140.7,
	teamBProduction: 142.1,
	productionDifferential: -1.4,
	teamAFinalScore: 61.4,
	teamBFinalScore: 39.6,
	teamAMadePlayoffs: 1,
	teamBMadePlayoffs: 0,
	tradeWinner: 'Trevor',
	evaluationStatus: 'Complete'
};

const trevorErik2025: TradeRow = {
	seasonYear: 2025,
	transactionWeek: 5,
	teamAManager: 'Trevor',
	teamAGives: 'J.K. Dobbins',
	teamBManager: 'Erik Snow',
	teamBGives: 'Chris Godwin, Chris Olave',
	tradeType: '1-for-2',
	teamAProduction: 299.1,
	teamBProduction: 59.8,
	productionDifferential: 239.3,
	teamAFinalScore: 61.3,
	teamBFinalScore: 38.7,
	teamAMadePlayoffs: 1,
	teamBMadePlayoffs: 1,
	tradeWinner: 'Trevor',
	evaluationStatus: 'Complete'
};

const israelGabe2026: TradeRow = {
	seasonYear: 2026,
	transactionWeek: 3,
	teamAManager: 'Israel',
	teamAGives: 'A.J. Brown, George Kittle',
	teamBManager: 'Gabe the Younger',
	teamBGives: 'Garrett Wilson, Jayden Daniels',
	tradeType: '2-for-2',
	teamAProduction: 62.2,
	teamBProduction: 26.2,
	productionDifferential: 36,
	teamAFinalScore: 54.7,
	teamBFinalScore: 45.3,
	tradeWinner: 'Israel',
	evaluationStatus: 'In Progress'
};

const five = [nickIsrael2020, craigOmar2019, israelOmar2020, justinIsrael2005, lukeOmar2014];

describe('lopsidedTrades', () => {
	it('sorts by the absolute production differential, largest first', () => {
		expect(lopsidedTrades(five).map((t) => t.productionDifferential)).toEqual([
			-308.9, 291.2, 194.1, 182.8, -181.0
		]);
	});

	it("doesn't reorder the API's array", () => {
		const input = [...five];
		lopsidedTrades(input);
		expect(input).toEqual(five);
	});
});

describe('lopsidedRow', () => {
	it('reads the winner, what each side got, the margin and the bar', () => {
		expect(lopsidedRow(israelOmar2020)).toEqual({
			winner: 'Israel',
			winnerGot: 'Kyler Murray',
			loser: 'Omar',
			loserGot: 'Henry Ruggs III',
			margin: { text: '+308.9', label: 'points gained', tone: 'field' },
			winnerScore: '81.5',
			loserScore: '18.5',
			share: 82,
			when: 'Week 3, 2020'
		});
	});
});

describe('tradeMargin', () => {
	it('reads "+" and "points gained" when the winner\'s players scored more', () => {
		// Israel is team B here, so the differential (A minus B) is negative.
		expect(tradeMargin(israelOmar2020)).toEqual({
			text: '+308.9',
			label: 'points gained',
			tone: 'field'
		});
		expect(tradeMargin(craigOmar2019).text).toBe('+291.2');
	});

	it('reads "1.4" and "points lost" for Trevor\'s 2025 trade, where his side scored 1.4 fewer', () => {
		expect(tradeMargin(trevorNick2025)).toEqual({ text: '1.4', label: 'points lost', tone: 'pennant' });
	});
});

describe('winnerShare', () => {
	it("is the winner's share of both scores, rounded", () => {
		expect(winnerShare(81.5, 18.5)).toBe(82);
		expect(winnerShare(71.1, 28.9)).toBe(71);
		expect(winnerShare(78.9, 28.1)).toBe(74);
	});

	it('is 0 when neither side scored', () => {
		expect(winnerShare(0, 0)).toBe(0);
	});
});

describe('championSide and gotBy', () => {
	it("gives the champion the other side's players", () => {
		expect(championSide(lukeOmar2014)).toBe('A');
		expect(gotBy(lukeOmar2014, 'A')).toBe('Demaryius Thomas, Odell Beckham Jr.');
		expect(championSide(justinIsrael2005)).toBe('B');
		expect(gotBy(justinIsrael2005, 'B')).toBe('Shaun Alexander');
	});
});

describe('tradeScoreLine', () => {
	it('reads "leading" while the trade is in progress', () => {
		expect(tradeScoreLine(israelGabe2026, 'A')).toEqual({ text: '54.7 · leading', tone: 'field' });
		expect(tradeScoreLine(israelGabe2026, 'B')).toEqual({ text: '45.3', tone: 'muted' });
	});

	it('names the margin when the winner also got the points and the swing is 50 or more', () => {
		expect(tradeScoreLine(trevorErik2025, 'A')).toEqual({
			text: '61.3 · won by 239.3 pts',
			tone: 'field'
		});
		expect(tradeScoreLine(trevorErik2025, 'B')).toEqual({ text: '38.7', tone: 'muted' });
	});

	it('says "won, made playoffs" when only the winner made the playoffs', () => {
		expect(tradeScoreLine(trevorNick2025, 'A')).toEqual({
			text: '61.4 · won, made playoffs',
			tone: 'field'
		});
		expect(tradeScoreLine(trevorNick2025, 'B')).toEqual({ text: '39.6', tone: 'muted' });
	});

	it('says plain "won" otherwise', () => {
		const plain = { ...trevorNick2025, teamBMadePlayoffs: 1 };
		expect(tradeScoreLine(plain, 'A')).toEqual({ text: '61.4 · won', tone: 'field' });
		// A swing under 50 isn't named, even when the winner's players scored more.
		const small = { ...trevorErik2025, productionDifferential: 49.9, teamBMadePlayoffs: 1 };
		expect(tradeScoreLine(small, 'A').text).toBe('61.3 · won');
	});

	it('shows both scores in muted for an Even Trade', () => {
		const even = { ...trevorNick2025, tradeWinner: 'Even Trade' };
		expect(tradeScoreLine(even, 'A')).toEqual({ text: '61.4', tone: 'muted' });
		expect(tradeScoreLine(even, 'B')).toEqual({ text: '39.6', tone: 'muted' });
	});
});

describe('perSeason and recentTrades', () => {
	it('divides the total over the seasons, one decimal', () => {
		expect(perSeason(104, 20)).toBe('5.2');
		expect(perSeason('104', 0)).toBeNull();
	});

	it('shows three until the list is opened', () => {
		const all = [1, 2, 3, 4, 5];
		expect(recentTrades(all, false)).toEqual([1, 2, 3]);
		expect(recentTrades(all, true)).toEqual(all);
	});
});
