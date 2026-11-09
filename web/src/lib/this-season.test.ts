import { describe, expect, it } from 'vitest';
import {
	championLine,
	finishGroups,
	finishTag,
	moveLine,
	nameList,
	oddsDetail,
	oddsMethod,
	oddsTone,
	pageKicker,
	playoffLineLabel,
	recordOrder,
	seasonState,
	seasonStatus,
	standingsRows,
	statTiles,
	updatedAgo,
	formatPoints,
	type OddsTeam,
	type StandingRow,
	type StatTilesInput
} from './this-season';

/** The This Season board's week 3: name, W-L, PF, PA, odds now, odds last week. */
const BOARD: [string, number, number, number, number, number, number][] = [
	['Israel', 3, 0, 424.1, 380.5, 85, 79],
	['Craig', 2, 1, 465.5, 402.2, 86, 89],
	['Gabe Flores', 2, 1, 368.35, 346.3, 63, 46],
	['Trevor', 2, 1, 368.2, 359.95, 61, 55],
	['Nick', 1, 2, 450.8, 432.65, 75, 83],
	['Omar', 1, 2, 372.45, 377.9, 51, 58],
	['Erik Snow', 1, 2, 363.15, 357.6, 48, 59],
	['Gabe the Younger', 1, 2, 357.4, 361.8, 45, 54],
	['Luke S', 1, 2, 353.25, 381.35, 45, 40],
	['Troy Colvin', 1, 2, 337.05, 460.0, 40, 39]
];

const standings: StandingRow[] = BOARD.map(([managerName, wins, losses, pointsFor, pointsAgainst], i) => ({
	rank: i + 1,
	managerName,
	wins,
	losses,
	ties: 0,
	pointsFor,
	pointsAgainst
}));

function team(over: Partial<OddsTeam> & { managerName: string }): OddsTeam {
	return {
		teamKey: 1,
		teamName: 'A team name',
		wins: 0,
		losses: 0,
		ties: 0,
		pointsFor: 0,
		allPlayWins: 0,
		allPlayLosses: 0,
		luck: 0,
		projectedPoints: 0,
		projectedPointsRank: 1,
		remainingOpponentPoints: null,
		remainingGames: 0,
		projectedWins: 0,
		playoffOdds: 0,
		byeOdds: 0,
		topSeedOdds: 0,
		titleOdds: 0,
		clinched: false,
		eliminated: false,
		previousOdds: null,
		...over
	};
}

const odds = BOARD.map(([managerName, , , , , now, prev]) =>
	team({ managerName, playoffOdds: now / 100, previousOdds: prev / 100 })
);

describe('seasonState', () => {
	it('is final once the season is complete', () => {
		expect(seasonState({ isSeasonComplete: true })).toBe('final');
	});
	it('is live in progress', () => {
		expect(seasonState({ isSeasonComplete: false })).toBe('live');
	});
	it('is live with nothing loaded yet or an empty new season', () => {
		expect(seasonState(null)).toBe('live');
		expect(seasonState({})).toBe('live');
	});
});

describe('seasonStatus and pageKicker', () => {
	it('names the playoffs, the deadline, or the regular season', () => {
		expect(seasonStatus({ playoffStatus: 'Playoffs' })).toBe('Playoffs are heating up');
		expect(seasonStatus({ tradeDeadlineStatus: 'Passed' })).toBe('Trade deadline passed');
		expect(seasonStatus({ tradeDeadlineStatus: 'Unknown' })).toBe('Regular season in progress');
	});
	it('prints the board kickers, desktop and phone', () => {
		expect(pageKicker('live', 2026, 3, 14, 'Regular season in progress')).toEqual({
			desktop: '2026 Season · Week 3 of 14 · Regular season in progress',
			phone: '2026 · Week 3 of 14'
		});
		expect(pageKicker('final', 2025, 17, 14, '')).toEqual({
			desktop: '2025 Season · Season complete',
			phone: '2025 · Season complete'
		});
	});
});

describe('statTiles', () => {
	const base: StatTilesInput = {
		currentWeek: 3,
		regularSeasonWeeks: 14,
		playoffStartWeek: 15,
		phase: 'regular',
		throughWeek: 3,
		standings,
		history: {
			label: '3-0 starts that made the playoffs',
			value: '30 of 32',
			sub: 'Since 2005. Four won the title.'
		}
	};

	it("draws the board's four tiles for one unbeaten team", () => {
		expect(statTiles(base)).toEqual([
			{ label: 'Current week', value: '3 of 14', sub: 'Playoffs start week 15', phone: 'Current week' },
			{ label: 'Most points for', value: '465.5', sub: 'Craig', phone: 'Most points · Craig' },
			{ label: 'Only unbeaten', value: '3-0', sub: 'Israel', phone: 'Unbeaten · Israel' },
			{
				label: '3-0 starts that made the playoffs',
				value: '30 of 32',
				sub: 'Since 2005. Four won the title.',
				phone: '3-0 starts that made the playoffs'
			}
		]);
	});

	it('names two unbeaten teams under "Unbeaten"', () => {
		const two = standings.map((r) => (r.managerName === 'Craig' ? { ...r, wins: 3, losses: 0 } : r));
		expect(statTiles({ ...base, standings: two })[2]).toEqual({
			label: 'Unbeaten',
			value: '3-0',
			sub: 'Israel and Craig',
			phone: 'Unbeaten · Israel and Craig'
		});
	});

	it('counts more than three unbeaten teams', () => {
		const many = standings.map((r, i) => (i < 4 ? { ...r, wins: 3, losses: 0 } : r));
		expect(statTiles({ ...base, standings: many })[2].sub).toBe('4 managers');
	});

	it('falls back to the best record with none unbeaten', () => {
		const none = standings.map((r) => (r.managerName === 'Israel' ? { ...r, wins: 2, losses: 1 } : r));
		expect(statTiles({ ...base, standings: none })[2]).toEqual({
			label: 'Best record',
			value: '2-1',
			sub: '4 managers',
			phone: 'Best record · 4 managers'
		});
	});

	it('joins managers tied on points for with "and"', () => {
		const tied = standings.map((r) => (r.managerName === 'Nick' ? { ...r, pointsFor: 465.5 } : r));
		expect(statTiles({ ...base, standings: tied })[1].sub).toBe('Craig and Nick');
	});

	it('reads "Playoffs" and drops the history tile in the postseason', () => {
		const tiles = statTiles({ ...base, phase: 'postseason', currentWeek: 16 });
		expect(tiles).toHaveLength(3);
		expect(tiles[0]).toMatchObject({ value: 'Playoffs', sub: 'Week 16' });
	});

	it('drops the history tile before week 1, and the record tiles with no games', () => {
		expect(statTiles({ ...base, throughWeek: 0 })).toHaveLength(3);
		expect(statTiles({ ...base, throughWeek: 0, standings: [] })).toHaveLength(1);
	});
});

describe('nameList', () => {
	it('lists one, two and three names, and counts more', () => {
		expect(nameList(['A'])).toBe('A');
		expect(nameList(['A', 'B'])).toBe('A and B');
		expect(nameList(['A', 'B', 'C'])).toBe('A, B and C');
		expect(nameList(['A', 'B', 'C', 'D'])).toBe('4 managers');
	});
});

describe('standingsRows', () => {
	const rows = standingsRows(standings, odds, 6);

	it('joins odds by manager and keeps the standings order', () => {
		expect(rows.map((r) => r.managerName)).toEqual(BOARD.map((b) => b[0]));
		expect(rows[0]).toMatchObject({ record: '3-0', pointsFor: '424.1', oddsText: '85%' });
	});

	it('computes Diff with a sign, one decimal', () => {
		expect(rows[0]).toMatchObject({ diff: '+43.6', diffNegative: false });
		expect(rows[9]).toMatchObject({ diff: '-122.9', diffNegative: true });
		const even = standingsRows([{ ...standings[0], pointsFor: 400, pointsAgainst: 400 }], odds, 6);
		expect(even[0]).toMatchObject({ diff: '0.0', diffNegative: false });
	});

	it('prints every move in playoff odds, as drawn', () => {
		expect(rows[0].weekChangeText).toBe('▲ 6');
		expect(rows[1].weekChangeText).toBe('▼ 3');
		const same = standingsRows(standings, [team({ managerName: 'Israel', playoffOdds: 0.5, previousOdds: 0.5 })], 6);
		expect(same[0].weekChangeText).toBe('Same');
	});

	it('leaves Wk Δ empty in week 1', () => {
		const week1 = standingsRows(standings, [team({ managerName: 'Israel', playoffOdds: 0.6 })], 6);
		expect(week1[0]).toMatchObject({ weekChange: null, weekChangeText: '' });
	});

	it('marks the playoff field and names no team', () => {
		expect(rows.filter((r) => r.aboveLine)).toHaveLength(6);
		expect(JSON.stringify(rows)).not.toContain('A team name');
	});

	it('prints certainty only for a locked team', () => {
		const locked = standingsRows(standings, [team({ managerName: 'Israel', playoffOdds: 1, clinched: true })], 6);
		expect(locked[0].oddsText).toBe('100%');
		const nearly = standingsRows(standings, [team({ managerName: 'Israel', playoffOdds: 0.999 })], 6);
		expect(nearly[0].oddsText).toBe('>99%');
	});
});

describe('playoffLineLabel', () => {
	it('names the field and the byes', () => {
		expect(playoffLineLabel(6, 2)).toBe('Playoff line · top 6, 2 byes');
		expect(playoffLineLabel(4, 1)).toBe('Playoff line · top 4, 1 bye');
		expect(playoffLineLabel(4, 0)).toBe('Playoff line · top 4');
	});
});

describe('finishTag', () => {
	it.each([
		['Champion', 'Champion', 'champion-strong'],
		['Runner-up', 'Runner-up', 'champion'],
		['Semifinalist', 'Semifinalist', 'bye'],
		['Quarterfinalist', 'Quarterfinalist', 'bye'],
		['Consolation Winner', 'Consolation winner', 'eliminated'],
		['Consolation Runner-up', 'Consolation runner-up', 'eliminated'],
		['Consolation 3rd', 'Consolation 3rd', 'eliminated'],
		['Consolation 4th (Last Place)', 'Last Place Trophy', 'last-place'],
		['Regular Season', 'Missed playoffs', 'eliminated']
	])('maps %s', (tier, text, variant) => {
		expect(finishTag(tier)).toEqual({ text, variant });
	});
});

/** The after-the-final board's 2025 season. */
const FINAL: StandingRow[] = [
	['Gabe the Younger', 7, 7, 1805.7, 1799.2, 'Champion'],
	['Omar', 11, 3, 2015.1, 1685.8, 'Runner-up'],
	['Luke S', 7, 7, 1873.7, 1934.3, 'Semifinalist'],
	['Gabe Flores', 9, 5, 2007.3, 1777.6, 'Semifinalist'],
	['Trevor', 7, 7, 1934.3, 1849.4, 'Quarterfinalist'],
	['Erik Snow', 9, 5, 1794.6, 1755.3, 'Quarterfinalist'],
	['Troy Colvin', 5, 9, 1808.7, 1809.0, 'Consolation Winner'],
	['Nick', 3, 11, 1390.9, 1857.5, 'Consolation Runner-up'],
	['Craig', 6, 8, 1720.8, 1845.8, 'Consolation 3rd'],
	['Israel', 6, 8, 1746.2, 1783.4, 'Consolation 4th (Last Place)']
].map(([managerName, wins, losses, pointsFor, pointsAgainst, playoffTier], i) => ({
	rank: i + 1,
	managerName: managerName as string,
	wins: wins as number,
	losses: losses as number,
	ties: 0,
	pointsFor: pointsFor as number,
	pointsAgainst: pointsAgainst as number,
	playoffTier: playoffTier as string
}));

describe('recordOrder and finishGroups', () => {
	it("seeds by wins, then points for, as the board's Record view", () => {
		expect(recordOrder(FINAL).map((r) => r.managerName)).toEqual([
			'Omar',
			'Gabe Flores',
			'Erik Snow',
			'Trevor',
			'Luke S',
			'Gabe the Younger',
			'Israel',
			'Craig',
			'Troy Colvin',
			'Nick'
		]);
	});

	it('groups the Playoff finish view', () => {
		expect(finishGroups(FINAL).map((g) => [g.name, g.rows.length])).toEqual([
			['Championship', 2],
			['Semifinalists', 2],
			['Quarterfinalists', 2],
			['Consolation bracket', 4]
		]);
	});

	it('prints points with a thousands separator', () => {
		expect(formatPoints(1805.7)).toBe('1,805.7');
		expect(formatPoints(1390.94)).toBe('1,390.9');
	});
});

describe('championLine', () => {
	const seeded = recordOrder(FINAL);
	const seed = (name: string) => {
		const i = seeded.findIndex((r) => r.managerName === name);
		return { ...seeded[i], seed: i + 1 };
	};

	it("prints the board's line word for word", () => {
		expect(championLine(seed('Gabe the Younger'), seed('Omar'))).toBe(
			'Won it from the 6 seed after a 7-7 regular season. Omar, the 11-3 top seed, finished second.'
		);
	});

	it('names a top-seed champion and a lower-seeded runner-up', () => {
		expect(championLine(seed('Omar'), seed('Gabe the Younger'))).toBe(
			'Won it as the top seed after an 11-3 regular season. Gabe the Younger, the 6 seed at 7-7, finished second.'
		);
	});

	it('leaves the runner-up part off with no runner-up', () => {
		expect(championLine(seed('Gabe the Younger'), null)).toBe(
			'Won it from the 6 seed after a 7-7 regular season.'
		);
	});
});

describe('oddsTone', () => {
	it.each([
		[75, 'var(--color-field)'],
		[74, 'var(--color-sage)'],
		[40, 'var(--color-sage)'],
		[15, 'var(--color-sage-tint)'],
		[14, 'var(--color-line-strong)']
	])('%i%% fills with %s', (p, color) => {
		expect(oddsTone(p)).toBe(color);
	});
});

describe('oddsDetail', () => {
	it("prints Craig's open row as the board does", () => {
		const craig = team({
			managerName: 'Craig',
			projectedPoints: 141.94,
			projectedPointsRank: 1,
			allPlayWins: 20,
			allPlayLosses: 7,
			luck: -0.21,
			remainingGames: 11,
			remainingOpponentPoints: 126.93,
			projectedWins: 8.71,
			byeOdds: 0.45,
			topSeedOdds: 0.25,
			titleOdds: 0.19
		});
		expect(oddsDetail(craig, 14)).toEqual([
			{ term: 'Projected score', value: '141.9 a week, #1' },
			{ term: 'All-play record', value: '20-7' },
			{ term: 'Luck', value: '-0.2 wins' },
			{ term: 'Schedule left', value: '11 games, opponents avg 126.9' },
			{ term: 'Projected finish', value: '8.7-5.3' },
			{ term: 'Bye · #1 seed · Title', value: '45% · 25% · 19%' }
		]);
	});

	it('keeps a half game in the all-play record and signs positive luck', () => {
		const t = team({ managerName: 'A', allPlayWins: 20.5, allPlayLosses: 6.5, luck: 1.06 });
		const rows = oddsDetail(t, 14);
		expect(rows[1].value).toBe('20.5-6.5');
		expect(rows[2].value).toBe('+1.1 wins');
	});
});

describe('oddsMethod', () => {
	it("prints the board's two paragraphs for six teams and two byes", () => {
		expect(oddsMethod(10000, 6, 2)).toEqual([
			'Each team gets a projected score, pulled toward the league average early in the year. The rest of the season is played 10,000 times with that score drifting week to week, and seeds go by wins, then points for.',
			'Six make it and the top two get byes: 3 plays 6, 4 plays 5. Back-tested on 2008 to 2025, teams given 85 to 95% made it 90% of the time.'
		]);
	});

	it('describes other formats', () => {
		expect(oddsMethod(10000, 4, 1)[1]).toMatch(/^The top 4 make it and the top 1 get byes\. Back/);
		expect(oddsMethod(10000, 4, 0)[1]).toMatch(/^The top 4 make it\. Back/);
	});
});

describe('moveLine', () => {
	it.each([
		['add', 'picked up', 'Add', 'bye'],
		['drop', 'dropped', 'Drop', 'champion'],
		['add/drop', 'swapped in', 'Add/drop', 'eliminated'],
		['trade', 'traded for', 'Trade', 'eliminated']
	])('words a %s', (type, verb, tag, tone) => {
		expect(moveLine({ type, playerName: 'Green Bay', managerName: 'Gabe Flores', faabBid: null })).toEqual({
			verb,
			player: 'Green Bay',
			tag,
			tone
		});
	});

	it('adds a FAAB bid in whole dollars, $0 included', () => {
		const line = (faabBid: number) =>
			moveLine({ type: 'add', playerName: 'T.J. Hockenson', managerName: 'Gabe Flores', faabBid }).player;
		expect(line(0)).toBe('T.J. Hockenson for $0 FAAB');
		expect(line(12.4)).toBe('T.J. Hockenson for $12 FAAB');
	});
});

describe('updatedAgo', () => {
	const now = new Date('2026-09-25T12:00:00Z');
	const ago = (minutes: number) => new Date(now.getTime() - minutes * 60_000).toISOString();

	it.each([
		[30, 'Updated less than an hour ago'],
		[60, 'Updated 1 hour ago'],
		[300, 'Updated 5 hours ago'],
		[1440, 'Updated 1 day ago'],
		[4320, 'Updated 3 days ago']
	])('%i minutes reads "%s"', (minutes, text) => {
		expect(updatedAgo(ago(minutes), now)).toBe(text);
	});

	it('gives nothing for a bad timestamp', () => {
		expect(updatedAgo('not a date', now)).toBeNull();
	});
});

describe('formatDiff', () => {
	it("rounds the way the board's script does", async () => {
		const { formatDiff } = await import('./this-season');
		expect(formatDiff(368.2, 359.95).text).toBe('+8.3');
		expect(formatDiff(337.05, 460).text).toBe('-122.9');
	});
});
