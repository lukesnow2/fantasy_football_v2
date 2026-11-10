import { describe, expect, it } from 'vitest';
import { pointsLeaderHistory, pointsLeaderTile, type PointsLeaderRow } from './points-leaders';

function row(
	season: number,
	pointsFor: number,
	madePlayoffs = false,
	wonTitle = false
): PointsLeaderRow {
	return { season, managerName: null, pointsFor, madePlayoffs, wonTitle };
}

describe('pointsLeaderHistory', () => {
	it("counts each season's leader once", () => {
		expect(
			pointsLeaderHistory([
				row(2005, 1500, true, true),
				row(2005, 1400, true, false),
				row(2006, 1300, false, false),
				row(2006, 1600, true, false),
				row(2007, 1200, true, true),
				row(2007, 1100, true, false)
			])
		).toEqual({ seasons: 3, madePlayoffs: 3, wonTitle: 2, firstSeason: 2005 });
	});

	it('takes a leader who missed the playoffs as missed', () => {
		const h = pointsLeaderHistory([row(2010, 1700), row(2010, 1600, true, true)]);
		expect(h).toMatchObject({ seasons: 1, madePlayoffs: 0, wonTitle: 0 });
	});

	it('counts a season tied at one decimal once, won when either leader won', () => {
		const h = pointsLeaderHistory([
			row(2012, 1500.04, true, false),
			row(2012, 1500.01, true, true),
			row(2012, 1400, true, false)
		]);
		expect(h).toEqual({ seasons: 1, madePlayoffs: 1, wonTitle: 1, firstSeason: 2012 });
	});

	it('is empty with no rows', () => {
		expect(pointsLeaderHistory([])).toEqual({
			seasons: 0,
			madePlayoffs: 0,
			wonTitle: 0,
			firstSeason: null
		});
	});
});

describe('pointsLeaderTile', () => {
	it('prints the tile when every leader made the playoffs', () => {
		expect(
			pointsLeaderTile({ seasons: 21, madePlayoffs: 21, wonTitle: 8, firstSeason: 2005 })
		).toEqual({
			label: 'Points leaders who won the title',
			value: '8 of 21',
			sub: 'Since 2005. All 21 made the playoffs.'
		});
	});

	it('words the playoff count with the house rule for numbers', () => {
		const sub = (madePlayoffs: number, seasons = 21) =>
			pointsLeaderTile({ seasons, madePlayoffs, wonTitle: 3, firstSeason: 2005 })!.sub;
		expect(sub(19)).toBe('Since 2005. 19 made the playoffs.');
		expect(sub(7, 9)).toBe('Since 2005. Seven made the playoffs.');
		expect(sub(9, 9)).toBe('Since 2005. All nine made the playoffs.');
		expect(sub(0, 2)).toBe('Since 2005. None made the playoffs.');
	});

	it('draws nothing without finished seasons', () => {
		expect(
			pointsLeaderTile({ seasons: 0, madePlayoffs: 0, wonTitle: 0, firstSeason: null })
		).toBeNull();
	});
});
