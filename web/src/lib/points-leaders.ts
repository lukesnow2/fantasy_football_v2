/**
 * How the regular season's points leaders have done, for This Season's history
 * tile. It replaces the tile that matched the standings leader's record, which
 * changed every week. This one only changes when a season ends. The rows come
 * from /api/standings/points-leaders: one per team-season in a finished season.
 * Counting and wording live here, out of the page, so vitest holds them.
 */

import { countWord } from '$lib/start-history';

/** One team-season's regular-season points for, and how its season ended. */
export interface PointsLeaderRow {
	season: number;
	managerName: string | null;
	pointsFor: number;
	madePlayoffs: boolean;
	wonTitle: boolean;
}

export interface PointsLeaderHistory {
	/** Finished seasons with a points leader. */
	seasons: number;
	/** Seasons whose points leader made the playoffs. */
	madePlayoffs: number;
	/** Seasons whose points leader won the title. */
	wonTitle: number;
	firstSeason: number | null;
}

/**
 * Each season's points leader, and how many made the playoffs and won the
 * title. Points are compared at one decimal, as the page prints them, so two
 * leaders tied there both count: the season counts once, and it counts as made
 * or won when either leader did.
 */
export function pointsLeaderHistory(rows: PointsLeaderRow[]): PointsLeaderHistory {
	const bySeason = new Map<number, PointsLeaderRow[]>();
	for (const row of rows) {
		const list = bySeason.get(row.season);
		if (list) list.push(row);
		else bySeason.set(row.season, [row]);
	}
	let seasons = 0;
	let madePlayoffs = 0;
	let wonTitle = 0;
	let firstSeason: number | null = null;
	for (const [season, list] of bySeason) {
		const top = Math.max(...list.map((r) => r.pointsFor)).toFixed(1);
		const leaders = list.filter((r) => r.pointsFor.toFixed(1) === top);
		seasons++;
		if (leaders.some((r) => r.madePlayoffs)) madePlayoffs++;
		if (leaders.some((r) => r.wonTitle)) wonTitle++;
		if (firstSeason === null || season < firstSeason) firstSeason = season;
	}
	return { seasons, madePlayoffs, wonTitle, firstSeason };
}

export interface HistoryTile {
	label: string;
	value: string;
	sub: string;
}

function capitalize(text: string): string {
	return text.charAt(0).toUpperCase() + text.slice(1);
}

/**
 * The stat strip's history tile: "Points leaders who won the title", "8 of
 * 21", "Since 2005. All 21 made the playoffs." Null with no finished seasons.
 */
export function pointsLeaderTile(history: PointsLeaderHistory): HistoryTile | null {
	if (history.seasons === 0 || history.firstSeason === null) return null;
	const made =
		history.madePlayoffs === history.seasons
			? `All ${countWord(history.seasons)} made the playoffs.`
			: history.madePlayoffs === 0
				? 'None made the playoffs.'
				: `${capitalize(countWord(history.madePlayoffs))} made the playoffs.`;
	return {
		label: 'Points leaders who won the title',
		value: `${history.wonTitle} of ${history.seasons}`,
		sub: `Since ${history.firstSeason}. ${made}`
	};
}
