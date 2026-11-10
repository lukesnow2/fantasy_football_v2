/**
 * Notes from week N (WP4 S4.6): a library of sentence templates filled from
 * data the page already loads. No model and no API key: nothing a template
 * prints can come from anywhere but the responses passed in.
 *
 * Writing rules every template follows: they/them for every manager; counts
 * one to nine as words, 10 and up as numerals; points to one decimal; gaps
 * rounded to whole points; ordinals as words; records as W-L, with ties as a
 * third number.
 */

import { formatOdds, formatRecord } from '$lib/home';
import { compareSeeding } from '$lib/playoff-odds';
import { countWord, startHistory, type StartHistoryRow } from '$lib/start-history';
import { nameList, sameRecord } from '$lib/this-season';

export interface NoteManager {
	name: string;
	wins: number;
	losses: number;
	ties: number;
	pointsFor: number;
	pointsAgainst: number;
	/** 0 to 1, null when the odds didn't load. */
	playoffOdds: number | null;
	previousOdds: number | null;
	/** 1 is first. Null when the power rankings didn't load. */
	powerRank: number | null;
	/** Positive means a climb since last week. Null in week 1. */
	rankChange: number | null;
}

export interface WeekNotesInput {
	/** The week the notes are from (the odds' last simulated week). */
	week: number;
	/** In standings order: the leader first. */
	managers: NoteManager[];
	/** The start-history rows for the same week, or null when they didn't load. */
	history: StartHistoryRow[] | null;
}

export interface Note {
	id: string;
	text: string;
	score: number;
	/** The first manager the note names; one note per subject makes the cut. */
	subject: string;
}

interface Template {
	id: string;
	build(input: WeekNotesInput): Omit<Note, 'id'> | null;
}

const record = (m: NoteManager) => formatRecord(m.wins, m.losses, m.ties);
const games = (m: NoteManager) => m.wins + m.losses + m.ties;

const ORDINALS = [
	'',
	'',
	'second',
	'third',
	'fourth',
	'fifth',
	'sixth',
	'seventh',
	'eighth',
	'ninth'
];

/** Rank 1 by points for is the most, 2 the second-most. */
function pointsRank(m: NoteManager, all: NoteManager[]): number {
	return 1 + all.filter((o) => o.pointsFor > m.pointsFor).length;
}

/** Rank 1 from the bottom is the fewest. */
function pointsRankFromBottom(m: NoteManager, all: NoteManager[]): number {
	return 1 + all.filter((o) => o.pointsFor < m.pointsFor).length;
}

/** Managers in current power-rank order, unranked last. */
function byPowerRank(list: NoteManager[]): NoteManager[] {
	return [...list].sort((a, b) => (a.powerRank ?? Infinity) - (b.powerRank ?? Infinity));
}

const losingButScoring: Template = {
	id: 'losing-but-scoring',
	build({ managers }) {
		const fits = managers.filter((m) => m.losses > m.wins && pointsRank(m, managers) <= 3);
		if (fits.length === 0) return null;
		const m = fits.reduce((a, b) => (b.pointsFor > a.pointsFor ? b : a));
		const rank = pointsRank(m, managers);
		const most = rank === 1 ? 'the most' : `the ${ORDINALS[rank]}-most`;
		return {
			text: `${m.name} is ${record(m)} with ${most} points in the league. The schedule owes them one.`,
			score: 80 + 10 * (m.losses - m.wins - 1) - 5 * (rank - 1),
			subject: m.name
		};
	}
};

const pointsAgainstLeader: Template = {
	id: 'points-against-leader',
	build({ managers }) {
		if (managers.length < 2) return null;
		const [first, second] = [...managers].sort((a, b) => b.pointsAgainst - a.pointsAgainst);
		const gap = first.pointsAgainst - second.pointsAgainst;
		if (gap < 15) return null;
		const weeks = games(first) === 1 ? 'one week' : `${countWord(games(first))} weeks`;
		return {
			text: `${first.name} has given up ${first.pointsAgainst.toFixed(1)} points in ${weeks}, the most by ${Math.round(gap)}.`,
			score: Math.min(85, 45 + gap),
			subject: first.name
		};
	}
};

/** Everyone who shares the biggest rank move in one direction, if it's three or more. */
function biggestMove(
	managers: NoteManager[],
	direction: 1 | -1
): { who: NoteManager[]; n: number } | null {
	const moves = managers.filter((m) => m.rankChange != null && m.rankChange * direction > 0);
	if (moves.length === 0) return null;
	const n = Math.max(...moves.map((m) => m.rankChange! * direction));
	if (n < 3) return null;
	return { who: byPowerRank(moves.filter((m) => m.rankChange! * direction === n)), n };
}

const powerClimb: Template = {
	id: 'power-climb',
	build({ managers }) {
		const move = biggestMove(managers, 1);
		if (!move || move.who.length > 3) return null;
		const list = move.who.map((m) => m.name);
		const verb = list.length === 1 ? 'climbed' : list.length === 2 ? 'both climbed' : 'all climbed';
		return {
			text: `${nameList(list)} ${verb} ${countWord(move.n)} spots in the power rankings.`,
			score: 36 + 8 * move.n,
			subject: list[0]
		};
	}
};

const powerFall: Template = {
	id: 'power-fall',
	build({ managers }) {
		const move = biggestMove(managers, -1);
		if (!move || move.who.length > 2) return null;
		const list = move.who.map((m) => m.name);
		const verb = list.length === 1 ? 'fell' : 'both fell';
		return {
			text: `${nameList(list)} ${verb} ${countWord(move.n)} spots in the power rankings.`,
			score: 30 + 6 * move.n,
			subject: list[0]
		};
	}
};

const winningButNotScoring: Template = {
	id: 'winning-but-not-scoring',
	build({ managers }) {
		const fits = managers.filter(
			(m) => m.wins > m.losses && pointsRankFromBottom(m, managers) <= 3
		);
		if (fits.length === 0) return null;
		const m = fits.reduce((a, b) => (b.pointsFor < a.pointsFor ? b : a));
		const rank = pointsRankFromBottom(m, managers);
		const fewest = rank === 1 ? 'the fewest' : `the ${ORDINALS[rank]}-fewest`;
		return {
			text: `${m.name} is ${record(m)} with ${fewest} points in the league.`,
			score: 70 + 10 * (m.wins - m.losses - 1) - 5 * (rank - 1),
			subject: m.name
		};
	}
};

const oddsSwing: Template = {
	id: 'odds-swing',
	build({ managers }) {
		let best: { m: NoteManager; moved: number } | null = null;
		for (const m of managers) {
			if (m.playoffOdds == null || m.previousOdds == null) continue;
			const moved = Math.round((m.playoffOdds - m.previousOdds) * 100);
			if (!best || Math.abs(moved) > Math.abs(best.moved)) best = { m, moved };
		}
		if (!best || Math.abs(best.moved) < 15) return null;
		const { m, moved } = best;
		const from = formatOdds(m.previousOdds!, false);
		const to = formatOdds(m.playoffOdds!, false);
		const which = moved > 0 ? 'jump' : 'drop';
		return {
			text: `Playoff odds for ${m.name} went from ${from} to ${to}, the biggest ${which} in the league.`,
			score: 40 + Math.abs(moved),
			subject: m.name
		};
	}
};

const roughStartHistory: Template = {
	id: 'rough-start-history',
	build({ managers, history }) {
		if (!history || managers.length === 0) return null;
		const ordered = [...managers].sort(compareSeeding);
		const worst = ordered[ordered.length - 1];
		const leader = ordered[0];
		if (sameRecord(worst, leader)) return null;
		const sharing = managers.filter((m) => sameRecord(m, worst));
		if (sharing.length > 2) return null;
		const past = startHistory(history, worst);
		if (past.count < 5 || past.firstSeason === null) return null;
		const wl = record(worst);
		const who = byPowerRank(sharing).map((m) => m.name);
		const lead = who.length === 1 ? `${who[0]} is ${wl}.` : `${nameList(who)} are ${wl}.`;
		const since =
			past.madePlayoffs === 0
				? `Since ${past.firstSeason}, none of the ${countWord(past.count)} teams that started ${wl} made the playoffs.`
				: `Since ${past.firstSeason}, ${countWord(past.madePlayoffs)} of ${countWord(past.count)} teams that started ${wl} made the playoffs.`;
		return {
			text: `${lead} ${since}`,
			score: 50 + 35 * (1 - past.madePlayoffs / past.count),
			subject: who[0]
		};
	}
};

/** The templates in template order, which breaks ties in score. */
export const TEMPLATES: Template[] = [
	losingButScoring,
	pointsAgainstLeader,
	powerClimb,
	powerFall,
	winningButNotScoring,
	oddsSwing,
	roughStartHistory
];

/**
 * Up to three notes: every note that applies, the highest-scoring one per
 * subject, sorted by score and then template order.
 */
export function buildWeekNotes(input: WeekNotesInput, templates: Template[] = TEMPLATES): Note[] {
	const notes = templates.flatMap((t, order) => {
		const note = t.build(input);
		return note ? [{ id: t.id, ...note, order }] : [];
	});
	const best = new Map<string, (typeof notes)[number]>();
	for (const note of notes) {
		const kept = best.get(note.subject);
		if (!kept || note.score > kept.score) best.set(note.subject, note);
	}
	return [...best.values()]
		.sort((a, b) => b.score - a.score || a.order - b.order)
		.slice(0, 3)
		.map(({ id, text, score, subject }) => ({ id, text, score, subject }));
}
