/**
 * Pure rules behind This Season (WP4). Every line on the page is computed from
 * an API response, so the rules live here, out of the page, where vitest holds
 * them. playoff-odds.ts stays frozen (G3): this imports its seeding order and
 * types and changes nothing in it.
 */

import { compareSeeding, type TeamOdds as EngineTeamOdds } from '$lib/playoff-odds';
import { finishTag, formatOdds, formatRecord } from '$lib/home';
import { historyTile, type HistoryTile } from '$lib/start-history';
import type { TagVariant } from '$lib/components/ui/Tag.svelte';

export { finishTag, formatOdds, formatRecord };

/* ---------- API shapes, as the page reads them ---------- */

/** One row of /api/standings. */
export interface StandingRow {
	rank: number;
	managerName: string;
	wins: number;
	losses: number;
	ties: number;
	pointsFor: number;
	pointsAgainst: number;
	playoffTier?: string | null;
}

export interface StandingsResponse {
	standings: StandingRow[];
	season: number;
	playoffTeams: number;
	regularSeasonWeeks: number;
	isSeasonComplete?: boolean;
	dataUpdatedAt?: string | null;
}

/** One team in /api/playoff-odds: the engine's row plus what the API adds. */
export interface OddsTeam extends EngineTeamOdds {
	teamName: string;
	managerName: string | null;
	projectedPointsRank?: number;
	previousOdds: number | null;
}

export interface OddsResponse {
	season: number;
	available: boolean;
	reason?: string;
	throughWeek: number;
	regularSeasonWeeks: number;
	phase: 'regular' | 'postseason';
	sims: number;
	playoffTeams: number;
	byes: number;
	leagueAverage: number;
	teams: OddsTeam[];
}

/* ---------- S4.1 The page and its two states ---------- */

export type SeasonState = 'live' | 'final';

/**
 * After the final once the standings API says the season has a champion;
 * live otherwise, including a new season before its first game. When next
 * season's league loads, the API's season moves on and the page is live again.
 */
export function seasonState(
	standings: Pick<StandingsResponse, 'isSeasonComplete'> | null | undefined
): SeasonState {
	return standings?.isSeasonComplete === true ? 'final' : 'live';
}

/** The overview fields the season status reads. */
export interface OverviewStatus {
	currentWeek?: number;
	playoffStatus?: string;
	tradeDeadlineStatus?: string;
	format?: { playoffStartWeek?: number } | null;
}

/**
 * The status at the end of the live kicker, from the season's real format:
 * the playoffs once the whole regular season is played, the trade deadline
 * only when Yahoo supplied it.
 */
export function seasonStatus(overview: OverviewStatus | null | undefined): string {
	if (overview?.playoffStatus === 'Playoffs') return 'Playoffs are heating up';
	if (overview?.tradeDeadlineStatus === 'Passed') return 'Trade deadline passed';
	return 'Regular season in progress';
}

/** The kicker over the title, desktop and phone. */
export function pageKicker(
	state: SeasonState,
	season: number,
	week: number,
	weeks: number,
	status: string
): { desktop: string; phone: string } {
	if (state === 'final') {
		return { desktop: `${season} Season · Season complete`, phone: `${season} · Season complete` };
	}
	return {
		desktop: `${season} Season · Week ${week} of ${weeks} · ${status}`,
		phone: `${season} · Week ${week} of ${weeks}`
	};
}

/* ---------- S4.2 The stat strip ---------- */

export interface StatTileData {
	label: string;
	value: string;
	sub: string;
	/** The phone strip's one line under the value: the label with the sub folded in. */
	phone: string;
}

/**
 * Names in a sentence: "Craig", "Craig and Nick", "Craig, Nick and Omar".
 * More than three become "{n} managers".
 */
export function nameList(names: string[]): string {
	if (names.length > 3) return `${names.length} managers`;
	if (names.length <= 2) return names.join(' and ');
	return `${names.slice(0, -1).join(', ')} and ${names[names.length - 1]}`;
}

const recordOf = (r: { wins: number; losses: number; ties: number }) =>
	formatRecord(r.wins, r.losses, r.ties);

/** The same wins, losses and ties. */
export const sameRecord = (
	a: { wins: number; losses: number; ties: number },
	b: { wins: number; losses: number; ties: number }
) => a.wins === b.wins && a.losses === b.losses && a.ties === b.ties;

export interface StatTilesInput {
	currentWeek: number;
	regularSeasonWeeks: number;
	playoffStartWeek: number;
	phase: 'regular' | 'postseason';
	/** The odds' last simulated week. The history tile needs week 1 or later. */
	throughWeek: number;
	standings: StandingRow[];
	/** The history tile, already worked out, or null when there's none. */
	history: HistoryTile | null;
}

function currentWeekTile(input: StatTilesInput): StatTileData {
	if (input.phase === 'postseason') {
		return {
			label: 'Current week',
			value: 'Playoffs',
			sub: `Week ${input.currentWeek}`,
			phone: `Current week · Week ${input.currentWeek}`
		};
	}
	return {
		label: 'Current week',
		value: `${input.currentWeek} of ${input.regularSeasonWeeks}`,
		sub: `Playoffs start week ${input.playoffStartWeek}`,
		phone: 'Current week'
	};
}

function mostPointsTile(rows: StandingRow[]): StatTileData | null {
	if (rows.length === 0) return null;
	const top = Math.max(...rows.map((r) => r.pointsFor));
	// Compared at one decimal, as printed, so a tie on the page is a tie here.
	const leaders = rows.filter((r) => r.pointsFor.toFixed(1) === top.toFixed(1));
	const names = nameList(leaders.map((r) => r.managerName));
	return {
		label: 'Most points for',
		value: top.toFixed(1),
		sub: names,
		phone: `Most points · ${names}`
	};
}

function recordTile(rows: StandingRow[]): StatTileData | null {
	if (rows.length === 0) return null;
	const unbeaten = rows.filter((r) => r.losses === 0 && r.ties === 0 && r.wins > 0);
	if (unbeaten.length > 0) {
		// The best unbeaten record and who has it. Teams are level on games in
		// a fully loaded week; if they aren't, a 3-0 isn't printed as 4-0.
		const top = [...unbeaten].sort(compareSeeding)[0];
		const names = nameList(unbeaten.filter((r) => sameRecord(r, top)).map((r) => r.managerName));
		const label = unbeaten.length === 1 ? 'Only unbeaten' : 'Unbeaten';
		return { label, value: recordOf(top), sub: names, phone: `Unbeaten · ${names}` };
	}
	const best = [...rows].sort(compareSeeding)[0];
	const sharing = rows.filter((r) => sameRecord(r, best));
	const names = nameList(sharing.map((r) => r.managerName));
	return {
		label: 'Best record',
		value: recordOf(best),
		sub: names,
		phone: `Best record · ${names}`
	};
}

/**
 * The four tiles in the board's order: current week, most points for, the
 * unbeaten (or best record) tile, and the history tile. The history tile shows
 * in the regular season from week 1 on; before that and in the postseason the
 * strip has three. A season with no games yet has only the current week.
 */
export function statTiles(input: StatTilesInput): StatTileData[] {
	const tiles: (StatTileData | null)[] = [
		currentWeekTile(input),
		mostPointsTile(input.standings),
		recordTile(input.standings)
	];
	if (input.phase === 'regular' && input.throughWeek >= 1 && input.history) {
		tiles.push({ ...input.history, phone: input.history.label });
	}
	return tiles.filter((t): t is StatTileData => t !== null);
}

/** The history tile for the standings leader's record, through start-history.ts. */
export { historyTile };

/* ---------- S4.3 Standings ---------- */

/** A type, not an interface, so it fits DataTable's Record<string, unknown> rows. */
export type StandingsTableRow = {
	rank: number;
	managerName: string;
	record: string;
	pointsFor: string;
	pointsAgainst: string;
	diff: string;
	diffNegative: boolean;
	/** Inside the playoff field. */
	aboveLine: boolean;
	/** Playoff odds 0 to 100 for the bar, and the printed number; null with no odds. */
	oddsPercent: number | null;
	oddsText: string | null;
	/** Whole points of playoff odds gained since last week; null in week 1. */
	weekChange: number | null;
	/** "▲ 6", "▼ 3", "Same", or "" in week 1. */
	weekChangeText: string;
};

/**
 * PF minus PA at one decimal: "+43.6", "-3.5", "0.0". The board's rule: the
 * sign from the difference, then its size to one decimal.
 */
export function formatDiff(
	pointsFor: number,
	pointsAgainst: number
): {
	text: string;
	negative: boolean;
} {
	const diff = pointsFor - pointsAgainst;
	const sign = diff > 0 ? '+' : diff < 0 ? '-' : '';
	return { text: `${sign}${Math.abs(diff).toFixed(1)}`, negative: diff < 0 };
}

/** "▲ 6", "▼ 3" or "Same" from a signed change. */
export function changeText(change: number): string {
	if (change > 0) return `▲ ${change}`;
	if (change < 0) return `▼ ${-change}`;
	return 'Same';
}

/**
 * The standings table: /api/standings in its order, with each team's playoff
 * odds and last week's odds joined on by manager name.
 */
export function standingsRows(
	standings: StandingRow[],
	odds: OddsTeam[] | null | undefined,
	playoffTeams: number
): StandingsTableRow[] {
	const byManager = new Map((odds ?? []).map((t) => [t.managerName, t]));
	return standings.map((row) => {
		const team = byManager.get(row.managerName);
		const diff = formatDiff(row.pointsFor, row.pointsAgainst);
		const locked = team ? team.clinched || team.eliminated : false;
		const weekChange =
			team && team.previousOdds != null
				? Math.round((team.playoffOdds - team.previousOdds) * 100)
				: null;
		return {
			rank: row.rank,
			managerName: row.managerName,
			record: recordOf(row),
			pointsFor: row.pointsFor.toFixed(1),
			pointsAgainst: row.pointsAgainst.toFixed(1),
			diff: diff.text,
			diffNegative: diff.negative,
			aboveLine: row.rank <= playoffTeams,
			oddsPercent: team ? team.playoffOdds * 100 : null,
			oddsText: team ? formatOdds(team.playoffOdds, locked) : null,
			weekChange,
			weekChangeText: weekChange === null ? '' : changeText(weekChange)
		};
	});
}

/** The divider after the playoff field: "Playoff line · top 6, 2 byes". */
export function playoffLineLabel(playoffTeams: number, byes: number | null | undefined): string {
	if (!byes) return `Playoff line · top ${playoffTeams}`;
	return `Playoff line · top ${playoffTeams}, ${byes === 1 ? '1 bye' : `${byes} byes`}`;
}

/**
 * When the season's data last changed: "Updated less than an hour ago",
 * "Updated 1 hour ago", "Updated 5 hours ago", "Updated 1 day ago",
 * "Updated 3 days ago".
 */
export function updatedAgo(iso: string, now: Date): string | null {
	const then = new Date(iso).getTime();
	if (!Number.isFinite(then)) return null;
	const hours = Math.floor(Math.max(0, now.getTime() - then) / 3_600_000);
	if (hours < 1) return 'Updated less than an hour ago';
	if (hours < 24) return hours === 1 ? 'Updated 1 hour ago' : `Updated ${hours} hours ago`;
	const days = Math.floor(hours / 24);
	return days === 1 ? 'Updated 1 day ago' : `Updated ${days} days ago`;
}

/** A whole number with a thousands separator: "10,000". */
export function thousands(n: number): string {
	return Math.round(n).toLocaleString('en-US');
}

/* ---------- S4.4 Playoff Odds ---------- */

/**
 * The odds bar's fill: field at 75% and up, sage from 40%, light sage from
 * 15%, line-strong below. `percent` is 0 to 100.
 */
export function oddsTone(percent: number): string {
	if (percent >= 75) return 'var(--color-field)';
	if (percent >= 40) return 'var(--color-sage)';
	if (percent >= 15) return 'var(--color-sage-tint)';
	return 'var(--color-line-strong)';
}

/** All-play records can carry a half from a tied week; print it rather than round. */
const halves = (x: number) => (Number.isInteger(x) ? String(x) : x.toFixed(1));

/** "+1.1" or "-0.2", one decimal. */
const signed = (x: number) => `${x >= 0 ? '+' : '-'}${Math.abs(x).toFixed(1)}`;

/** The open row's details, in the board's order. */
export function oddsDetail(
	team: OddsTeam,
	regularSeasonWeeks: number
): { term: string; value: string }[] {
	const rows: { term: string; value: string }[] = [
		{
			term: 'Projected score',
			value: `${team.projectedPoints.toFixed(1)} a week${team.projectedPointsRank ? `, #${team.projectedPointsRank}` : ''}`
		},
		{ term: 'All-play record', value: `${halves(team.allPlayWins)}-${halves(team.allPlayLosses)}` },
		{ term: 'Luck', value: `${signed(team.luck)} wins` }
	];
	if (team.remainingOpponentPoints != null) {
		const games = team.remainingGames === 1 ? '1 game' : `${team.remainingGames} games`;
		rows.push({
			term: 'Schedule left',
			value: `${games}, opponents avg ${team.remainingOpponentPoints.toFixed(1)}`
		});
	}
	rows.push(
		{
			term: 'Projected finish',
			value: `${team.projectedWins.toFixed(1)}-${(regularSeasonWeeks - team.projectedWins).toFixed(1)}`
		},
		{
			term: 'Bye · #1 seed · Title',
			value: [team.byeOdds, team.topSeedOdds, team.titleOdds]
				.map((p) => formatOdds(p, team.eliminated))
				.join(' · ')
		}
	);
	return rows;
}

/** The two paragraphs under "How these odds work", with the response's numbers. */
export function oddsMethod(sims: number, playoffTeams: number, byes: number): [string, string] {
	const first = `Each team gets a projected score, pulled toward the league average early in the year. The rest of the season is played ${thousands(sims)} times with that score drifting week to week, and seeds go by wins, then points for.`;
	const format =
		playoffTeams === 6 && byes === 2
			? 'Six make it and the top two get byes: 3 plays 6, 4 plays 5.'
			: byes === 1
				? `The top ${playoffTeams} make it and the top seed gets a bye.`
				: byes > 0
					? `The top ${playoffTeams} make it and the top ${byes} get byes.`
					: `The top ${playoffTeams} make it.`;
	return [
		first,
		`${format} Back-tested on 2008 to 2025, teams given 85 to 95% made it 90% of the time.`
	];
}

/* ---------- S4.5 Recent moves ---------- */

export interface Transaction {
	type: string;
	playerName: string;
	managerName: string | null;
	faabBid: number | null;
}

export interface MoveLine {
	verb: string;
	player: string;
	tag: string;
	/** Tag's existing tones in the board's colors: field-tint, pennant-tint, wash. */
	tone: TagVariant;
}

/**
 * One Recent moves row: the verb, the player (with the FAAB bid when there
 * was one, $0 included) and the tag.
 */
export function moveLine(tx: Transaction): MoveLine {
	const bid = tx.faabBid == null ? '' : ` for $${Math.round(tx.faabBid)} FAAB`;
	const player = `${tx.playerName}${bid}`;
	switch (tx.type) {
		case 'add':
			return { verb: 'picked up', player, tag: 'Add', tone: 'bye' };
		case 'drop':
			return { verb: 'dropped', player, tag: 'Drop', tone: 'champion' };
		case 'trade':
			return { verb: 'traded for', player, tag: 'Trade', tone: 'eliminated' };
		default:
			return { verb: 'swapped in', player, tag: 'Add/drop', tone: 'eliminated' };
	}
}

/* ---------- S4.8 After the final: the champion ---------- */

export interface SeededTeam {
	managerName: string;
	seed: number;
	wins: number;
	losses: number;
	ties: number;
}

/**
 * Each team's seed: its place in the Record order (wins, ties as half, then
 * points for), the order the Final standings' Record view shows.
 */
export function recordOrder<T extends StandingRow>(rows: T[]): T[] {
	return [...rows].sort(compareSeeding);
}

/** "an" before a number said with a vowel sound (8, 11, 18, the 80s), "a" otherwise. */
export function article(n: number): 'a' | 'an' {
	return n === 8 || n === 11 || n === 18 || (n >= 80 && n < 90) ? 'an' : 'a';
}

/**
 * The line under the champion's name: "Won it from the 6 seed after a 7-7
 * regular season. Omar, the 11-3 top seed, finished second."
 */
export function championLine(champion: SeededTeam, runnerUp: SeededTeam | null): string {
	const season = `${article(champion.wins)} ${recordOf(champion)} regular season`;
	const won =
		champion.seed === 1
			? `Won it as the top seed after ${season}.`
			: `Won it from the ${champion.seed} seed after ${season}.`;
	if (!runnerUp) return won;
	const second =
		runnerUp.seed === 1
			? `${runnerUp.managerName}, the ${recordOf(runnerUp)} top seed, finished second.`
			: `${runnerUp.managerName}, the ${runnerUp.seed} seed at ${recordOf(runnerUp)}, finished second.`;
	return `${won} ${second}`;
}

/* ---------- S4.9 Final standings ---------- */

export type FinishGroup =
	| 'Championship'
	| 'Semifinalists'
	| 'Quarterfinalists'
	| 'Consolation bracket';

/** The Playoff finish view's group for a tier. */
export function finishGroup(tier: string | null | undefined): FinishGroup {
	if (tier === 'Champion' || tier === 'Runner-up') return 'Championship';
	if (tier === 'Semifinalist') return 'Semifinalists';
	if (tier === 'Quarterfinalist') return 'Quarterfinalists';
	return 'Consolation bracket';
}

/** Rows in the API's final order, grouped under the board's labels. Empty groups drop. */
export function finishGroups<T extends StandingRow>(rows: T[]): { name: FinishGroup; rows: T[] }[] {
	const order: FinishGroup[] = [
		'Championship',
		'Semifinalists',
		'Quarterfinalists',
		'Consolation bracket'
	];
	return order
		.map((name) => ({ name, rows: rows.filter((r) => finishGroup(r.playoffTier) === name) }))
		.filter((g) => g.rows.length > 0);
}

/** "1,805.7": a thousands separator and one decimal. */
export function formatPoints(n: number): string {
	return n.toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
}
