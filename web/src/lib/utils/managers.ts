/**
 * Pure rules behind the Managers page and the manager profiles (WP6), kept
 * out of the components so tests hold what the pages say (build spec G9,
 * S6.10). Nothing here knows who Trevor is.
 */
import type { TagVariant } from '$lib/components/ui';
import { hofTier } from './hall-of-fame';
import { spellNumber } from './power-rankings';

// ---------------------------------------------------------------------------
// Shapes read from /api/managers/performance
// ---------------------------------------------------------------------------

/** The parts of a `rankings` row the helpers read. */
export interface RankingLike {
	managerName: string | null;
	lastSeason: number | string | null;
	totalChampionships: number | null;
	winPercentage: number | null;
}

/** One team-season from `data.seasons`. Counts can arrive as strings. */
export interface SeasonLike {
	seasonYear: number | string;
	wins: number | string;
	losses: number | string;
	ties: number | string;
	pointsFor?: number | string | null;
	isChampion?: boolean | null;
	madePlayoffs?: boolean | null;
	inProgress?: boolean | null;
	bracketSet?: boolean | null;
	playoffOdds?: number | null;
}

/** One row of `data.head_to_head`, oriented to the profile's manager. */
export interface H2HApiRow {
	opponent: string;
	totalMatchups: number | null;
	wins: number | null;
	losses: number | null;
	ties?: number | null;
	avgPoints?: number | null;
	opponentAvgPoints?: number | null;
	currentStreak?: number | null;
	playoffWins?: number | null;
	playoffLosses?: number | null;
	opponentLastSeason?: number | null;
}

/** One entry of `data.titleGames`. */
export interface TitleGame {
	seasonYear: number;
	points: number | null;
	opponentPoints: number | null;
	opponent: string | null;
	won: boolean;
}

const n = (value: unknown): number => {
	const v = typeof value === 'number' ? value : Number(value);
	return Number.isFinite(v) ? v : 0;
};

// ---------------------------------------------------------------------------
// Words and numbers
// ---------------------------------------------------------------------------

/** "League Legend" to "League legend": the first letter kept, the rest lowercase. */
export function sentenceCase(text: string): string {
	if (!text) return text;
	return text.charAt(0).toUpperCase() + text.slice(1).toLowerCase();
}

/** 1st, 2nd, 3rd, 4th, 11th, 12th, 13th, 21st. */
export function ordinal(rank: number): string {
	const mod100 = rank % 100;
	if (mod100 >= 11 && mod100 <= 13) return `${rank}th`;
	switch (rank % 10) {
		case 1:
			return `${rank}st`;
		case 2:
			return `${rank}nd`;
		case 3:
			return `${rank}rd`;
		default:
			return `${rank}th`;
	}
}

/** "best in the league" at rank 1, else "4th in the league". */
export function rankPhrase(rank: number): string {
	return rank === 1 ? 'best in the league' : `${ordinal(rank)} in the league`;
}

/** A win rate to three places without the leading zero: ".625", "1.000". */
export function formatPct3(value: number): string {
	return value.toFixed(3).replace(/^0(?=\.)/, '');
}

/** A rate as a percent to one place: 0.6667 to "66.7%". */
export function formatPercent1(value: number): string {
	return `${(value * 100).toFixed(1)}%`;
}

/**
 * Playoff odds to one place (Luke, 2026-11-10: a statistic isn't rounded to a
 * whole number). The odds are a share of 10,000 simulated seasons, so 100%
 * and 0% print only when every season agrees; 99.96% reads "99.9%", never
 * "100.0%", and 0.04% reads "0.1%".
 */
export function formatOdds(odds: number): string {
	if (odds >= 1) return '100%';
	if (odds <= 0) return '0%';
	const pct = Math.min(99.9, Math.max(0.1, Math.round(odds * 1000) / 10));
	return `${pct.toFixed(1)}%`;
}

/** A whole number with thousands: 42974. */
export function formatWhole(value: number): string {
	return Math.round(value).toLocaleString('en-US');
}

/** One place with thousands: "1,929.0". */
export function formatOnePlace(value: number): string {
	return value.toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
}

/** Signed, one place, with thousands: "+1,893.7", "-12.0", "0.0". */
export function formatSigned(value: number): string {
	const r = Math.round(value * 10) / 10;
	const text = formatOnePlace(Math.abs(r));
	return r > 0 ? `+${text}` : r < 0 ? `-${text}` : text;
}

/** "13-2", with ties as a third number only when above 0: "9-6-1". */
export function shortRecord(wins: number, losses: number, ties = 0): string {
	return ties > 0 ? `${wins}-${losses}-${ties}` : `${wins}-${losses}`;
}

/** "187-145-1": every number, as the career record is drawn. */
export function fullRecord(wins: number, losses: number, ties: number): string {
	return `${wins}-${losses}-${ties}`;
}

/** "1 title", "3 titles": the TitleFlags label. */
export function titleCount(count: number): string {
	return `${count} ${count === 1 ? 'title' : 'titles'}`;
}

/** "A", "A and B", "A, B and C". */
function joinWithAnd(items: string[]): string {
	if (items.length <= 1) return items.join('');
	return `${items.slice(0, -1).join(', ')} and ${items[items.length - 1]}`;
}

// ---------------------------------------------------------------------------
// The Managers page (S6.1 to S6.3)
// ---------------------------------------------------------------------------

/**
 * The kicker's counts. All-time is every named manager in the rankings;
 * active is the ones whose last season is the latest anyone has.
 */
export function rosterCounts(rankings: RankingLike[]): { active: number; allTime: number } {
	const named = rankings.filter((r) => r.managerName);
	const seasons = named.map((r) => Number(r.lastSeason)).filter(Number.isFinite);
	if (seasons.length === 0) return { active: 0, allTime: named.length };
	const latest = Math.max(...seasons);
	return {
		active: named.filter((r) => Number(r.lastSeason) === latest).length,
		allTime: named.length
	};
}

/** A rail entry: the manager, and whether they've left the league. */
export interface RailEntry<R> {
	manager: R;
	/** Their last season is before the league's latest: a league alum. */
	alum: boolean;
}

/**
 * The rail: active managers by titles, then career win rate, both
 * descending, then former managers in the same order, kept as an honor and
 * marked as alumni (Luke, 2026-11-10; the board draws only the active ten).
 */
export function railOrder<R extends RankingLike>(rankings: R[]): RailEntry<R>[] {
	const named = rankings.filter((r) => r.managerName);
	const seasons = named.map((r) => Number(r.lastSeason)).filter(Number.isFinite);
	if (seasons.length === 0) return [];
	const latest = Math.max(...seasons);
	const byRecord = (a: R, b: R) =>
		n(b.totalChampionships) - n(a.totalChampionships) || n(b.winPercentage) - n(a.winPercentage);
	const active = named.filter((r) => Number(r.lastSeason) === latest).sort(byRecord);
	const alumni = named.filter((r) => Number(r.lastSeason) !== latest).sort(byRecord);
	return [
		...active.map((manager) => ({ manager, alum: false })),
		...alumni.map((manager) => ({ manager, alum: true }))
	];
}

export interface RankItem {
	label: string;
	rank: number | null | undefined;
}

/**
 * Rank tiles sorted by rank, lowest number first. Ties keep the order they
 * were given in, and a missing rank goes last.
 */
export function rankMedals<T extends RankItem>(items: T[]): T[] {
	return items
		.map((item, index) => ({ item, index }))
		.sort((a, b) => {
			const ra = a.item.rank ?? Infinity;
			const rb = b.item.rank ?? Infinity;
			return ra - rb || a.index - b.index;
		})
		.map(({ item }) => item);
}

export interface Medal {
	/** The medal circle's fill, a token name. */
	medal: string;
	/** The number's color on the medal, a token name. */
	text: string;
	/** The tile's fill, a token name. */
	tile: string;
}

/**
 * The medal and tile colors for a rank: 1 pennant on pennant-tint, 2 field on
 * field-tint, 3 sage on field-wash. The boards draw nothing below 3, so 4 and
 * on are a line-strong medal with ink text on a ground tile.
 */
export function rankMedal(rank: number): Medal {
	if (rank === 1) return { medal: 'pennant', text: 'white', tile: 'pennant-tint' };
	if (rank === 2) return { medal: 'field', text: 'white', tile: 'field-tint' };
	if (rank === 3) return { medal: 'sage', text: 'white', tile: 'field-wash' };
	return { medal: 'line-strong', text: 'ink', tile: 'ground' };
}

/**
 * The Achievements chips' tag tone: Champion pennant, Consistent winner
 * field, High scorer field-tint, and the tenure chips (Long timer,
 * Established, Newcomer) wash.
 */
export function badgeTone(name: string): TagVariant {
	switch (name.toLowerCase()) {
		case 'champion':
			return 'champion-strong';
		case 'consistent winner':
			return 'clinched';
		case 'high scorer':
			return 'bye';
		default:
			return 'eliminated';
	}
}

/** The phone hero's line: "147-87-1 · .625 · 3 titles", ending at the win rate with no titles. */
export function heroLine(record: string, winPct: number, titles: number): string {
	const head = `${record} · ${formatPct3(winPct)}`;
	return titles > 0 ? `${head} · ${titleCount(titles)}` : head;
}

/** A season's draft score, from /api/draft's draftGrades.bySeason. */
export interface DraftSeasonScore {
	managerName: string;
	seasonYear: number;
	score: number;
	inProgress: boolean;
}

export interface DraftCareerGrade {
	managerName: string;
	grade: string;
}

/**
 * Career range's draft rows (D7): the years of the manager's highest and
 * lowest season draft scores among finished seasons, and the career letter.
 * Ties go to the more recent season.
 */
export function draftRange(
	bySeason: DraftSeasonScore[],
	byManager: DraftCareerGrade[],
	managerName: string
): { bestYear: number | null; worstYear: number | null; grade: string | null } {
	const mine = bySeason
		.filter((s) => s.managerName === managerName && !s.inProgress)
		.sort((a, b) => b.seasonYear - a.seasonYear);
	let best: DraftSeasonScore | null = null;
	let worst: DraftSeasonScore | null = null;
	for (const s of mine) {
		if (!best || s.score > best.score) best = s;
		if (!worst || s.score < worst.score) worst = s;
	}
	return {
		bestYear: best?.seasonYear ?? null,
		worstYear: worst?.seasonYear ?? null,
		grade: byManager.find((m) => m.managerName === managerName)?.grade ?? null
	};
}

// ---------------------------------------------------------------------------
// Profile: header and tabs (S6.4)
// ---------------------------------------------------------------------------

export type ProfileTab = 'overview' | 'seasons' | 'head-to-head' | 'achievements';

export const PROFILE_TABS: { id: ProfileTab; label: string }[] = [
	{ id: 'overview', label: 'Overview' },
	{ id: 'seasons', label: 'Season history' },
	{ id: 'head-to-head', label: 'Head to head' },
	{ id: 'achievements', label: 'Achievements' }
];

/** The tab `?tab=` names, or overview when it's missing or unknown. */
export function profileTab(url: URL | URLSearchParams | string): ProfileTab {
	const params =
		url instanceof URLSearchParams
			? url
			: new URL(typeof url === 'string' ? url : url.href, 'http://x').searchParams;
	const tab = params.get('tab');
	return PROFILE_TABS.some((t) => t.id === tab) ? (tab as ProfileTab) : 'overview';
}

/** A profile link: overview carries no `tab`. */
export function profileHref(name: string, tab: ProfileTab = 'overview'): string {
	const base = `/managers/${encodeURIComponent(name)}`;
	return tab === 'overview' ? base : `${base}?tab=${tab}`;
}

/** The header kicker, from the manager's Hall of Fame row, else their first season. */
export function profileKicker(
	hallOfFameRank: number | null | undefined,
	firstSeason: number | string | null | undefined
): string {
	if (hallOfFameRank != null && Number.isFinite(hallOfFameRank)) {
		return `${hofTier(hallOfFameRank)} · No. ${hallOfFameRank} all-time`;
	}
	return `Manager since ${firstSeason}`;
}

/** "2005 to 2026 · 22 seasons · never missed one". */
export function tenureLine(firstSeason: number, lastSeason: number, totalSeasons: number): string {
	const seasons = `${totalSeasons} ${totalSeasons === 1 ? 'season' : 'seasons'}`;
	const line = `${firstSeason} to ${lastSeason} · ${seasons}`;
	return totalSeasons === lastSeason - firstSeason + 1 ? `${line} · never missed one` : line;
}

// ---------------------------------------------------------------------------
// Profile Overview (S6.5)
// ---------------------------------------------------------------------------

export type BarTone = 'title' | 'playoffs' | 'missed' | 'progress';

export interface WinsBar {
	year: number;
	/** "'05". */
	label: string;
	wins: number;
	tone: BarTone;
	/** Percent of the chart's height. */
	height: number;
}

function wonTitle(season: SeasonLike, championshipSeasons: number[]): boolean {
	return season.isChampion ?? championshipSeasons.includes(Number(season.seasonYear));
}

/**
 * One bar per season year, oldest first. A year with two teams sums their
 * wins. Height is wins over the most games the manager played in any year.
 * A title is pennant, a playoff season field, a missed one line-strong, and a
 * season still being played the dashed outline.
 */
export function winsBars(seasons: SeasonLike[], championshipSeasons: number[] = []): WinsBar[] {
	const years = new Map<
		number,
		{ wins: number; games: number; title: boolean; playoffs: boolean; live: boolean }
	>();
	for (const s of seasons) {
		const year = Number(s.seasonYear);
		const y = years.get(year) ?? { wins: 0, games: 0, title: false, playoffs: false, live: false };
		y.wins += n(s.wins);
		y.games += n(s.wins) + n(s.losses) + n(s.ties);
		y.title ||= wonTitle(s, championshipSeasons);
		y.playoffs ||= Boolean(s.madePlayoffs);
		y.live ||= Boolean(s.inProgress);
		years.set(year, y);
	}
	const most = Math.max(0, ...[...years.values()].map((y) => y.games));
	return [...years.entries()]
		.sort(([a], [b]) => a - b)
		.map(([year, y]) => ({
			year,
			label: `'${String(year).slice(-2)}`,
			wins: y.wins,
			tone: y.title ? 'title' : y.live ? 'progress' : y.playoffs ? 'playoffs' : 'missed',
			height: most > 0 ? Math.round((y.wins / most) * 100) : 0
		}));
}

/** The chart's aria-label: the years it covers and the title years. */
export function winsChartLabel(bars: WinsBar[]): string {
	if (bars.length === 0) return 'Wins per season.';
	const first = bars[0].year;
	const last = bars[bars.length - 1].year;
	const span = first === last ? `${first}` : `${first} to ${last}`;
	const titles = bars.filter((b) => b.tone === 'title').map((b) => String(b.year));
	const tail =
		titles.length === 0
			? 'No titles yet.'
			: `${titles.length === 1 ? 'Title' : 'Titles'} in ${joinWithAnd(titles)}.`;
	return `Wins per season, ${span}. ${tail}`;
}

export interface SeasonPick {
	seasonYear: number;
	wins: number;
	losses: number;
	ties: number;
	champion: boolean;
	madePlayoffs: boolean;
}

/**
 * The best and worst finished seasons, by wins over games played (the mart's
 * rule for best_season_record). Ties go to more points for on the best, fewer
 * on the worst. A season still being played is never either.
 */
export function bestWorstSeason(
	seasons: SeasonLike[],
	championshipSeasons: number[] = []
): { best: SeasonPick | null; worst: SeasonPick | null } {
	const finished = seasons
		.filter((s) => !s.inProgress)
		.map((s) => {
			const wins = n(s.wins);
			const losses = n(s.losses);
			const ties = n(s.ties);
			const games = wins + losses + ties;
			return {
				pick: {
					seasonYear: Number(s.seasonYear),
					wins,
					losses,
					ties,
					champion: wonTitle(s, championshipSeasons),
					madePlayoffs: Boolean(s.madePlayoffs)
				},
				rate: games > 0 ? wins / games : 0,
				pf: n(s.pointsFor)
			};
		});
	if (finished.length === 0) return { best: null, worst: null };
	let best = finished[0];
	let worst = finished[0];
	for (const s of finished.slice(1)) {
		if (s.rate > best.rate || (s.rate === best.rate && s.pf > best.pf)) best = s;
		if (s.rate < worst.rate || (s.rate === worst.rate && s.pf < worst.pf)) worst = s;
	}
	return { best: best.pick, worst: worst.pick };
}

/** "13-2 · 2017". */
export function seasonPickText(pick: SeasonPick): string {
	return `${shortRecord(pick.wins, pick.losses, pick.ties)} · ${pick.seasonYear}`;
}

// ---------------------------------------------------------------------------
// Profile Season history (S6.6)
// ---------------------------------------------------------------------------

export type OutcomeTag = 'champion' | 'playoffs' | 'missed' | 'in-progress';

/**
 * A season's outcome tag. A title is Champion. A season still being played
 * reads its playoff odds until the bracket is set, then whether the team is in
 * it. A finished season is Playoffs or Missed playoffs.
 */
export function seasonOutcome(
	season: SeasonLike,
	championshipSeasons: number[] = []
): { label: string; tag: OutcomeTag; champion: boolean } {
	if (wonTitle(season, championshipSeasons)) {
		return { label: 'Champion', tag: 'champion', champion: true };
	}
	if (season.inProgress) {
		if (season.bracketSet) {
			return season.madePlayoffs
				? { label: 'Playoffs · in progress', tag: 'playoffs', champion: false }
				: { label: 'Missed playoffs', tag: 'missed', champion: false };
		}
		return season.playoffOdds != null
			? {
					label: `In progress · ${formatOdds(season.playoffOdds)} playoff odds`,
					tag: 'in-progress',
					champion: false
				}
			: { label: 'In progress', tag: 'in-progress', champion: false };
	}
	return season.madePlayoffs
		? { label: 'Playoffs', tag: 'playoffs', champion: false }
		: { label: 'Missed playoffs', tag: 'missed', champion: false };
}

// ---------------------------------------------------------------------------
// Profile Head to head (S6.7)
// ---------------------------------------------------------------------------

export type Lean = 'up' | 'down' | 'even';

export type H2HRow = {
	opponent: string;
	games: number;
	wins: number;
	losses: number;
	ties: number;
	record: string;
	/** (wins + ties / 2) / games. */
	pct: number;
	pctText: string;
	pctTone: Lean;
	/** The win % bar on a centered track: where the fill starts and how wide it is, in percent. */
	bar: { left: number; width: number; tone: 'up' | 'down' };
	avgPoints: number | null;
	opponentAvgPoints: number | null;
	/** "2-1", or null with no bracket games (rendered "None"). */
	playoffs: string | null;
	streak: { text: string; tone: Lean };
	currentStreak: number;
	opponentLastSeason: number | null;
};

/** The share as printed, so a .4996 that reads ".500" is even, not down. */
const printed = (pct: number) => Math.round(pct * 1000) / 1000;

/**
 * Every opponent's row, best win % first, then most games. The bar leans
 * right of center at .500 or better and left below, and its width is how far
 * from .500 the share is, reaching the edge at .250 either side.
 */
export function h2hRows(rows: H2HApiRow[]): H2HRow[] {
	return rows
		.map((r) => {
			const wins = n(r.wins);
			const losses = n(r.losses);
			const ties = n(r.ties);
			const games = r.totalMatchups != null ? n(r.totalMatchups) : wins + losses + ties;
			const pct = games > 0 ? (wins + ties / 2) / games : 0;
			const shown = printed(pct);
			const dev = Math.min(Math.abs(pct - 0.5) / 0.25, 1) * 50;
			const up = shown >= 0.5;
			const bracket = n(r.playoffWins) + n(r.playoffLosses);
			const streak = n(r.currentStreak);
			return {
				opponent: r.opponent,
				games,
				wins,
				losses,
				ties,
				record: shortRecord(wins, losses, ties),
				pct,
				pctText: formatPct3(pct),
				pctTone: (shown > 0.5 ? 'up' : shown < 0.5 ? 'down' : 'even') as Lean,
				bar: { left: up ? 50 : 50 - dev, width: dev, tone: (up ? 'up' : 'down') as 'up' | 'down' },
				avgPoints: r.avgPoints ?? null,
				opponentAvgPoints: r.opponentAvgPoints ?? null,
				playoffs: bracket > 0 ? `${n(r.playoffWins)}-${n(r.playoffLosses)}` : null,
				streak:
					streak > 0
						? { text: `W${streak}`, tone: 'up' as Lean }
						: streak < 0
							? { text: `L${-streak}`, tone: 'down' as Lean }
							: { text: 'Tie', tone: 'even' as Lean },
				currentStreak: streak,
				opponentLastSeason: r.opponentLastSeason ?? null
			};
		})
		.sort((a, b) => b.pct - a.pct || b.games - a.games);
}

export interface H2HTile {
	label: string;
	/** The opponent the tile is about; null on Title games and on an empty tile. */
	opponent: string | null;
	/** The big number, or null for EmptyValue "None". */
	value: string | null;
	sub: string;
	tone: 'field' | 'pennant' | 'ink' | 'pennant-dark';
}

/** "Seven titles in 10 trips", "One title in 3 trips", "No titles in 2 trips", "in 1 trip". */
export function titleTripsLine(wins: number, trips: number): string {
	const first = wins === 0 ? 'No' : wins < 10 ? sentenceCase(spellNumber(wins)) : String(wins);
	return `${first} ${wins === 1 ? 'title' : 'titles'} in ${trips} ${trips === 1 ? 'trip' : 'trips'}`;
}

/**
 * The four tiles above the table. Picks come from opponents with at least 10
 * games (every opponent when none has 10), no one is picked twice, and a tie
 * goes to more games, then name order.
 */
export function h2hTiles(rows: H2HRow[], titleGames: TitleGame[]): H2HTile[] {
	const tenUp = rows.filter((r) => r.games >= 10);
	const pool = tenUp.length > 0 ? tenUp : rows;
	const picked = new Set<string>();

	const pick = (score: (r: H2HRow) => number): H2HRow | null => {
		const candidates = pool.filter((r) => !picked.has(r.opponent));
		if (candidates.length === 0) return null;
		const winner = [...candidates].sort(
			(a, b) => score(a) - score(b) || b.games - a.games || a.opponent.localeCompare(b.opponent)
		)[0];
		picked.add(winner.opponent);
		return winner;
	};

	const empty = (label: string, tone: H2HTile['tone']): H2HTile => ({
		label,
		opponent: null,
		value: null,
		sub: 'Not enough games yet',
		tone
	});

	const best = pick((r) => -r.pct);
	const toughest = pick((r) => r.pct);
	const even = pick((r) => Math.abs(r.pct - 0.5));

	const wins = titleGames.filter((g) => g.won).length;
	const trips = titleGames.length;

	return [
		best
			? {
					label: 'Best against',
					opponent: best.opponent,
					value: best.record,
					sub: `${best.opponent}, ${best.pctText}`,
					tone: 'field'
				}
			: empty('Best against', 'field'),
		toughest
			? {
					label: 'Toughest',
					opponent: toughest.opponent,
					value: toughest.record,
					sub:
						toughest.currentStreak <= -2
							? `${toughest.opponent}, who has won the last ${-toughest.currentStreak}`
							: `${toughest.opponent}, ${toughest.pctText}`,
					tone: 'pennant'
				}
			: empty('Toughest', 'pennant'),
		even
			? {
					label: 'Dead even',
					opponent: even.opponent,
					value: even.record,
					sub: `${even.opponent}, over ${even.games} games`,
					tone: 'ink'
				}
			: empty('Dead even', 'ink'),
		trips > 0
			? {
					label: 'Title games',
					opponent: null,
					value: `${wins}-${trips - wins}`,
					sub: titleTripsLine(wins, trips),
					tone: 'pennant-dark'
				}
			: {
					label: 'Title games',
					opponent: null,
					value: null,
					sub: 'No title games yet',
					tone: 'pennant-dark'
				}
	];
}

/** The Against every manager caption, from the luck card's current season. */
export function h2hCaption(
	current: { seasonYear: number; throughWeek: number; final: boolean } | null | undefined
): string {
	if (!current) return 'Best record first';
	return current.final
		? `Best record first · through the ${current.seasonYear} season`
		: `Best record first · through week ${current.throughWeek} of ${current.seasonYear}`;
}

/** The Rivalry link: League History's head to head, both managers named. */
export function rivalryHref(manager: string, opponent: string): string {
	return `/historical?tab=head-to-head&a=${encodeURIComponent(manager)}&b=${encodeURIComponent(opponent)}`;
}

// ---------------------------------------------------------------------------
// Profile Achievements (S6.8)
// ---------------------------------------------------------------------------

/**
 * The Title games header line, in two parts so the page can bold the first:
 * "7 wins" and " in 10 trips, most in the league". "Most" needs rank 1 held
 * alone; a shared rank 1 reads "tied for the most". Null with no title games.
 */
export function titleGamesSummary(
	titleGames: TitleGame[],
	championshipRank: number | null | undefined,
	sharesTopRank: boolean
): { wins: string; rest: string } | null {
	const trips = titleGames.length;
	if (trips === 0) return null;
	const won = titleGames.filter((g) => g.won).length;
	const suffix =
		championshipRank === 1
			? sharesTopRank
				? ', tied for the most in the league'
				: ', most in the league'
			: '';
	return {
		wins: `${won} ${won === 1 ? 'win' : 'wins'}`,
		rest: ` in ${trips} ${trips === 1 ? 'trip' : 'trips'}${suffix}`
	};
}

export interface CareerMark {
	label: string;
	/** Null renders EmptyValue "None". */
	value: string | null;
	sub: string;
	tone: 'field' | 'pennant' | 'ink';
}

function seasonSub(pick: SeasonPick): string {
	if (pick.champion) return `${pick.seasonYear}, won the title`;
	if (pick.madePlayoffs) return `${pick.seasonYear}, made the playoffs`;
	return `${pick.seasonYear}, missed the playoffs`;
}

/**
 * Career marks, finished seasons only: the streaks (already finished-only
 * from the API), the best and worst seasons, playoff trips out of finished
 * seasons, and the playoff win rate with its league rank.
 */
export function careerMarks(input: {
	streaks: { longestWinStreak: number; longestLossStreak: number } | null | undefined;
	seasons: SeasonLike[];
	championshipSeasons?: number[];
	playoffWinPercentage: number | null | undefined;
	playoffRank: number | null | undefined;
}): CareerMark[] {
	const { best, worst } = bestWorstSeason(input.seasons, input.championshipSeasons ?? []);
	const finished = new Map<number, boolean>();
	for (const s of input.seasons) {
		if (s.inProgress) continue;
		const year = Number(s.seasonYear);
		finished.set(year, (finished.get(year) ?? false) || Boolean(s.madePlayoffs));
	}
	const trips = [...finished.values()].filter(Boolean).length;
	const finishedCount = finished.size;
	const streaks = input.streaks;

	return [
		{
			label: 'Longest win streak',
			value: streaks ? String(streaks.longestWinStreak) : null,
			sub: 'Games in a row',
			tone: 'field'
		},
		{
			label: 'Longest losing streak',
			value: streaks ? String(streaks.longestLossStreak) : null,
			sub: 'Games in a row',
			tone: 'pennant'
		},
		{
			label: 'Best season',
			value: best ? shortRecord(best.wins, best.losses, best.ties) : null,
			sub: best ? seasonSub(best) : 'No finished season yet',
			tone: 'ink'
		},
		{
			label: 'Worst season',
			value: worst ? shortRecord(worst.wins, worst.losses, worst.ties) : null,
			sub: worst ? seasonSub(worst) : 'No finished season yet',
			tone: 'ink'
		},
		{
			label: 'Playoff trips',
			value: String(trips),
			sub: `In ${finishedCount} finished ${finishedCount === 1 ? 'season' : 'seasons'}`,
			tone: 'ink'
		},
		{
			label: 'Playoff win rate',
			value: input.playoffWinPercentage != null ? formatPercent1(input.playoffWinPercentage) : null,
			sub: input.playoffRank != null ? sentenceCase(rankPhrase(input.playoffRank)) : 'Unranked',
			tone: 'ink'
		}
	];
}
