/**
 * The League Table (WP14): every manager's career numbers in one sortable
 * table. The columns, their groups, how each sorts, ranks and prints, and
 * the URL state all live here as pure functions, so the page only renders
 * what these return and vitest holds the rules (S14.4, S14.5).
 */
import { hofTier, managerCount, toNumber, type ColorToken } from '$lib/utils/hall-of-fame';
import { draftRange, formatPct3, formatWhole, type DraftSeasonScore } from '$lib/utils/managers';
import { careerGrade } from '$lib/draft-value';
import { longestStreaks, type GameResult } from '$lib/streaks';

// ---------------------------------------------------------------------------
// Rows: one per manager, joined from the performance, draft and HoF APIs
// ---------------------------------------------------------------------------

export interface LeagueRow {
	name: string;
	firstSeason: number | null;
	lastSeason: number | null;
	/** Their last season is before the league's latest. */
	alum: boolean;
	seasons: number | null;
	wins: number;
	losses: number;
	ties: number;
	/** (W + ½T) ÷ games (D13.1). */
	winPct: number | null;
	titles: number | null;
	titleGameWins: number | null;
	titleGameLosses: number | null;
	trips: number | null;
	/** Every playoff and title game, as the mart counts them. */
	playoffWins: number | null;
	playoffLosses: number | null;
	playoffWinPct: number | null;
	ppg: number | null;
	ppSeason: number | null;
	points: number | null;
	pointsAgainst: number | null;
	pointDiff: number | null;
	regWins: number | null;
	expectedWins: number | null;
	luck: number | null;
	faab: number | null;
	moves: number | null;
	movesPerSeason: number | null;
	draftGrade: string | null;
	draftValue: number | null;
	bestDraft: number | null;
	worstDraft: number | null;
	hofIndex: number | null;
	hofRank: number | null;
	/** The Hall of Fame list loaded, so a missing index means under 3 seasons. */
	hofKnown: boolean;
	/**
	 * Added up over a span of seasons, not the whole career: the Hall of Fame
	 * index, a whole-career number, then reads "All time only".
	 */
	careerOnly?: boolean;
	swing: number | null;
	bestSeason: string | null;
	worstSeason: string | null;
	winStreak: number | null;
	lossStreak: number | null;
}

/** The fields the table reads from /api/managers/performance's all-managers rows. */
export interface PerformanceRow {
	managerName: string;
	firstSeason?: number | string | null;
	lastSeason?: number | string | null;
	totalSeasons?: number | null;
	totalWins?: number | null;
	totalLosses?: number | null;
	totalTies?: number | null;
	totalChampionships?: number | null;
	championshipAppearances?: number | null;
	playoffAppearances?: number | null;
	playoffWinPercentage?: number | null;
	playoffWins?: number | null;
	playoffLosses?: number | null;
	avgPointsPerGame?: number | null;
	totalPointsScored?: number | null;
	careerRegularSeasonWins?: number | null;
	careerPythagoreanWins?: number | null;
	careerLuckFactor?: number | null;
	faabEfficiencyRating?: number | null;
	totalTransactions?: number | null;
	avgTransactionsPerSeason?: number | null;
	seasonConsistencyScore?: number | null;
	bestSeasonRecord?: string | null;
	worstSeasonRecord?: string | null;
	pointsAgainst?: number | null;
	finishedPointsPerSeason?: number | null;
	streaks?: { longestWinStreak: number; longestLossStreak: number } | null;
}

export interface DraftManagerRow {
	managerName: string;
	grade: string;
	averageScore: number;
}

export interface HofRow {
	managerName: string;
	hallOfFameIndex: number | string | null;
	hallOfFameRank: number | null;
}

const n = toNumber;

/**
 * One row per manager in the performance API (D13.9: the same eleven as the
 * Managers page), with the draft grades and the Hall of Fame index joined on
 * by name. Best and worst draft are the Managers page's (D7): the years of
 * the highest and lowest finished-season draft scores.
 */
export function buildRows(
	performance: PerformanceRow[],
	draft: { byManager: DraftManagerRow[]; bySeason: DraftSeasonScore[] } = {
		byManager: [],
		bySeason: []
	},
	/** null when the Hall of Fame didn't load: its columns then read "None". */
	hallOfFame: HofRow[] | null = []
): LeagueRow[] {
	const named = performance.filter((p) => p.managerName);
	const lasts = named.map((p) => n(p.lastSeason)).filter((v): v is number => v != null);
	const latest = lasts.length ? Math.max(...lasts) : null;
	return named.map((p) => {
		const wins = n(p.totalWins) ?? 0;
		const losses = n(p.totalLosses) ?? 0;
		const ties = n(p.totalTies) ?? 0;
		const games = wins + losses + ties;
		const titles = n(p.totalChampionships);
		const appearances = n(p.championshipAppearances);
		const points = n(p.totalPointsScored);
		const pointsAgainst = n(p.pointsAgainst);
		const grade = draft.byManager.find((d) => d.managerName === p.managerName);
		const range = draftRange(draft.bySeason, draft.byManager, p.managerName);
		const hof = hallOfFame?.find((h) => h.managerName === p.managerName);
		const lastSeason = n(p.lastSeason);
		const playoffWins = n(p.playoffWins);
		const playoffLosses = n(p.playoffLosses);
		const playoffGames = (playoffWins ?? 0) + (playoffLosses ?? 0);
		return {
			name: p.managerName,
			firstSeason: n(p.firstSeason),
			lastSeason,
			alum: latest != null && lastSeason != null && lastSeason < latest,
			seasons: n(p.totalSeasons),
			wins,
			losses,
			ties,
			winPct: games > 0 ? (wins + 0.5 * ties) / games : null,
			titles,
			titleGameWins: titles,
			titleGameLosses: titles != null && appearances != null ? appearances - titles : null,
			trips: n(p.playoffAppearances),
			playoffWins,
			playoffLosses,
			// From the record when there is one, so the rate and the record
			// agree to the last digit (the mart stores the rate to 4 places).
			// A 0-0 record is no rate, whatever the mart stored: it used to
			// store 0, which printed 0.0%. The mart's rate is only for a
			// manager whose record didn't come back.
			playoffWinPct:
				playoffWins != null && playoffLosses != null
					? playoffGames > 0
						? playoffWins / playoffGames
						: null
					: n(p.playoffWinPercentage),
			ppg: n(p.avgPointsPerGame),
			ppSeason: n(p.finishedPointsPerSeason),
			points,
			pointsAgainst,
			pointDiff: points != null && pointsAgainst != null ? points - pointsAgainst : null,
			regWins: n(p.careerRegularSeasonWins),
			expectedWins: n(p.careerPythagoreanWins),
			luck: n(p.careerLuckFactor),
			faab: n(p.faabEfficiencyRating),
			moves: n(p.totalTransactions),
			movesPerSeason: n(p.avgTransactionsPerSeason),
			draftGrade: grade?.grade ?? null,
			draftValue: n(grade?.averageScore),
			bestDraft: range.bestYear,
			worstDraft: range.worstYear,
			hofIndex: hof ? n(hof.hallOfFameIndex) : null,
			hofRank: hof?.hallOfFameRank ?? null,
			hofKnown: hallOfFame != null,
			swing: n(p.seasonConsistencyScore),
			bestSeason: p.bestSeasonRecord || null,
			worstSeason: p.worstSeasonRecord || null,
			winStreak: n(p.streaks?.longestWinStreak),
			lossStreak: n(p.streaks?.longestLossStreak)
		};
	});
}

/** The league's first and latest seasons, for the kicker. */
export function seasonSpan(rows: LeagueRow[]): { first: number; latest: number } | null {
	const firsts = rows.map((r) => r.firstSeason).filter((v): v is number => v != null);
	const lasts = rows.map((r) => r.lastSeason).filter((v): v is number => v != null);
	if (!firsts.length || !lasts.length) return null;
	return { first: Math.min(...firsts), latest: Math.max(...lasts) };
}

// ---------------------------------------------------------------------------
// A span of seasons: the same rows, added up from /api/managers/seasons
// ---------------------------------------------------------------------------

/** One team-season (a manager running two teams in a year has two). */
export interface TeamSeasonRow {
	managerName: string;
	seasonYear: number;
	teamKey: number;
	/** Its championship game is in. */
	finished: boolean;
	wins: number;
	losses: number;
	ties: number;
	games: number;
	/** From the matchups, as the career points total counts them. */
	points: number;
	/** From the weekly team rows, as points per season and points against count them. */
	pointsFor: number | null;
	pointsAgainst: number | null;
	titles: number;
	titleGames: number;
	playoffWins: number;
	playoffLosses: number;
	madePlayoffs: boolean;
	/** Every game's result in week order ("WWLT..."), for the streaks. */
	results: string;
}

/** A team-season's regular-season wins against its expected wins (the power rankings'). */
export interface SeasonLuckRow {
	managerName: string;
	seasonYear: number;
	wins: number;
	ties: number;
	pythagoreanWins: number | null;
	luckFactor: number | null;
}

/** A manager-season's transactions and paid claims. */
export interface SeasonMovesRow {
	managerName: string;
	seasonYear: number;
	moves: number;
	faabSpent: number;
	faabPoints: number;
}

export interface SeasonsPayload {
	teamSeasons: TeamSeasonRow[];
	luck: SeasonLuckRow[];
	moves: SeasonMovesRow[];
}

export interface SeasonRange {
	from: number;
	to: number;
}

const inRange = (year: number, range: SeasonRange) => year >= range.from && year <= range.to;

/** Sample standard deviation, as Postgres STDDEV takes it; null under two values. */
function sampleSd(values: number[]): number | null {
	if (values.length < 2) return null;
	const mean = values.reduce((a, b) => a + b, 0) / values.length;
	return Math.sqrt(values.reduce((a, b) => a + (b - mean) ** 2, 0) / (values.length - 1));
}

/**
 * A sum that is null when every value is, as SQL SUM is: the career points
 * against and points per season are SUMs over the weekly team rows.
 */
function sqlSum<T>(rows: T[], f: (row: T) => number | null): number | null {
	const values = rows.map(f).filter((v): v is number => v != null);
	return values.length ? values.reduce((a, b) => a + b, 0) : null;
}

/** Rows grouped by manager, in their original order. */
function byManager<T extends { managerName: string }>(rows: T[]): Map<string, T[]> {
	const out = new Map<string, T[]>();
	for (const row of rows) {
		const list = out.get(row.managerName);
		if (list) list.push(row);
		else out.set(row.managerName, [row]);
	}
	return out;
}

const seasonPct = (t: TeamSeasonRow) => (t.games > 0 ? (t.wins + 0.5 * t.ties) / t.games : 0);

/**
 * The table's rows for a span of seasons: every number the career rows carry,
 * added up from that span's seasons by the career rules (each matches its
 * mart or API counterpart when the span is every season). A manager who
 * played no game in the span has no row. League alum still means they have
 * left the league, whatever the span. The Hall of Fame index is a
 * whole-career number, so it reads "All time only".
 */
export function buildRangeRows(
	career: LeagueRow[],
	seasons: SeasonsPayload,
	draft: { bySeason: (DraftSeasonScore & { exactScore?: number })[]; spread: number | null },
	range: SeasonRange
): LeagueRow[] {
	const span = <T extends { seasonYear: number }>(rows: T[] | undefined) =>
		(rows ?? []).filter((r) => inRange(r.seasonYear, range));
	const teamsOf = byManager(seasons.teamSeasons);
	const luckOf = byManager(seasons.luck);
	const movesOf = byManager(seasons.moves);
	const draftsOf = byManager(draft.bySeason);

	const out: LeagueRow[] = [];
	for (const base of career) {
		const teams = span(teamsOf.get(base.name)).sort(
			(a, b) => a.seasonYear - b.seasonYear || a.teamKey - b.teamKey
		);
		if (!teams.length) continue;
		const sum = (f: (t: TeamSeasonRow) => number) => teams.reduce((acc, t) => acc + f(t), 0);
		const years = new Set(teams.map((t) => t.seasonYear));
		const finished = teams.filter((t) => t.finished);
		const finishedYears = new Set(finished.map((t) => t.seasonYear));

		const wins = sum((t) => t.wins);
		const losses = sum((t) => t.losses);
		const ties = sum((t) => t.ties);
		const games = sum((t) => t.games);
		const titles = sum((t) => t.titles);
		const playoffWins = sum((t) => t.playoffWins);
		const playoffLosses = sum((t) => t.playoffLosses);
		const points = sum((t) => t.points);
		const pointsAgainst = sqlSum(teams, (t) => t.pointsAgainst);
		const finishedPoints = sqlSum(finished, (t) => t.pointsFor);

		const luck = span(luckOf.get(base.name)).filter(
			(l) => l.pythagoreanWins != null && l.luckFactor != null
		);
		const luckSum = (f: (l: SeasonLuckRow) => number) =>
			luck.length ? luck.reduce((acc, l) => acc + f(l), 0) : null;

		const moveRows = span(movesOf.get(base.name));
		const moves = moveRows.reduce((acc, m) => acc + m.moves, 0);
		const faabSpent = moveRows.reduce((acc, m) => acc + m.faabSpent, 0);
		const faabPoints = moveRows.reduce((acc, m) => acc + m.faabPoints, 0);

		const drafts = span(draftsOf.get(base.name)).filter((d) => !d.inProgress);
		const grade =
			drafts.length && draft.spread != null
				? careerGrade(
						drafts.map((d) => d.exactScore ?? d.score),
						draft.spread
					)
				: null;
		const draftYears = draftRange(drafts, [], base.name);

		// Best and worst finished team-season by win rate, the earlier season
		// on a tie (the rows are in season order and the sort is stable).
		const best = [...finished].sort((a, b) => seasonPct(b) - seasonPct(a))[0];
		const worst = [...finished].sort((a, b) => seasonPct(a) - seasonPct(b))[0];
		const record = (t: TeamSeasonRow | undefined) => (t ? `${t.wins}-${t.losses}-${t.ties}` : null);
		const streaks = longestStreaks(teams.flatMap((t) => [...t.results] as GameResult[]));
		const playoffGames = playoffWins + playoffLosses;
		const swing = sampleSd(finished.map(seasonPct));

		// Points per game and moves per season to 2 places, FAAB efficiency and
		// swing to 4, as mart_manager_performance stores them, so a value on the
		// edge of the table's rounding prints the same as on the career rows.
		out.push({
			...base,
			firstSeason: Math.min(...years),
			lastSeason: Math.max(...years),
			seasons: years.size,
			wins,
			losses,
			ties,
			winPct: games > 0 ? (wins + 0.5 * ties) / games : null,
			titles,
			titleGameWins: titles,
			titleGameLosses: sum((t) => t.titleGames) - titles,
			trips: new Set(teams.filter((t) => t.madePlayoffs).map((t) => t.seasonYear)).size,
			playoffWins,
			playoffLosses,
			playoffWinPct: playoffGames > 0 ? playoffWins / playoffGames : null,
			ppg: games > 0 ? round(points / games, 2) : null,
			ppSeason:
				finishedYears.size && finishedPoints != null ? finishedPoints / finishedYears.size : null,
			points,
			pointsAgainst,
			pointDiff: pointsAgainst != null ? points - pointsAgainst : null,
			regWins: luckSum((l) => l.wins + 0.5 * l.ties),
			expectedWins: luckSum((l) => l.pythagoreanWins as number),
			luck: luckSum((l) => l.luckFactor as number),
			faab: faabSpent > 0 ? round(faabPoints / faabSpent, 4) : null,
			moves,
			movesPerSeason: round(moves / years.size, 2),
			draftGrade: grade?.grade ?? null,
			// To 2 places, as the career grades store it.
			draftValue: grade ? round(grade.average, 2) : null,
			bestDraft: draftYears.bestYear,
			worstDraft: draftYears.worstYear,
			hofIndex: null,
			hofRank: null,
			careerOnly: true,
			swing: swing == null ? null : round(swing, 4),
			bestSeason: record(best),
			worstSeason: record(worst),
			winStreak: streaks.longestWinStreak,
			lossStreak: streaks.longestLossStreak
		});
	}
	return out;
}

// ---------------------------------------------------------------------------
// Columns
// ---------------------------------------------------------------------------

/** The first-click direction: high sorts highest first, low lowest first, neutral luckiest first. */
export type Better = 'high' | 'low' | 'neutral';
export type Direction = 'asc' | 'desc';

/** How a value is drawn: plain, signed (field above zero, pennant below), or a tier color. */
export type Tone = 'plain' | 'signed' | ColorToken;

export interface Display {
	text: string;
	tone: Tone;
	/** A blank value's word, in muted 400. */
	blank?: boolean;
}

export interface LeagueColumn {
	key: string;
	label: string;
	/** Desktop width in px (the phone board keeps them). */
	width: number;
	/** null: not sortable (Best draft, Worst draft are years). */
	better: Better | null;
	/** Gets the leader chip on its best value. */
	leader: boolean;
	/** The StatTip's Data Dictionary entry. */
	metricId?: string;
	/** What it sorts by, lexicographically, or null for a blank value. */
	sortKey: (row: LeagueRow) => number[] | null;
	display: (row: LeagueRow) => Display;
}

/** A blank value's word: "None" unless the column has its own (S14.5). */
const blank = (text = 'None'): Display => ({ text, tone: 'plain', blank: true });

/**
 * Rounded to the places shown, a half away from zero as toLocaleString
 * prints it (-639.5 is -640), so ranks, ties and leaders follow what's printed.
 */
function round(value: number, places: number): number {
	const f = 10 ** places;
	return (Math.sign(value) * Math.round(Math.abs(value) * f + 1e-9)) / f;
}

const plain = (text: string): Display => ({ text, tone: 'plain' });

function fixedWithCommas(value: number, places: number): string {
	return value.toLocaleString('en-US', {
		minimumFractionDigits: places,
		maximumFractionDigits: places
	});
}

/**
 * Signed to `places`, with thousands: "+1,897", "-0.99". Zero (judged on the
 * rounded value) has no sign, so +0.03 to one place prints "0.0".
 */
export function signed(value: number, places: number): string {
	const r = round(value, places);
	const text = fixedWithCommas(Math.abs(r), places);
	return r > 0 ? `+${text}` : r < 0 ? `-${text}` : text;
}

/** Career luck: "-1.8 W", "+7.2 W", "0.0 W". */
export function formatLuck(value: number): string {
	return `${signed(value, 1)} W`;
}

/** W-L, with -T only when there's a tie: "188-145-1", "158-139". */
export function formatRecord(row: Pick<LeagueRow, 'wins' | 'losses' | 'ties'>): string {
	return row.ties > 0 ? `${row.wins}-${row.losses}-${row.ties}` : `${row.wins}-${row.losses}`;
}

/** A season record string's win rate ("13-2-0" to .867), or null when it doesn't parse. */
export function recordPct(record: string | null): number | null {
	if (!record) return null;
	const parts = record.split('-').map(Number);
	if (parts.length < 2 || parts.some((p) => !Number.isFinite(p))) return null;
	const [w, l, t = 0] = parts;
	const games = w + l + t;
	return games > 0 ? (w + 0.5 * t) / games : null;
}

/** The Hall of Fame tier as the table names it, short (the board's cells). */
export function tierLabel(rank: number): { text: string; color: ColorToken } {
	const tier = hofTier(rank);
	const short: Record<string, string> = {
		'Hall of Fame Elite': 'HoF Elite',
		'Hall of Fame': 'Hall of Fame',
		'Hall of Very Good': 'Very Good',
		'Solid Contributor': 'Solid',
		'Developing Legacy': 'Developing'
	};
	return { text: short[tier.name] ?? tier.name, color: tier.color };
}

/**
 * A missing HoF index: a whole-career number, so "All time only" on a span of
 * seasons; under three finished seasons when the Hall of Fame loaded;
 * otherwise just missing.
 */
function hofBlank(row: LeagueRow): string | undefined {
	if (row.careerOnly) return 'All time only';
	return row.hofKnown ? 'Under 3 seasons' : undefined;
}

type NumField = {
	[K in keyof LeagueRow]-?: LeagueRow[K] extends number | null ? K : never;
}[keyof LeagueRow];

/** A numeric column: sorts on the value rounded to the places shown. */
function numeric(
	key: string,
	label: string,
	width: number,
	field: NumField,
	places: number,
	format: (value: number) => string,
	opts: {
		better?: Better;
		leader?: boolean;
		metricId?: string;
		/** The blank value's word, or a rule for it. */
		blank?: string | ((row: LeagueRow) => string | undefined);
		tone?: Tone;
	} = {}
): LeagueColumn {
	return {
		key,
		label,
		width,
		better: opts.better ?? 'high',
		leader: opts.leader ?? true,
		metricId: opts.metricId,
		sortKey: (row) => {
			const v = row[field];
			return v == null ? null : [round(v, places)];
		},
		display: (row) => {
			const v = row[field];
			// Printed from the rounded value, so the text and the sort agree.
			const word = typeof opts.blank === 'function' ? opts.blank(row) : opts.blank;
			return v == null
				? blank(word)
				: { text: format(round(v, places)), tone: opts.tone ?? 'plain' };
		}
	};
}

/** A W-L column ("7-3"): most wins first, then fewer losses. */
function winLoss(
	key: string,
	label: string,
	width: number,
	wins: NumField,
	losses: NumField
): LeagueColumn {
	return {
		key,
		label,
		width,
		better: 'high',
		leader: true,
		sortKey: (row) => {
			const w = row[wins];
			const l = row[losses];
			return w == null || l == null ? null : [w, -l];
		},
		display: (row) => {
			const w = row[wins];
			const l = row[losses];
			return w == null || l == null ? blank() : plain(`${w}-${l}`);
		}
	};
}

const int = (v: number) => formatWhole(v);
const one = (v: number) => fixedWithCommas(v, 1);

const COLUMN_LIST: LeagueColumn[] = [
	numeric('seasons', 'Seasons', 76, 'seasons', 0, int),
	numeric('titles', 'Titles', 70, 'titles', 0, int),
	numeric('trips', 'Playoff trips', 82, 'trips', 0, int),
	numeric('winpct', 'Win %', 72, 'winPct', 3, formatPct3),
	numeric('ppg', 'Pts/G', 72, 'ppg', 1, one),
	numeric('luck', 'Luck', 84, 'luck', 1, formatLuck, {
		better: 'neutral',
		leader: false,
		metricId: 'career_luck',
		tone: 'signed'
	}),
	numeric('hof', 'HoF index', 92, 'hofIndex', 3, (v) => v.toFixed(3), {
		metricId: 'hall_of_fame_index',
		blank: hofBlank
	}),
	{
		key: 'record',
		label: 'Record',
		width: 96,
		better: 'high',
		leader: false,
		sortKey: (row) => (row.winPct == null ? null : [round(row.winPct, 3)]),
		display: (row) => plain(formatRecord(row))
	},
	numeric('winstreak', 'Longest win streak', 96, 'winStreak', 0, int),
	numeric('lossstreak', 'Longest losing streak', 104, 'lossStreak', 0, int, { better: 'low' }),
	winLoss('titlegames', 'Title games', 82, 'titleGameWins', 'titleGameLosses'),
	winLoss('precord', 'Playoff record', 96, 'playoffWins', 'playoffLosses'),
	numeric('ppct', 'Playoff win %', 96, 'playoffWinPct', 3, (v) => `${(v * 100).toFixed(1)}%`, {
		metricId: 'playoff_win_percentage'
	}),
	numeric('ppseason', 'Pts/season', 92, 'ppSeason', 1, one),
	numeric('points', 'Career points', 96, 'points', 0, int),
	numeric('pa', 'Points against', 96, 'pointsAgainst', 0, int, { better: 'low' }),
	numeric('diff', 'Point diff', 84, 'pointDiff', 0, (v) => signed(v, 0), { tone: 'signed' }),
	numeric('wins', 'Wins', 72, 'regWins', 1, one),
	numeric('xwins', 'Expected wins', 104, 'expectedWins', 1, one, { metricId: 'all_play_wins' }),
	numeric('faab', 'FAAB efficiency', 120, 'faab', 2, (v) => `${v.toFixed(2)} pts/$`, {
		metricId: 'faab_efficiency_rating',
		blank: 'No bids'
	}),
	numeric('moves', 'Total moves', 86, 'moves', 0, int),
	numeric('movesps', 'Moves/season', 96, 'movesPerSeason', 1, one),
	{
		key: 'grade',
		label: 'Draft grade',
		width: 92,
		better: 'high',
		leader: true,
		metricId: 'draft_grade',
		// The letter sorts by the score it comes from.
		sortKey: (row) =>
			row.draftGrade == null || row.draftValue == null ? null : [round(row.draftValue, 2)],
		display: (row) => (row.draftGrade == null ? blank() : plain(row.draftGrade))
	},
	numeric('dval', 'Draft value', 92, 'draftValue', 2, (v) => signed(v, 2), {
		metricId: 'draft_value_per_season',
		tone: 'signed'
	}),
	{
		key: 'bestdraft',
		label: 'Best draft',
		width: 80,
		better: null,
		leader: false,
		sortKey: () => null,
		display: (row) => (row.bestDraft == null ? blank() : plain(String(row.bestDraft)))
	},
	{
		key: 'worstdraft',
		label: 'Worst draft',
		width: 84,
		better: null,
		leader: false,
		sortKey: () => null,
		display: (row) => (row.worstDraft == null ? blank() : plain(String(row.worstDraft)))
	},
	{
		key: 'tier',
		label: 'HoF tier',
		width: 108,
		better: 'high',
		leader: false,
		sortKey: (row) => (row.hofIndex == null ? null : [round(row.hofIndex, 3)]),
		display: (row) => {
			if (row.hofIndex == null || row.hofRank == null) return blank(hofBlank(row));
			const tier = tierLabel(row.hofRank);
			return { text: tier.text, tone: tier.color };
		}
	},
	numeric('swing', 'Swing', 76, 'swing', 3, (v) => `±${formatPct3(v)}`, {
		better: 'low',
		metricId: 'season_consistency_score'
	}),
	{
		key: 'bestseason',
		label: 'Best season',
		width: 92,
		better: 'high',
		leader: true,
		sortKey: (row) => {
			const pct = recordPct(row.bestSeason);
			return pct == null ? null : [round(pct, 3)];
		},
		display: (row) => (row.bestSeason ? plain(row.bestSeason) : blank())
	},
	{
		key: 'worstseason',
		label: 'Worst season',
		width: 96,
		better: 'high',
		leader: true,
		sortKey: (row) => {
			const pct = recordPct(row.worstSeason);
			return pct == null ? null : [round(pct, 3)];
		},
		display: (row) => (row.worstSeason ? plain(row.worstSeason) : blank())
	}
];

export const COLUMNS: Record<string, LeagueColumn> = Object.fromEntries(
	COLUMN_LIST.map((c) => [c.key, c])
);

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

export type GroupId =
	| 'overview'
	| 'winning'
	| 'playoffs'
	| 'scoring'
	| 'luck'
	| 'market'
	| 'draft'
	| 'legacy';

export interface LeagueGroup {
	id: GroupId;
	label: string;
	description: string;
	columns: string[];
	defaultSort: string;
}

export const GROUPS: LeagueGroup[] = [
	{
		id: 'overview',
		label: 'Overview',
		description: 'One column from each group.',
		columns: ['seasons', 'titles', 'trips', 'winpct', 'ppg', 'luck', 'hof'],
		defaultSort: 'titles'
	},
	{
		id: 'winning',
		label: 'Winning',
		description: 'Every game, regular season and playoffs. A tie counts as half a win.',
		columns: ['seasons', 'record', 'winpct', 'winstreak', 'lossstreak'],
		defaultSort: 'winpct'
	},
	{
		id: 'playoffs',
		label: 'Playoffs',
		description: 'Championships, title games, playoff trips and every playoff game.',
		columns: ['titles', 'titlegames', 'trips', 'precord', 'ppct'],
		defaultSort: 'ppct'
	},
	{
		id: 'scoring',
		label: 'Scoring',
		description: 'Points for and against. Points per season counts finished seasons.',
		columns: ['ppg', 'ppseason', 'points', 'pa', 'diff'],
		defaultSort: 'ppg'
	},
	{
		id: 'luck',
		label: 'Luck',
		description: 'Regular-season wins against the wins a team’s points should have earned.',
		columns: ['wins', 'xwins', 'luck'],
		defaultSort: 'luck'
	},
	{
		id: 'market',
		label: 'Waiver wire',
		description: 'Free agent bids, adds and drops, and trades.',
		columns: ['faab', 'moves', 'movesps'],
		defaultSort: 'faab'
	},
	{
		id: 'draft',
		label: 'Draft',
		description: 'How much each manager’s draft classes returned.',
		columns: ['grade', 'dval', 'bestdraft', 'worstdraft'],
		defaultSort: 'dval'
	},
	{
		id: 'legacy',
		label: 'Legacy',
		description:
			'The Hall of Fame index and how steady each career has been, finished seasons only.',
		columns: ['hof', 'tier', 'swing', 'bestseason', 'worstseason'],
		defaultSort: 'hof'
	}
];

export function groupById(id: GroupId): LeagueGroup {
	return GROUPS.find((g) => g.id === id) ?? GROUPS[0];
}

export function groupColumns(group: LeagueGroup): LeagueColumn[] {
	return group.columns.map((key) => COLUMNS[key]);
}

/** The first-click direction for a column. */
export function firstDirection(column: LeagueColumn): Direction {
	return column.better === 'low' ? 'asc' : 'desc';
}

// ---------------------------------------------------------------------------
// State: group, sort, direction, filter, and the URL that carries them
// ---------------------------------------------------------------------------

export type Who = 'all' | 'active';

export interface TableState {
	group: GroupId;
	sort: string;
	dir: Direction;
	who: Who;
	/** The first season counted; null for the league's first. */
	from: number | null;
	/** The last season counted; null for the latest. */
	to: number | null;
}

export const DEFAULT_STATE: TableState = {
	group: 'overview',
	sort: 'titles',
	dir: 'desc',
	who: 'all',
	from: null,
	to: null
};

/** A season in the URL: a four-digit year, or nothing. */
function yearParam(value: string | null): number | null {
	return value != null && /^\d{4}$/.test(value) ? Number(value) : null;
}

/**
 * The table's state from the URL (`?group=scoring&sort=pa&dir=asc&who=active`).
 * An unknown group, a sort the group doesn't have or can't sort by, a bad
 * direction or filter each fall back to the default.
 */
export function parseState(params: URLSearchParams): TableState {
	const groupParam = params.get('group');
	const group = GROUPS.find((g) => g.id === groupParam) ?? GROUPS[0];
	const sortParam = params.get('sort');
	const sortable =
		sortParam != null && group.columns.includes(sortParam) && COLUMNS[sortParam].better != null;
	const sort = sortable ? sortParam : group.defaultSort;
	const dirParam = params.get('dir');
	const dir: Direction =
		sortable && (dirParam === 'asc' || dirParam === 'desc')
			? dirParam
			: firstDirection(COLUMNS[sort]);
	const who: Who = params.get('who') === 'active' ? 'active' : 'all';
	let from = yearParam(params.get('from'));
	let to = yearParam(params.get('to'));
	if (from != null && to != null && from > to) [from, to] = [to, from];
	return { group: group.id, sort, dir, who, from, to };
}

/** The query string for a state, with every default left out ("" for the defaults). */
export function stateQuery(state: TableState): string {
	const group = groupById(state.group);
	const params = new URLSearchParams();
	if (group.id !== 'overview') params.set('group', group.id);
	const defaultSort = state.sort === group.defaultSort;
	const defaultDir = state.dir === firstDirection(COLUMNS[state.sort]);
	if (!defaultSort || !defaultDir) params.set('sort', state.sort);
	if (!defaultDir) params.set('dir', state.dir);
	if (state.who === 'active') params.set('who', 'active');
	if (state.from != null) params.set('from', String(state.from));
	if (state.to != null) params.set('to', String(state.to));
	const query = params.toString();
	return query ? `?${query}` : '';
}

/**
 * The seasons the table counts, inside the league's span: the whole span
 * (`all`) when the state picks no narrower one. A season outside the span
 * is pulled in to its edge.
 */
export function seasonRange(
	state: Pick<TableState, 'from' | 'to'>,
	span: { first: number; latest: number }
): SeasonRange & { all: boolean } {
	const clamp = (y: number) => Math.min(span.latest, Math.max(span.first, y));
	let from = state.from == null ? span.first : clamp(state.from);
	let to = state.to == null ? span.latest : clamp(state.to);
	if (from > to) [from, to] = [to, from];
	return { from, to, all: from === span.first && to === span.latest };
}

/**
 * A new first or last season. The league's own first or latest season is
 * left out of the state (and the URL), and a first season after the last
 * (or a last before the first) moves the other end with it, so the span
 * is never empty.
 */
export function pickSeason(
	state: TableState,
	end: 'from' | 'to',
	year: number,
	span: { first: number; latest: number }
): TableState {
	const current = seasonRange(state, span);
	let from = end === 'from' ? year : current.from;
	let to = end === 'to' ? year : current.to;
	if (from > to) {
		if (end === 'from') to = from;
		else from = to;
	}
	return {
		...state,
		from: from <= span.first ? null : from,
		to: to >= span.latest ? null : to
	};
}

/** "all time", or the span: "2010 to 2020", "2020" for one season. */
export function rangeLabel(range: SeasonRange & { all: boolean }): string {
	if (range.all) return 'all time';
	return range.from === range.to ? String(range.from) : `${range.from} to ${range.to}`;
}

/**
 * A header click: the first click on a column sorts it in its better
 * direction, a second click on the sorted column flips it.
 */
export function clickSort(state: TableState, key: string): TableState {
	const column = COLUMNS[key];
	if (!column || column.better == null) return state;
	if (state.sort === key) return { ...state, dir: state.dir === 'desc' ? 'asc' : 'desc' };
	return { ...state, sort: key, dir: firstDirection(column) };
}

/**
 * Switching groups keeps the sort (and its direction) when the new group has
 * that column, and otherwise sorts by the new group's default.
 */
export function switchGroup(state: TableState, id: GroupId): TableState {
	const group = groupById(id);
	if (group.columns.includes(state.sort)) return { ...state, group: group.id };
	const sort = group.defaultSort;
	return { ...state, group: group.id, sort, dir: firstDirection(COLUMNS[sort]) };
}

// ---------------------------------------------------------------------------
// Sorting and ranks
// ---------------------------------------------------------------------------

function compareKeys(a: number[], b: number[]): number {
	for (let i = 0; i < Math.max(a.length, b.length); i++) {
		const d = (a[i] ?? 0) - (b[i] ?? 0);
		if (d !== 0) return d;
	}
	return 0;
}

/**
 * The rows sorted by a column. Blank values go last in both directions.
 * Inside a tie, rows order by win %, highest first, then name, so the order
 * is stable.
 */
export function sortRows(
	rows: LeagueRow[],
	column: LeagueColumn,
	direction: Direction
): LeagueRow[] {
	const sign = direction === 'desc' ? -1 : 1;
	return [...rows].sort((a, b) => {
		const ka = column.sortKey(a);
		const kb = column.sortKey(b);
		if (ka == null || kb == null) {
			if (ka == null && kb == null) return a.name.localeCompare(b.name);
			return ka == null ? 1 : -1;
		}
		const d = compareKeys(ka, kb);
		if (d !== 0) return sign * d;
		return (b.winPct ?? -1) - (a.winPct ?? -1) || a.name.localeCompare(b.name);
	});
}

export interface RankedRow {
	row: LeagueRow;
	/** null for a blank value: it gets no rank. */
	rank: number | null;
	/** Shares its rank with another row. */
	tied: boolean;
}

/**
 * Ranks for rows already in sorted order: equal values share a rank and the
 * next rank skips (1, T2, T2, T2, 5). A blank value gets none.
 */
export function rankRows(sorted: LeagueRow[], column: LeagueColumn): RankedRow[] {
	const keys = sorted.map((row) => column.sortKey(row));
	return sorted.map((row, i) => {
		const key = keys[i];
		if (key == null) return { row, rank: null, tied: false };
		const first = keys.findIndex((k) => k != null && compareKeys(k, key) === 0);
		const count = keys.filter((k) => k != null && compareKeys(k, key) === 0).length;
		return { row, rank: first + 1, tied: count > 1 };
	});
}

/**
 * The names holding a column's best value, by its better direction; every
 * manager tied for the best. Columns with no better direction (Luck, the
 * draft years, the HoF tier, Record) have no leader.
 */
export function leaders(rows: LeagueRow[], column: LeagueColumn): Set<string> {
	if (!column.leader || column.better == null || column.better === 'neutral') return new Set();
	const keyed = rows
		.map((row) => ({ row, key: column.sortKey(row) }))
		.filter((x): x is { row: LeagueRow; key: number[] } => x.key != null);
	if (!keyed.length) return new Set();
	const sign = column.better === 'low' ? 1 : -1;
	const best = keyed.reduce((acc, x) => (sign * compareKeys(x.key, acc.key) < 0 ? x : acc)).key;
	return new Set(keyed.filter((x) => compareKeys(x.key, best) === 0).map((x) => x.row.name));
}

/** The rows the filter shows: everyone, or only managers still in the league. */
export function filterRows(rows: LeagueRow[], who: Who): LeagueRow[] {
	return who === 'active' ? rows.filter((r) => !r.alum) : rows;
}

// ---------------------------------------------------------------------------
// Copy
// ---------------------------------------------------------------------------

/** "sorted by titles, highest first"; Luck reads "luckiest first" and "unluckiest first". */
export function sortPhrase(column: LeagueColumn, direction: Direction): string {
	const order =
		column.better === 'neutral'
			? direction === 'desc'
				? 'luckiest first'
				: 'unluckiest first'
			: direction === 'desc'
				? 'highest first'
				: 'lowest first';
	return `sorted by ${column.label.toLowerCase()}, ${order}`;
}

/** "11 managers · sorted by titles, highest first". */
export function sortLine(count: number, column: LeagueColumn, direction: Direction): string {
	return `${managerCount(count)} · ${sortPhrase(column, direction)}`;
}

/** The table's hidden caption: "League table, Overview, sorted by Titles". */
export function caption(group: LeagueGroup, column: LeagueColumn): string {
	return `League table, ${group.label}, sorted by ${column.label}`;
}

/**
 * Phones: the sorted column moves to the first spot after Manager, so the
 * number the table is sorted by is always on screen. The others keep order.
 */
export function phoneColumns(group: LeagueGroup, sort: string): LeagueColumn[] {
	const cols = groupColumns(group);
	const sorted = cols.find((c) => c.key === sort);
	return sorted ? [sorted, ...cols.filter((c) => c.key !== sort)] : cols;
}

/** The two columns that never change: # and Manager, in px, and the gap before Manager. */
export function leadColumns(phone: boolean): { rank: number; manager: number; gap: number } {
	return phone ? { rank: 32, manager: 128, gap: 8 } : { rank: 44, manager: 250, gap: 8 };
}

/** The table's min-width in px: every column, its gap, and the board's 4px. */
export function tableMinWidth(columns: LeagueColumn[], phone: boolean): number {
	const l = leadColumns(phone);
	const lead = l.rank + l.gap + l.manager;
	return lead + columns.reduce((sum, c) => sum + 12 + c.width, 0) + 4;
}

/** The value's tone with zero judged on the printed text: "0.0 W" is plain. */
export function signTone(display: Display): 'up' | 'down' | 'zero' {
	if (display.text.startsWith('+')) return 'up';
	if (display.text.startsWith('-')) return 'down';
	return 'zero';
}
