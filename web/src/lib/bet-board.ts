/**
 * Pure rules behind the Bet Board (WP9), kept out of the page so they can be
 * tested on their own (build spec G9, S9.10).
 *
 * The page reads `WagerRow`s from load. Nothing here decides who may do what:
 * that's still `canAccept`, `isMine` and `isCommissioner` in the page, as before.
 */
import type { LedgerEntry, WagerRow } from '$lib/server/wagers';

/** The fields of a bet these helpers read. Load serialises dates, so either form is fine. */
export type BetLike = Pick<
	WagerRow,
	'status' | 'outcome' | 'winnerName'
>;

export interface BetGroups<T> {
	onBoard: T[];
	live: T[];
	awaitingRuling: T[];
	/** Settled and voided bets: both have a ruling. */
	settled: T[];
	passed: T[];
}

/** Every bet in the stage it sits in, in load's order (newest first). */
export function groupBets<T extends Pick<WagerRow, 'status'>>(wagers: T[]): BetGroups<T> {
	return {
		onBoard: wagers.filter((w) => w.status === 'open'),
		live: wagers.filter((w) => w.status === 'accepted'),
		awaitingRuling: wagers.filter((w) => w.status === 'pending_resolution'),
		settled: wagers.filter((w) => w.status === 'settled' || w.status === 'void'),
		passed: wagers.filter((w) => w.status === 'declined')
	};
}

export interface BetCounts {
	onBoard: number;
	live: number;
	awaitingRuling: number;
	settled: number;
}

/**
 * The four count chips. Voided bets share the Settled card, because they have
 * a ruling, but they explicitly did not settle, so they don't count as settled.
 */
export function betCounts(wagers: Pick<WagerRow, 'status'>[]): BetCounts {
	const groups = groupBets(wagers);
	return {
		onBoard: groups.onBoard.length,
		live: groups.live.length,
		awaitingRuling: groups.awaitingRuling.length,
		settled: wagers.filter((w) => w.status === 'settled').length
	};
}

/** The chips' copy, in board order. */
export function countChips(counts: BetCounts): string[] {
	return [
		`${counts.onBoard} on the board`,
		`${counts.live} live`,
		`${counts.awaitingRuling} awaiting a ruling`,
		`${counts.settled} settled`
	];
}

export type VerdictTone = 'muted' | 'ink-2' | 'field';

/** What a settled card says in place of the stake, and the color it says it in. */
export function verdictLabel(w: BetLike): { text: string; tone: VerdictTone } {
	if (w.status === 'void') return { text: 'Voided, no action', tone: 'muted' };
	if (w.outcome === 'push') return { text: 'Push', tone: 'ink-2' };
	if (w.winnerName) return { text: `${w.winnerName} won`, tone: 'field' };
	return { text: 'Settled', tone: 'field' };
}

/** "3-1", or "3-1-2" with pushes, as the ledger has always read. */
export function recordText(entry: Pick<LedgerEntry, 'won' | 'lost' | 'pushed'>): string {
	return `${entry.won}-${entry.lost}${entry.pushed ? `-${entry.pushed}` : ''}`;
}

export interface LedgerRow {
	managerKey: number;
	name: string;
	record: string;
	/** False for a member with no settled bets, who reads 0-0 in muted. */
	hasRecord: boolean;
}

/**
 * One ledger tile per active member, record or no record (S9.8). Members with
 * a ledger entry come first, in computeLedger's order (most wins first), then
 * everyone else A to Z at 0-0. An entry for someone who is no longer an active
 * member has no tile, since the ledger lists the league as it stands.
 */
export function ledgerRows(
	members: { managerKey: number; name: string }[],
	ledger: LedgerEntry[]
): LedgerRow[] {
	const active = new Map(members.map((m) => [m.managerKey, m]));
	const withRecord = ledger
		.filter((e) => active.has(e.managerKey))
		.map((e) => ({
			managerKey: e.managerKey,
			name: active.get(e.managerKey)!.name,
			record: recordText(e),
			hasRecord: true
		}));
	const seen = new Set(withRecord.map((r) => r.managerKey));
	const rest = members
		.filter((m) => !seen.has(m.managerKey))
		.sort((a, b) => a.name.localeCompare(b.name))
		.map((m) => ({ managerKey: m.managerKey, name: m.name, record: '0-0', hasRecord: false }));
	return [...withRecord, ...rest];
}

/** "Nov 7, 2026", or null when there's no date, so the page shows a word instead. */
export function betDate(value: string | Date | null | undefined): string | null {
	if (!value) return null;
	const date = new Date(value);
	if (Number.isNaN(date.getTime())) return null;
	return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

export type StageKey = 'onBoard' | 'live' | 'awaitingRuling' | 'settled' | 'passed';

export interface Stage {
	key: StageKey;
	title: string;
	/** The dot's color token, without the --color- prefix. */
	dot: 'sage' | 'field' | 'pennant' | 'ink' | 'muted';
	/** The one-line description an empty stage shows under its head. */
	what: string;
	/** The dashed box's line when the stage is empty. */
	empty: string;
}

/**
 * The stage cards in board order, with the board's `sections` copy. Settled's
 * description is the longer line the markup draws. Passed on isn't on the board
 * (S9.4); it only shows when it has bets, so its empty lines never render.
 */
export const STAGES: Stage[] = [
	{
		key: 'onBoard',
		title: 'On the board',
		dot: 'sage',
		what: 'Offered and waiting for someone to take it.',
		empty: 'Nothing offered right now.'
	},
	{
		key: 'live',
		title: 'Live',
		dot: 'field',
		what: 'Both sides agreed. Now it plays out.',
		empty: 'No bets in flight.'
	},
	{
		key: 'awaitingRuling',
		title: 'Awaiting a ruling',
		dot: 'pennant',
		what: 'Over, and somebody has to call it.',
		empty: "Nothing on the commissioner's desk."
	},
	{
		key: 'settled',
		title: 'Settled',
		dot: 'ink',
		what: 'Ruled on and counted in the ledger. Voided bets show here too, marked "Voided, no action".',
		empty: 'Nothing has been ruled on yet.'
	},
	{
		key: 'passed',
		title: 'Passed on',
		dot: 'muted',
		what: '',
		empty: ''
	}
];
