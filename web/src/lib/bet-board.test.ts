import { describe, expect, it } from 'vitest';
import {
	betCounts,
	betDate,
	countChips,
	groupBets,
	ledgerRows,
	recordText,
	STAGES,
	verdictLabel
} from './bet-board';
import type { LedgerEntry } from '$lib/server/wagers';

const entry = (managerKey: number, name: string, won: number, lost: number, pushed = 0): LedgerEntry => ({
	managerKey,
	name,
	won,
	lost,
	pushed
});

describe('ledgerRows', () => {
	const members = [
		{ managerKey: 3, name: 'Trevor' },
		{ managerKey: 1, name: 'Craig' },
		{ managerKey: 2, name: 'Luke S' },
		{ managerKey: 4, name: 'Omar' }
	];

	it('lists every member at 0-0, A to Z, when nobody has a record', () => {
		expect(ledgerRows(members, [])).toEqual([
			{ managerKey: 1, name: 'Craig', record: '0-0', hasRecord: false },
			{ managerKey: 2, name: 'Luke S', record: '0-0', hasRecord: false },
			{ managerKey: 4, name: 'Omar', record: '0-0', hasRecord: false },
			{ managerKey: 3, name: 'Trevor', record: '0-0', hasRecord: false }
		]);
	});

	it('puts record holders first in the ledger order, then everyone else A to Z', () => {
		const ledger = [entry(3, 'Trevor', 2, 0), entry(2, 'Luke S', 1, 1, 1), entry(4, 'Omar', 0, 2)];
		expect(ledgerRows(members, ledger).map((r) => [r.name, r.record, r.hasRecord])).toEqual([
			['Trevor', '2-0', true],
			['Luke S', '1-1-1', true],
			['Omar', '0-2', true],
			['Craig', '0-0', false]
		]);
	});

	it('appends pushes to the record', () => {
		expect(ledgerRows(members, [entry(1, 'Craig', 0, 0, 2)])[0].record).toBe('0-0-2');
	});

	it('gives a record of only pushes its own row, not a 0-0 one', () => {
		const rows = ledgerRows(members, [entry(1, 'Craig', 0, 0, 1)]);
		expect(rows.filter((r) => r.managerKey === 1)).toHaveLength(1);
		expect(rows[0].hasRecord).toBe(true);
	});

	it('leaves out an entry for someone who is no longer a member', () => {
		const rows = ledgerRows(members, [entry(99, 'Bobby', 3, 0)]);
		expect(rows.map((r) => r.managerKey)).toEqual([1, 2, 4, 3]);
	});

	it('names a member the way the member list does', () => {
		expect(ledgerRows(members, [entry(2, 'Manager 2', 1, 0)])[0].name).toBe('Luke S');
	});
});

describe('recordText', () => {
	it('reads won-lost, with pushes only when there are some', () => {
		expect(recordText({ won: 3, lost: 1, pushed: 0 })).toBe('3-1');
		expect(recordText({ won: 3, lost: 1, pushed: 2 })).toBe('3-1-2');
	});
});

describe('verdictLabel', () => {
	it('marks a voided bet as no action, in muted', () => {
		expect(verdictLabel({ status: 'void', outcome: 'void', winnerName: null })).toEqual({
			text: 'Voided, no action',
			tone: 'muted'
		});
	});

	it('calls a push a push, in ink-2', () => {
		expect(verdictLabel({ status: 'settled', outcome: 'push', winnerName: null })).toEqual({
			text: 'Push',
			tone: 'ink-2'
		});
	});

	it('names the winner, in field', () => {
		expect(verdictLabel({ status: 'settled', outcome: 'taker', winnerName: 'Trevor' })).toEqual({
			text: 'Trevor won',
			tone: 'field'
		});
	});

	it('falls back to "Settled" with no winner on record', () => {
		expect(verdictLabel({ status: 'settled', outcome: 'proposer', winnerName: null }).text).toBe(
			'Settled'
		);
	});
});

describe('the counts', () => {
	const statuses = ['open', 'open', 'accepted', 'pending_resolution', 'settled', 'void', 'void', 'declined'];
	const wagers = statuses.map((status) => ({ status }));

	it('counts voided bets as neither settled nor anything else', () => {
		expect(betCounts(wagers)).toEqual({ onBoard: 2, live: 1, awaitingRuling: 1, settled: 1 });
	});

	it('still shows voided bets on the Settled card', () => {
		expect(groupBets(wagers).settled.map((w) => w.status)).toEqual(['settled', 'void', 'void']);
	});

	it('keeps declined bets for Passed on', () => {
		expect(groupBets(wagers).passed).toHaveLength(1);
	});

	it("reads the board's chip copy", () => {
		expect(countChips({ onBoard: 1, live: 1, awaitingRuling: 1, settled: 0 })).toEqual([
			'1 on the board',
			'1 live',
			'1 awaiting a ruling',
			'0 settled'
		]);
	});
});

describe('betDate', () => {
	it('formats a date the way the page always has', () => {
		expect(betDate('2026-11-07T18:00:00Z')).toBe('Nov 7, 2026');
	});

	it("writes the reader's day", () => {
		// 11:30pm Pacific on Nov 6 is already Nov 7 in Mountain time.
		expect(betDate('2026-11-07T07:30:00Z', 'America/Los_Angeles')).toBe('Nov 6, 2026');
		expect(betDate('2026-11-07T07:30:00Z', 'America/Denver')).toBe('Nov 7, 2026');
	});

	it('is null for a missing or broken date, never a dash', () => {
		expect(betDate(null)).toBeNull();
		expect(betDate('not a date')).toBeNull();
	});
});

describe('STAGES', () => {
	it('runs in board order with Passed on last', () => {
		expect(STAGES.map((s) => s.title)).toEqual([
			'On the board',
			'Live',
			'Awaiting a ruling',
			'Settled',
			'Passed on'
		]);
	});
});
