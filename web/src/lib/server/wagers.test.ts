import { describe, expect, it } from 'vitest';
import { computeLedger, type WagerRow } from './wagers';

function row(over: Partial<WagerRow>): WagerRow {
	return {
		wagerKey: 1,
		wagerId: 'bet-1',
		title: 'A bet',
		terms: 'terms',
		stake: '$20',
		seasonYear: 2025,
		proposedBy: 14,
		proposerName: 'Luke S',
		counterpartyKey: 19,
		counterpartyName: 'Troy Colvin',
		acceptedBy: 19,
		takerName: 'Troy Colvin',
		acceptedAt: new Date(),
		status: 'settled',
		resolutionRequestedBy: null,
		resolutionRequestedAt: null,
		resolutionNote: null,
		outcome: 'proposer',
		winnerKey: 14,
		winnerName: 'Luke S',
		rulingNote: null,
		resolvedAt: new Date(),
		createdAt: new Date(),
		...over
	};
}

describe('computeLedger', () => {
	it('credits the winner and debits the other side', () => {
		const ledger = computeLedger([row({ wagerKey: 1, outcome: 'proposer', winnerKey: 14 })]);

		expect(ledger).toEqual([
			{ managerKey: 14, name: 'Luke S', won: 1, lost: 0, pushed: 0 },
			{ managerKey: 19, name: 'Troy Colvin', won: 0, lost: 1, pushed: 0 }
		]);
	});

	it('counts a push for both and a win for neither', () => {
		const ledger = computeLedger([
			row({ wagerKey: 1, outcome: 'push', winnerKey: null, winnerName: null })
		]);

		expect(ledger.every((e) => e.won === 0 && e.lost === 0 && e.pushed === 1)).toBe(true);
	});

	it('ignores anything that never settled', () => {
		// A voided bet carries an outcome and a resolvedAt, so filtering on those
		// would let it into someone's record. Only `status === 'settled'` counts.
		expect(
			computeLedger([
				row({ wagerKey: 1, status: 'void', outcome: 'void', winnerKey: null }),
				row({ wagerKey: 2, status: 'accepted', outcome: null, winnerKey: null }),
				row({ wagerKey: 3, status: 'open', acceptedBy: null, outcome: null, winnerKey: null })
			])
		).toEqual([]);
	});

	it('accumulates across bets and ranks by wins', () => {
		const ledger = computeLedger([
			row({ wagerKey: 1, outcome: 'proposer', winnerKey: 14 }),
			row({ wagerKey: 2, outcome: 'proposer', winnerKey: 14 }),
			row({ wagerKey: 3, outcome: 'taker', winnerKey: 19 })
		]);

		expect(ledger[0]).toEqual({ managerKey: 14, name: 'Luke S', won: 2, lost: 1, pushed: 0 });
		expect(ledger[1]).toEqual({ managerKey: 19, name: 'Troy Colvin', won: 1, lost: 2, pushed: 0 });
	});
});
