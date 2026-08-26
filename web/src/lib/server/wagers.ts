import { alias } from 'drizzle-orm/pg-core';
import { desc, eq } from 'drizzle-orm';
import { db } from './db';
import { dimManager, wager } from './db/schema';

/**
 * Side bets — an informal ledger, not a book.
 *
 * The league makes bets with each other constantly and then argues about what
 * was actually agreed. This records the terms at the moment both sides said yes,
 * and the commissioner's ruling at the end. Nothing here moves money and nothing
 * here is enforceable by anything except embarrassment.
 */

export const WAGER_STATUSES = [
	'open',
	'accepted',
	'declined',
	'withdrawn',
	'pending_resolution',
	'settled',
	'void'
] as const;
export type WagerStatus = (typeof WAGER_STATUSES)[number];

/** Who the bet went to. `push` is a tie; `void` means it never really happened. */
export const WAGER_OUTCOMES = ['proposer', 'taker', 'push', 'void'] as const;
export type WagerOutcome = (typeof WAGER_OUTCOMES)[number];

export function isWagerOutcome(value: string): value is WagerOutcome {
	return (WAGER_OUTCOMES as readonly string[]).includes(value);
}

export interface WagerRow {
	wagerKey: number;
	wagerId: string;
	title: string;
	terms: string;
	stake: string;
	seasonYear: number | null;
	proposedBy: number;
	proposerName: string | null;
	counterpartyKey: number | null;
	counterpartyName: string | null;
	acceptedBy: number | null;
	takerName: string | null;
	acceptedAt: Date | null;
	status: string;
	resolutionRequestedBy: number | null;
	resolutionRequestedAt: Date | null;
	resolutionNote: string | null;
	outcome: string | null;
	winnerKey: number | null;
	winnerName: string | null;
	rulingNote: string | null;
	resolvedAt: Date | null;
	createdAt: Date;
}

/**
 * Every bet, newest first, with the four manager references resolved to names.
 *
 * Names come from `edw.dim_manager` while the FKs point at `app.league_member`:
 * the allowlist decides who may be named on a bet, dim_manager just knows what
 * to call them. Four aliases because the same dimension is joined four times.
 */
export async function listWagers(): Promise<WagerRow[]> {
	const proposer = alias(dimManager, 'proposer');
	const counterparty = alias(dimManager, 'counterparty');
	const taker = alias(dimManager, 'taker');
	const winner = alias(dimManager, 'winner');

	return db
		.select({
			wagerKey: wager.wagerKey,
			wagerId: wager.wagerId,
			title: wager.title,
			terms: wager.terms,
			stake: wager.stake,
			seasonYear: wager.seasonYear,
			proposedBy: wager.proposedBy,
			proposerName: proposer.managerName,
			counterpartyKey: wager.counterpartyKey,
			counterpartyName: counterparty.managerName,
			acceptedBy: wager.acceptedBy,
			takerName: taker.managerName,
			acceptedAt: wager.acceptedAt,
			status: wager.status,
			resolutionRequestedBy: wager.resolutionRequestedBy,
			resolutionRequestedAt: wager.resolutionRequestedAt,
			resolutionNote: wager.resolutionNote,
			outcome: wager.outcome,
			winnerKey: wager.winnerKey,
			winnerName: winner.managerName,
			rulingNote: wager.rulingNote,
			resolvedAt: wager.resolvedAt,
			createdAt: wager.createdAt
		})
		.from(wager)
		.leftJoin(proposer, eq(wager.proposedBy, proposer.managerKey))
		.leftJoin(counterparty, eq(wager.counterpartyKey, counterparty.managerKey))
		.leftJoin(taker, eq(wager.acceptedBy, taker.managerKey))
		.leftJoin(winner, eq(wager.winnerKey, winner.managerKey))
		.orderBy(desc(wager.createdAt));
}

export interface LedgerEntry {
	managerKey: number;
	name: string;
	won: number;
	lost: number;
	pushed: number;
}

/**
 * Settled W-L per manager.
 *
 * Pure over the rows so it is testable without a database, and deliberately not
 * a SQL aggregate: a bet contributes to two managers at once, which is a union
 * of two GROUP BYs in SQL and four lines here.
 *
 * Only `settled` counts. A `void` bet has an outcome recorded but explicitly did
 * not happen, so it must not show up in anyone's record.
 */
export function computeLedger(wagers: WagerRow[]): LedgerEntry[] {
	const byManager = new Map<number, LedgerEntry>();

	const entry = (key: number, name: string | null): LedgerEntry => {
		let found = byManager.get(key);
		if (!found) {
			found = { managerKey: key, name: name ?? `Manager ${key}`, won: 0, lost: 0, pushed: 0 };
			byManager.set(key, found);
		}
		return found;
	};

	for (const w of wagers) {
		if (w.status !== 'settled' || w.acceptedBy == null) continue;

		const proposerSide = entry(w.proposedBy, w.proposerName);
		const takerSide = entry(w.acceptedBy, w.takerName);

		if (w.outcome === 'proposer') {
			proposerSide.won += 1;
			takerSide.lost += 1;
		} else if (w.outcome === 'taker') {
			takerSide.won += 1;
			proposerSide.lost += 1;
		} else if (w.outcome === 'push') {
			proposerSide.pushed += 1;
			takerSide.pushed += 1;
		}
	}

	return [...byManager.values()].sort(
		(a, b) => b.won - a.won || a.lost - b.lost || a.name.localeCompare(b.name)
	);
}
