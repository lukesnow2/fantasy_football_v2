import { fail, redirect } from '@sveltejs/kit';
import { and, eq, inArray, sql } from 'drizzle-orm';
import { nanoid } from 'nanoid';
import { db } from '$lib/server/db';
import { wager } from '$lib/server/db/schema';
import { requireCommissioner, requireManagerKey } from '$lib/server/auth-manager';
import { listActiveMembers } from '$lib/server/members';
import {
	computeLedger,
	isWagerOutcome,
	listBettableManagers,
	listWagers
} from '$lib/server/wagers';
import type { Actions, PageServerLoad } from './$types';

const MAX_TITLE = 200;
const MAX_STAKE = 200;
const MAX_TERMS = 2000;
const MAX_NOTE = 1000;

/** The league's first season; nothing can be bet on anything earlier. */
const FIRST_SEASON = 2005;
/** Next season is biddable during the preseason, so the ceiling moves with the clock. */
const LATEST_SEASON = () => new Date().getFullYear() + 1;

/**
 * The bet board is members-only.
 *
 * hooks.server.ts already redirects — every route is private unless named in
 * PUBLIC_PREFIXES, and /bets deliberately is not. This guard is here anyway
 * because a guard nobody can see from the route file is a guard someone will
 * later remove by accident.
 */
export const load: PageServerLoad = async ({ locals }) => {
	if (!locals.user) throw redirect(302, '/login?redirect=/bets');

	const wagers = await listWagers();

	return {
		wagers,
		ledger: computeLedger(wagers),
		members: await listBettableManagers(),
		myManagerKey: locals.member?.active ? locals.member.managerKey : null,
		isCommissioner: locals.member?.role === 'commissioner'
	};
};

export const actions: Actions = {
	propose: async ({ request, locals }) => {
		const managerKey = requireManagerKey(locals);
		const form = await request.formData();

		const title = String(form.get('title') ?? '').trim();
		const terms = String(form.get('terms') ?? '').trim();
		const stake = String(form.get('stake') ?? '').trim();
		const seasonRaw = String(form.get('seasonYear') ?? '').trim();
		const counterpartyRaw = String(form.get('counterpartyKey') ?? '').trim();

		if (!title) return fail(400, { error: 'Give the bet a name.' });
		if (!terms) return fail(400, { error: 'Spell out what has to happen for you to win.' });
		if (!stake) return fail(400, { error: "Say what's at stake — $20, wings, bragging rights." });

		// Bounded, not just parsed. season_year is int4, so an unbounded parseInt
		// lets "99999999999" through to Postgres, which raises 22003 and turns a
		// bad form field into a 500.
		const seasonYear = seasonRaw ? Number.parseInt(seasonRaw, 10) : null;
		if (
			seasonYear !== null &&
			(Number.isNaN(seasonYear) || seasonYear < FIRST_SEASON || seasonYear > LATEST_SEASON())
		) {
			return fail(400, { error: `Pick a season between ${FIRST_SEASON} and ${LATEST_SEASON()}.` });
		}

		let counterpartyKey: number | null = null;
		if (counterpartyRaw) {
			counterpartyKey = Number.parseInt(counterpartyRaw, 10);
			if (Number.isNaN(counterpartyKey)) {
				return fail(400, { error: 'Pick a manager, or leave it open to anyone.' });
			}
			if (counterpartyKey === managerKey) {
				return fail(400, { error: "You can't bet against yourself." });
			}
			// Checked against the allowlist rather than left to the FK: a foreign key
			// violation surfaces as a 500, and "that manager is not on the roster" is
			// something the person filling in the form should just be told.
			const members = await listActiveMembers();
			if (!members.some((m) => m.managerKey === counterpartyKey)) {
				return fail(400, { error: 'That manager is not on the league roster.' });
			}
		}

		await db.insert(wager).values({
			wagerId: `bet-${nanoid(10)}`,
			title: title.slice(0, MAX_TITLE),
			terms: terms.slice(0, MAX_TERMS),
			stake: stake.slice(0, MAX_STAKE),
			seasonYear,
			proposedBy: managerKey,
			counterpartyKey,
			status: 'open'
		});

		return { success: true };
	},

	accept: async ({ request, locals }) => {
		const managerKey = requireManagerKey(locals);
		const wagerKey = Number(String((await request.formData()).get('wagerKey')));
		if (Number.isNaN(wagerKey)) return fail(400, { error: 'Which bet?' });

		const [existing] = await db
			.select({
				proposedBy: wager.proposedBy,
				counterpartyKey: wager.counterpartyKey,
				status: wager.status
			})
			.from(wager)
			.where(eq(wager.wagerKey, wagerKey))
			.limit(1);

		if (!existing) return fail(404, { error: 'That bet no longer exists.' });
		if (existing.proposedBy === managerKey) {
			return fail(403, { error: "That's your own bet." });
		}
		if (existing.counterpartyKey != null && existing.counterpartyKey !== managerKey) {
			return fail(403, { error: 'That bet was offered to someone else.' });
		}

		// Conditional on the status, not on the row read above. Two managers racing
		// to take the same open prop would both pass the check and both write; the
		// WHERE clause makes the second write match zero rows instead.
		const taken = await db
			.update(wager)
			.set({
				acceptedBy: managerKey,
				acceptedAt: new Date(),
				status: 'accepted',
				updatedAt: new Date()
			})
			.where(and(eq(wager.wagerKey, wagerKey), eq(wager.status, 'open')))
			.returning({ wagerKey: wager.wagerKey });

		if (taken.length === 0) {
			return fail(409, { error: 'Someone already took that one.' });
		}

		return { success: true };
	},

	decline: async ({ request, locals }) => {
		const managerKey = requireManagerKey(locals);
		const wagerKey = Number(String((await request.formData()).get('wagerKey')));
		if (Number.isNaN(wagerKey)) return fail(400, { error: 'Which bet?' });

		// Only the named counterparty declines. An open prop has nobody to decline
		// it — it just sits there until someone takes it or the proposer pulls it.
		const declined = await db
			.update(wager)
			.set({ status: 'declined', updatedAt: new Date() })
			.where(
				and(
					eq(wager.wagerKey, wagerKey),
					eq(wager.status, 'open'),
					eq(wager.counterpartyKey, managerKey)
				)
			)
			.returning({ wagerKey: wager.wagerKey });

		if (declined.length === 0) {
			return fail(409, { error: "That bet isn't yours to decline, or it already moved on." });
		}

		return { success: true };
	},

	withdraw: async ({ request, locals }) => {
		const managerKey = requireManagerKey(locals);
		const wagerKey = Number(String((await request.formData()).get('wagerKey')));
		if (Number.isNaN(wagerKey)) return fail(400, { error: 'Which bet?' });

		// Deleted rather than flagged: nobody agreed to it, so there is no agreement
		// to keep a record of. Once accepted it stays on the board forever.
		const removed = await db
			.delete(wager)
			.where(
				and(
					eq(wager.wagerKey, wagerKey),
					eq(wager.proposedBy, managerKey),
					inArray(wager.status, ['open', 'declined'])
				)
			)
			.returning({ wagerKey: wager.wagerKey });

		if (removed.length === 0) {
			return fail(409, { error: "You can't pull a bet someone already took." });
		}

		return { success: true };
	},

	requestResolution: async ({ request, locals }) => {
		const managerKey = requireManagerKey(locals);
		const form = await request.formData();
		const wagerKey = Number(String(form.get('wagerKey')));
		const note = String(form.get('resolutionNote') ?? '').trim();
		if (Number.isNaN(wagerKey)) return fail(400, { error: 'Which bet?' });

		const flagged = await db
			.update(wager)
			.set({
				status: 'pending_resolution',
				resolutionRequestedBy: managerKey,
				resolutionRequestedAt: new Date(),
				resolutionNote: note ? note.slice(0, MAX_NOTE) : null,
				updatedAt: new Date()
			})
			.where(
				and(
					eq(wager.wagerKey, wagerKey),
					eq(wager.status, 'accepted'),
					// Either side may flag it, nobody else.
					sql`(${wager.proposedBy} = ${managerKey} or ${wager.acceptedBy} = ${managerKey})`
				)
			)
			.returning({ wagerKey: wager.wagerKey });

		if (flagged.length === 0) {
			return fail(409, { error: "That's not your bet, or it isn't live." });
		}

		return { success: true };
	},

	/**
	 * The commissioner's ruling — the only way a bet gets an outcome.
	 *
	 * Accepts a live bet as well as one already flagged: if the commissioner knows
	 * it is over, there is no reason to make him wait for one of the two to say so.
	 */
	resolve: async ({ request, locals }) => {
		const commissionerKey = requireCommissioner(locals);
		const form = await request.formData();
		const wagerKey = Number(String(form.get('wagerKey')));
		const outcome = String(form.get('outcome') ?? '');
		const rulingNote = String(form.get('rulingNote') ?? '').trim();

		if (Number.isNaN(wagerKey)) return fail(400, { error: 'Which bet?' });
		if (!isWagerOutcome(outcome)) return fail(400, { error: 'Say who took it.' });

		const [existing] = await db
			.select({
				proposedBy: wager.proposedBy,
				acceptedBy: wager.acceptedBy,
				status: wager.status
			})
			.from(wager)
			.where(eq(wager.wagerKey, wagerKey))
			.limit(1);

		if (!existing) return fail(404, { error: 'That bet no longer exists.' });
		if (existing.status !== 'accepted' && existing.status !== 'pending_resolution') {
			return fail(409, { error: `That bet is ${existing.status.replace('_', ' ')}.` });
		}

		const winnerKey =
			outcome === 'proposer'
				? existing.proposedBy
				: outcome === 'taker'
					? existing.acceptedBy
					: null;

		// Conditional on the status, like every other action here. The read above
		// is not a guard: two stale tabs, or a double-click on a button with no
		// disabled state, both pass it and both write, and the second silently
		// overwrites a ruling that has already been recorded.
		const ruled = await db
			.update(wager)
			.set({
				outcome,
				winnerKey,
				rulingNote: rulingNote ? rulingNote.slice(0, MAX_NOTE) : null,
				resolvedBy: commissionerKey,
				resolvedAt: new Date(),
				status: outcome === 'void' ? 'void' : 'settled',
				updatedAt: new Date()
			})
			.where(
				and(eq(wager.wagerKey, wagerKey), inArray(wager.status, ['accepted', 'pending_resolution']))
			)
			.returning({ wagerKey: wager.wagerKey });

		if (ruled.length === 0) {
			return fail(409, { error: 'That bet has already been ruled on.' });
		}

		return { success: true };
	}
};
