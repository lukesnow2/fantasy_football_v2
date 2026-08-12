import { error, fail } from '@sveltejs/kit';
import { asc, eq } from 'drizzle-orm';
import { nanoid } from 'nanoid';
import { db } from '$lib/server/db';
import { dimManager, leagueMember, user as userTable } from '$lib/server/db/schema';
import { invalidateUserSessions } from '$lib/server/auth';
import { requireCommissioner, getUnclaimedManagers } from '$lib/server/auth-manager';
import { emailService, generateMagicLinkEmail } from '$lib/server/email';
import { describeMailConfig, requireOrigin } from '$lib/server/env';
import { issueLoginToken } from '$lib/server/login-token';
import { consumeLoginEmailBudget } from '$lib/server/rate-limit';
import type { Actions, PageServerLoad } from './$types';

export const load: PageServerLoad = async ({ locals }) => {
	// Checked here as well as in hooks.server.ts. A guard you cannot see from the
	// route file is a guard someone later removes by accident.
	requireCommissioner(locals);

	const members = await db
		.select({
			id: leagueMember.id,
			email: leagueMember.email,
			managerKey: leagueMember.managerKey,
			role: leagueMember.role,
			active: leagueMember.active,
			displayName: leagueMember.displayName,
			invitedAt: leagueMember.invitedAt,
			firstLoginAt: leagueMember.firstLoginAt
		})
		.from(leagueMember)
		.orderBy(asc(leagueMember.displayName));

	return {
		members,
		// Current managers with no allowlist row yet — the "who is still missing"
		// list, so an unseeded manager is visible rather than silently absent.
		unclaimed: await getUnclaimedManagers(),
		// Every mail misconfiguration is otherwise invisible: a missing key falls
		// back to console mode and a missing EMAIL_FROM is refused by the
		// provider, both reported only to a serverless log nobody reads. The
		// commissioner is the one person who looks at this page and can fix it.
		mailConfig: describeMailConfig()
	};
};

/**
 * Emails are unique case-insensitively (see the lower(email) index in the
 * migration), so 23505 on any of these writes means "someone already has that
 * address" and nothing else. Anything else is a real failure and must not be
 * reported as a duplicate.
 */
function isDuplicateEmail(err: unknown): boolean {
	return (err as { code?: string })?.code === '23505';
}

function normalizeEmail(raw: FormDataEntryValue | null): string {
	return String(raw ?? '')
		.toLowerCase()
		.trim();
}

function looksLikeEmail(value: string): boolean {
	// Deliberately loose. The authoritative test is whether the sign-in link
	// arrives; a strict regex here only rejects addresses that actually work.
	return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
}

export const actions: Actions = {
	/**
	 * Put a manager on the allowlist.
	 *
	 * Nine of the ten managers had no league_member row at all, because the seed
	 * script skips entries with a blank email and nine of them were blank. That
	 * left the commissioner with a banner naming who was missing and no way to do
	 * anything about it short of editing JSON and re-running a script.
	 */
	addMember: async ({ request, locals }) => {
		requireCommissioner(locals);
		const form = await request.formData();
		const managerKey = Number(form.get('managerKey'));
		const email = normalizeEmail(form.get('email'));
		const role = form.get('role') === 'commissioner' ? 'commissioner' : 'member';

		if (!Number.isInteger(managerKey)) return fail(400, { error: 'Pick a manager.' });
		if (!looksLikeEmail(email)) return fail(400, { error: 'Enter a valid email address.' });

		const [manager] = await db
			.select({ managerKey: dimManager.managerKey, managerName: dimManager.managerName })
			.from(dimManager)
			.where(eq(dimManager.managerKey, managerKey))
			.limit(1);

		if (!manager) return fail(404, { error: 'That manager is not in the warehouse.' });

		// manager_key is unique on league_member, so this is also enforced by the
		// database. Checked here so the commissioner gets a sentence rather than a
		// constraint violation.
		const [existing] = await db
			.select({ id: leagueMember.id })
			.from(leagueMember)
			.where(eq(leagueMember.managerKey, managerKey))
			.limit(1);

		if (existing) return fail(409, { error: `${manager.managerName} is already on the allowlist.` });

		try {
			await db.insert(leagueMember).values({
				id: nanoid(),
				email,
				managerKey,
				role,
				active: true,
				displayName: manager.managerName
			});
		} catch (err) {
			if (isDuplicateEmail(err)) {
				return fail(409, { error: 'Another manager already uses that address.' });
			}
			console.error('[admin] Failed to add member:', err);
			return fail(500, { error: "Couldn't add that manager. Check the logs." });
		}

		// Deliberately not auto-inviting. The invite action reports send failures
		// honestly and stamps invitedAt only on a confirmed send; folding it in here
		// would make one button either lie about the mail or fail the whole add
		// because the mail bounced.
		return {
			success: `${manager.managerName} added as ${role}. Send them an invite when you're ready.`
		};
	},

	/**
	 * Promote or demote. Guarded by the same last-commissioner rule as setActive —
	 * a league with no commissioner has no way back into this page.
	 */
	setRole: async ({ request, locals }) => {
		requireCommissioner(locals);
		const form = await request.formData();
		const memberId = String(form.get('memberId') ?? '');
		const role = form.get('role') === 'commissioner' ? 'commissioner' : 'member';

		const [member] = await db
			.select()
			.from(leagueMember)
			.where(eq(leagueMember.id, memberId))
			.limit(1);

		if (!member) return fail(404, { error: 'Member not found.' });
		if (member.role === role) return { success: `${member.displayName} is already a ${role}.` };

		if (role === 'member' && member.role === 'commissioner') {
			const commissioners = await db
				.select({ id: leagueMember.id })
				.from(leagueMember)
				.where(eq(leagueMember.role, 'commissioner'));

			if (commissioners.filter((c) => c.id !== memberId).length === 0) {
				return fail(409, {
					error: 'That would leave the league with no commissioner. Promote someone else first.'
				});
			}
		}

		await db
			.update(leagueMember)
			.set({ role, updatedAt: new Date() })
			.where(eq(leagueMember.id, memberId));

		return { success: `${member.displayName ?? member.email} is now a ${role}.` };
	},

	invite: async ({ request, locals }) => {
		requireCommissioner(locals);
		const memberId = String((await request.formData()).get('memberId') ?? '');

		const [member] = await db
			.select()
			.from(leagueMember)
			.where(eq(leagueMember.id, memberId))
			.limit(1);

		if (!member) return fail(404, { error: 'Member not found.' });
		if (!member.active) return fail(409, { error: 'Reactivate them before inviting.' });
		if (!consumeLoginEmailBudget(member.email)) {
			return fail(429, { error: 'Too many invites to that address just now. Try again shortly.' });
		}

		const { token } = await issueLoginToken({
			email: member.email,
			purpose: 'invite',
			redirectTo: null,
			ip: null,
			userAgent: null
		});

		const sent = await emailService.sendEmail(
			generateMagicLinkEmail(`${requireOrigin()}/login/verify?token=${encodeURIComponent(token)}`, member.email, {
				purpose: 'invite',
				displayName: member.displayName
			})
		);

		// Reported honestly to the commissioner. Unlike the login form there is no
		// oracle to protect here — they already know who is on the roster — and
		// silently "succeeding" on a failed send is how ten people end up unable to
		// log in with nobody knowing why.
		// Says "could not be confirmed", not "was not accepted". With an 8s timeout
		// on the send, a failure here can mean the provider accepted the message
		// and we stopped waiting — so the mail may well arrive. invitedAt stays
		// unstamped in that case, which is the safe direction: re-inviting issues
		// a fresh token and both links work.
		if (!sent) {
			return fail(502, {
				error: `The invite to ${member.email} could not be confirmed — the mail provider did not accept it, or did not answer in time. Check that the Resend domain is verified, then try again.`
			});
		}

		// Stamped only after a confirmed send. Recording the invite first made the
		// roster show a manager as invited when the mail never left.
		await db
			.update(leagueMember)
			.set({ invitedAt: new Date(), updatedAt: new Date() })
			.where(eq(leagueMember.id, memberId));

		return { success: `Invite sent to ${member.email}.` };
	},

	setActive: async ({ request, locals }) => {
		requireCommissioner(locals);
		const form = await request.formData();
		const memberId = String(form.get('memberId') ?? '');
		const active = form.get('active') === 'true';

		const [member] = await db
			.select()
			.from(leagueMember)
			.where(eq(leagueMember.id, memberId))
			.limit(1);

		if (!member) return fail(404, { error: 'Member not found.' });

		// Don't let the last commissioner remove their own access — that locks
		// everyone out of the admin surface with no way back in.
		if (!active && member.role === 'commissioner') {
			const commissioners = await db
				.select({ id: leagueMember.id })
				.from(leagueMember)
				.where(eq(leagueMember.role, 'commissioner'));

			if (commissioners.filter((c) => c.id !== memberId).length === 0) {
				return fail(409, {
					error: 'That would leave the league with no commissioner. Promote someone else first.'
				});
			}
		}

		await db
			.update(leagueMember)
			.set({ active, updatedAt: new Date() })
			.where(eq(leagueMember.id, memberId));

		if (!active) {
			// Deactivating blocks future logins but says nothing about a 30-day
			// session already in a browser. Revocation has to reach in and kill it,
			// or it is only prospective.
			const [linked] = await db
				.select({ id: userTable.id })
				.from(userTable)
				.where(eq(userTable.managerKey, member.managerKey))
				.limit(1);

			if (linked) await invalidateUserSessions(linked.id);
		}

		return {
			success: `${member.displayName ?? member.email} ${active ? 'reactivated' : 'deactivated and signed out'}.`
		};
	},

	updateEmail: async ({ request, locals }) => {
		requireCommissioner(locals);
		const form = await request.formData();
		const memberId = String(form.get('memberId') ?? '');
		const email = normalizeEmail(form.get('email'));

		if (!looksLikeEmail(email)) return fail(400, { error: 'Enter a valid email address.' });

		let updated;
		try {
			[updated] = await db
				.update(leagueMember)
				.set({ email, updatedAt: new Date() })
				.where(eq(leagueMember.id, memberId))
				.returning({ id: leagueMember.id });
		} catch (error) {
			// Only 23505 is a duplicate address. A bare catch reported connection
			// loss, timeouts and constraint violations all as "already in use",
			// and logged none of them.
			if (isDuplicateEmail(error)) {
				return fail(409, { error: 'Another manager already uses that address.' });
			}
			console.error('[admin] Failed to update member email:', error);
			return fail(500, { error: "Couldn't update that address. Check the logs." });
		}

		// A zero-row update is not a success. Without this, a stale memberId
		// reported "Address updated" having changed nothing.
		if (!updated) return fail(404, { error: 'Member not found.' });

		return { success: `Address updated to ${email}.` };
	}
};
