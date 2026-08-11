import { error, fail } from '@sveltejs/kit';
import { asc, eq } from 'drizzle-orm';
import { db } from '$lib/server/db';
import { leagueMember, user as userTable } from '$lib/server/db/schema';
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

export const actions: Actions = {
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
		const email = String(form.get('email') ?? '').toLowerCase().trim();

		if (!email || !email.includes('@')) return fail(400, { error: 'Enter a valid email address.' });

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
			const code = (error as { code?: string })?.code;
			if (code === '23505') {
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
