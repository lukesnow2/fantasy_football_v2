import { redirect } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { createSession, generateSessionToken, setSessionTokenCookie } from '$lib/server/auth';
import { consumeLoginToken } from '$lib/server/login-token';
import { findMemberByEmail, getOrCreateUserForMember, markFirstLogin } from '$lib/server/members';
import { safeRedirect } from '$lib/server/redirects';
import type { PageServerLoad } from './$types';

/**
 * Redeem on GET.
 *
 * The tradeoff: a mail gateway that prefetches links (Outlook SafeLinks and
 * similar) will burn the token before the human clicks, and they will see
 * "already used". For ten managers on consumer mail that is rare, and a fresh
 * link is fifteen seconds away.
 *
 * Kept behind a flag so switching to a POST interstitial — which fixes
 * prefetching at the cost of an extra click for everyone — stays a one-line
 * change rather than a rewrite of this file.
 */
const CONSUME_ON_GET = true;

type Failure = 'not_found' | 'expired' | 'consumed' | 'not_allowed' | 'missing';

export const load: PageServerLoad = async (event) => {
	const token = event.url.searchParams.get('token');

	if (!token) return { failed: 'missing' satisfies Failure };
	if (!CONSUME_ON_GET) return { failed: 'missing' satisfies Failure };

	const now = new Date();

	const outcome = await db.transaction(async (tx) => {
		const consumed = await consumeLoginToken(tx, token, now);
		if (!consumed.ok) return { failed: consumed.reason satisfies Failure };

		// Re-check the allowlist inside the transaction rather than trusting the
		// membership that existed when the link was mailed. A commissioner who
		// deactivates someone must not be undone by a link already sitting in that
		// person's inbox.
		const member = await findMemberByEmail(consumed.row.email, tx);
		if (!member?.active) return { failed: 'not_allowed' satisfies Failure };

		const { userId } = await getOrCreateUserForMember(tx, member);
		await markFirstLogin(tx, member.id, now);

		const sessionToken = generateSessionToken();
		const session = await createSession(sessionToken, userId, tx);

		return { sessionToken, session, redirectTo: consumed.row.redirectTo };
	});

	if ('failed' in outcome) return { failed: outcome.failed };

	// Set outside the transaction: the cookie is only meaningful once the session
	// row has actually committed.
	setSessionTokenCookie(event, outcome.sessionToken, outcome.session.expiresAt);

	throw redirect(303, safeRedirect(outcome.redirectTo) ?? '/');
};
