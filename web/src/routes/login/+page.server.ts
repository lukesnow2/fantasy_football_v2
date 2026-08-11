import { fail, redirect } from '@sveltejs/kit';
import { requireOrigin } from '$lib/server/env';
import { emailService, generateMagicLinkEmail } from '$lib/server/email';
import { issueLoginToken } from '$lib/server/login-token';
import { findMemberByEmail } from '$lib/server/members';
import { consumeLoginEmailBudget, consumeLoginIpBudget } from '$lib/server/rate-limit';
import { safeRedirect } from '$lib/server/redirects';
import type { Actions, PageServerLoad } from './$types';

/**
 * Where every sign-in attempt lands, successful or not.
 *
 * The refusal path and the success path must be indistinguishable from outside.
 * If an address that is not on the roster produced an error, or a different
 * redirect, anyone could enumerate the league by trying addresses.
 */
const CHECK_EMAIL = '/login/check-email';


export const load: PageServerLoad = async ({ locals, url }) => {
	if (locals.user && locals.member?.active) {
		throw redirect(302, safeRedirect(url.searchParams.get('redirect')) ?? '/');
	}

	return {
		// Present so the page can explain why a signed-in user is still here.
		membershipRevoked: !!locals.user && !locals.member?.active
	};
};

export const actions: Actions = {
	default: async ({ request, url, getClientAddress }) => {
		const form = await request.formData();
		const rawEmail = form.get('email');

		if (typeof rawEmail !== 'string' || !rawEmail.trim()) {
			return fail(400, { error: 'Enter your email address.' });
		}

		const email = rawEmail.toLowerCase().trim();
		const redirectTo = safeRedirect(url.searchParams.get('redirect'));

		// Everything below this line must reach the same redirect, whatever
		// happens, so that a caller learns nothing about who is on the roster.
		//
		// This equalizes *content*, not *latency*: the success path additionally
		// awaits a token insert and a Resend call, so a determined attacker timing
		// responses can still distinguish them. Closing that would mean deferring
		// the send to a queue we do not have. Judged acceptable for a ten-person
		// league; noted so nobody mistakes this for constant time.
		const ip = getClientAddress();
		const withinIpBudget = consumeLoginIpBudget(ip);
		const withinEmailBudget = consumeLoginEmailBudget(email);
		const member = await findMemberByEmail(email);

		if (!withinIpBudget || !withinEmailBudget || !member?.active) {
			throw redirect(303, CHECK_EMAIL);
		}

		const { token } = await issueLoginToken({
			email: member.email,
			purpose: 'login',
			redirectTo,
			ip,
			userAgent: request.headers.get('user-agent')
		});

		const link = `${requireOrigin()}/login/verify?token=${encodeURIComponent(token)}`;
		const sent = await emailService.sendEmail(
			generateMagicLinkEmail(link, member.email, {
				purpose: 'login',
				displayName: member.displayName
			})
		);

		// Log rather than throw. Turning a provider outage into an error page
		// would mean provisioned addresses fail loudly while unknown ones redirect
		// quietly — which reopens exactly the oracle this flow closes.
		if (!sent) {
			console.error(`[login] Could not deliver a sign-in link to ${member.email}`);
		}

		throw redirect(303, CHECK_EMAIL);
	}
};
