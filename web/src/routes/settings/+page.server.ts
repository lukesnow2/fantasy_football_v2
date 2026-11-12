import { fail, redirect } from '@sveltejs/kit';
import {
	getAuthenticatedManager,
	updateNotificationPreferences
} from '$lib/server/auth-manager';
import type { Actions, PageServerLoad } from './$types';

export const load: PageServerLoad = async ({ locals }) => {
	// Previously read a cookie named `session_token`, which nothing in the
	// codebase has ever set — so this page always saw a signed-out user. The
	// session is already resolved once per request in hooks.server.ts.
	if (!locals.user) throw redirect(302, '/login?redirect=/settings');

	const manager = await getAuthenticatedManager(locals.user);

	return {
		manager,
		role: locals.member?.role ?? 'member'
	};
};

export const actions: Actions = {
	notifications: async ({ request, locals }) => {
		if (!locals.user) throw redirect(302, '/login?redirect=/settings');

		const form = await request.formData();
		const saved = await updateNotificationPreferences(locals.user.id, {
			emailOnNewProposal: form.get('emailOnNewProposal') === 'on',
			emailOnVoteResults: form.get('emailOnVoteResults') === 'on',
			emailOnTradeOffers: form.get('emailOnTradeOffers') === 'on'
		});

		if (!saved) return fail(500, { error: "Couldn't save your notification settings." });
		return { success: true };
	}
};
