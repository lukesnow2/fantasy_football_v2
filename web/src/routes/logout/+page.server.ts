import { redirect } from '@sveltejs/kit';
import * as auth from '$lib/server/auth';
import type { Actions, PageServerLoad } from './$types';

export const load: PageServerLoad = async () => {
	// Redirect to login if someone visits /logout directly
	return redirect(302, '/login');
};

export const actions: Actions = {
	default: async (event) => {
		// Invalidate session if user is logged in
		if (event.locals.session) {
			await auth.invalidateSession(event.locals.session.id);
		}
		
		// Delete session cookie
		auth.deleteSessionTokenCookie(event);
		
		return redirect(302, '/login');
	}
}; 