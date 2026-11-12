import { getAuthenticatedManager } from '$lib/server/auth-manager';
import type { LayoutServerLoad } from './$types';

export const load: LayoutServerLoad = async (event) => {
	const user = event.locals.user;
	let authenticatedManager = null;
	
	if (user) {
		authenticatedManager = await getAuthenticatedManager(user);
	}
	
	return {
		user: user ? {
			id: user.id,
			username: user.username
		} : null,
		// Exposed so the nav can show commissioner-only links. Authorisation is
		// enforced server-side in hooks.server.ts and each admin load — this is
		// only for deciding what to render.
		member: event.locals.member,
		authenticatedManager
	};
}; 