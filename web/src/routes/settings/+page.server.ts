import type { ServerLoad } from '@sveltejs/kit';
import { validateSessionToken } from '$lib/server/auth';

export const load: ServerLoad = async ({ cookies }) => {
	const sessionToken = cookies.get('session_token');
	
	if (!sessionToken) {
		return {
			user: null
		};
	}

	const { user } = await validateSessionToken(sessionToken);
	
	return {
		user
	};
}; 