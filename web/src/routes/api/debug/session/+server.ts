import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async ({ locals, cookies, request }) => {
	const sessionCookie = cookies.get('auth-session');
	const allCookies = cookies.getAll();
	const cookieHeader = request.headers.get('cookie');
	
	console.log('🍪 Debug - Cookie header:', cookieHeader);
	console.log('🍪 Debug - All cookies:', allCookies);
	console.log('🍪 Debug - Session cookie:', sessionCookie);
	
	return json({
		hasUser: !!locals.user,
		hasSession: !!locals.session,
		hasCookie: !!sessionCookie,
		user: locals.user ? {
			id: locals.user.id,
			username: locals.user.username
		} : null,
		sessionExists: !!locals.session,
		cookieValue: sessionCookie ? 'present' : 'missing',
		debug: {
			cookieHeader,
			allCookies,
			sessionCookie
		}
	});
}; 