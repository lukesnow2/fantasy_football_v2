import { sequence } from '@sveltejs/kit/hooks';
import { json, redirect, type Handle } from '@sveltejs/kit';
import * as auth from '$lib/server/auth';
import { paraglideMiddleware } from '$lib/paraglide/server';

const handleParaglide: Handle = ({ event, resolve }) =>
	paraglideMiddleware(event.request, ({ request, locale }) => {
		event.request = request;

		return resolve(event, {
			transformPageChunk: ({ html }) => html.replace('%paraglide.lang%', locale)
		});
	});

const handleAuth: Handle = async ({ event, resolve }) => {
	const sessionToken = event.cookies.get(auth.sessionCookieName);

	if (!sessionToken) {
		event.locals.user = null;
		event.locals.session = null;
		event.locals.member = null;
		return resolve(event);
	}

	const { session, user, member } = await auth.validateSessionToken(sessionToken);

	if (session) {
		auth.setSessionTokenCookie(event, sessionToken, session.expiresAt);
	} else {
		auth.deleteSessionTokenCookie(event);
	}

	event.locals.user = user;
	event.locals.session = session;
	event.locals.member = member;
	return resolve(event);
};

/**
 * Pages and read APIs anyone may see.
 *
 * The league's 20 years of history is a public archive — someone should be able
 * to send a friend a link to the record book. Everything that identifies a
 * living manager's *actions* (how they voted, what they said in chat) or that
 * changes state is gated below.
 *
 * Deny-by-default: a new route is private until it is named here.
 */
const PUBLIC_PREFIXES = [
	'/login',
	'/logout',
	'/this-season',
	'/historical',
	'/hall-of-fame',
	'/managers',
	'/trades',
	'/draft',
	'/data-dictionary',
	'/constitution', // the text is public; proposing and voting are form actions, gated below
	'/api/overview',
	'/api/standings',
	'/api/managers',
	'/api/hall-of-fame',
	'/api/head-to-head',
	'/api/record-book',
	'/api/draft',
	'/api/trades',
	'/api/transactions',
	'/api/meta-data',
	'/api/leagues'
];

/**
 * Read APIs that look public but are not.
 *
 * `/api/rule-votes` returns who voted which way, with their comments. That is
 * league-internal even though it is a GET, so it is listed here rather than
 * being covered by the write-only rule below.
 */
const PRIVATE_API_PREFIXES = ['/api/rule-votes', '/api/rule-proposals', '/api/chat'];

function isPublicPath(pathname: string): boolean {
	if (pathname === '/') return true;
	return PUBLIC_PREFIXES.some(
		(prefix) => pathname === prefix || pathname.startsWith(`${prefix}/`)
	);
}

function matchesPrefix(pathname: string, prefixes: string[]): boolean {
	return prefixes.some((prefix) => pathname === prefix || pathname.startsWith(`${prefix}/`));
}

const handleProtect: Handle = async ({ event, resolve }) => {
	const { pathname } = event.url;
	const isApi = pathname.startsWith('/api/');
	const isRead = event.request.method === 'GET' || event.request.method === 'HEAD';

	// A signed-in user whose membership was revoked is treated as signed out for
	// authorisation purposes. `locals.user` stays populated so the UI can tell
	// them why rather than silently bouncing them to a login form.
	const authorized = !!event.locals.user && !!event.locals.member?.active;

	const needsAuth =
		matchesPrefix(pathname, PRIVATE_API_PREFIXES) ||
		(isApi && !isRead) ||
		(!isApi && !isPublicPath(pathname));

	if (needsAuth && !authorized) {
		if (isApi) {
			return json(
				{ error: event.locals.user ? 'League membership required' : 'Authentication required' },
				{ status: event.locals.user ? 403 : 401 }
			);
		}
		throw redirect(302, `/login?redirect=${encodeURIComponent(pathname + event.url.search)}`);
	}

	// Commissioner-only surfaces. Checked here as a backstop; each admin route
	// also checks in its own `load`, because a guard nobody can see from the
	// route file is a guard someone will later remove by accident.
	if (pathname.startsWith('/admin') && event.locals.member?.role !== 'commissioner') {
		throw redirect(302, '/');
	}

	return resolve(event);
};

export const handle: Handle = sequence(handleParaglide, handleAuth, handleProtect);
