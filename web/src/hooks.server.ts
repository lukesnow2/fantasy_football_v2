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
/**
 * Public for every method, not just reads.
 *
 * The sign-in flow is the one place an unauthenticated POST is the entire
 * point: `/login` is the form that mails the link, and requiring a session to
 * reach it means nobody can ever obtain one. Kept as its own list so the
 * write-gating rule below can stay strict everywhere else.
 */
const PUBLIC_WRITE_PREFIXES = ['/login', '/logout'];

const PUBLIC_PREFIXES = [
	'/login',
	'/logout',
	'/this-season',
	'/historical',
	'/hall-of-fame',
	'/managers',
	'/power-rankings',
	'/trades',
	'/draft',
	'/data-dictionary',
	'/constitution', // the text is public; proposing and voting are form actions, gated below
	'/api/overview',
	'/api/standings',
	'/api/managers',
	'/api/power-rankings',
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

/**
 * Does this request require a signed-in, active member?
 *
 * Exported so it can be unit-tested without a server — this predicate is the
 * whole of the access-control policy for routes, and the deny-by-default claim
 * above is only worth anything if something pins it.
 *
 * Genuinely deny-by-default in every direction:
 *  - a route not named in PUBLIC_PREFIXES is private, API or page
 *  - a write is private unless the whole prefix is public AND it is a read
 * An earlier version only applied the public-list check to page routes, so an
 * unlisted `GET /api/anything` fell through every clause and was served to
 * anyone — the exact class of hole this file exists to close.
 */
export function requiresAuth(pathname: string, method: string): boolean {
	const isRead = method === 'GET' || method === 'HEAD';

	// Explicitly private, whatever the method — ballots are not public reads.
	if (matchesPrefix(pathname, PRIVATE_API_PREFIXES)) return true;

	// Anything not on the public list is private.
	if (!isPublicPath(pathname)) return true;

	// The sign-in flow accepts unauthenticated writes; nothing else does.
	if (matchesPrefix(pathname, PUBLIC_WRITE_PREFIXES)) return false;

	// Public prefixes are otherwise public to *read* only. A form action posting
	// to a public page (e.g. POST /constitution?/vote) still needs a session.
	return !isRead;
}

const handleProtect: Handle = async ({ event, resolve }) => {
	const { pathname } = event.url;
	const isApi = pathname.startsWith('/api/');

	// A signed-in user whose membership was revoked is treated as signed out for
	// authorisation purposes. `locals.user` stays populated so the UI can tell
	// them why rather than silently bouncing them to a login form.
	const authorized = !!event.locals.user && !!event.locals.member?.active;

	const needsAuth = requiresAuth(pathname, event.request.method);

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
