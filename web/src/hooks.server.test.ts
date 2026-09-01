import { describe, expect, it } from 'vitest';
import { requiresAuth } from './hooks.server';

/**
 * Pins the deny-by-default contract the route guard claims.
 *
 * The case that matters most is the first one: a route nobody has thought of
 * yet must be private. A previous version only consulted the public list for
 * page routes, so an unlisted `GET /api/...` matched no clause and was served
 * to anyone.
 */
describe('requiresAuth', () => {
	it('a route nobody has added yet is private — API included', () => {
		expect(requiresAuth('/api/not-yet-invented', 'GET')).toBe(true);
		expect(requiresAuth('/api/my-votes', 'GET')).toBe(true);
		expect(requiresAuth('/some-new-page', 'GET')).toBe(true);
	});

	it('keeps the public archive readable', () => {
		expect(requiresAuth('/', 'GET')).toBe(false);
		expect(requiresAuth('/hall-of-fame', 'GET')).toBe(false);
		expect(requiresAuth('/historical', 'GET')).toBe(false);
		expect(requiresAuth('/constitution', 'GET')).toBe(false);
		expect(requiresAuth('/api/standings', 'GET')).toBe(false);
		expect(requiresAuth('/api/record-book', 'GET')).toBe(false);
		expect(requiresAuth('/power-rankings', 'GET')).toBe(false);
		expect(requiresAuth('/api/power-rankings', 'GET')).toBe(false);
	});

	it('gates every write, including form actions on public pages', () => {
		// POST /constitution?/vote has pathname '/constitution' — public to read,
		// but voting is a write and must not ride in on the public prefix.
		expect(requiresAuth('/constitution', 'POST')).toBe(true);
		expect(requiresAuth('/api/standings', 'POST')).toBe(true);
		expect(requiresAuth('/api/standings', 'DELETE')).toBe(true);
	});

	it('gates ballots even on GET', () => {
		// Who voted which way, and their comments, are league-internal even though
		// the constitution text itself is public.
		expect(requiresAuth('/api/rule-votes', 'GET')).toBe(true);
		expect(requiresAuth('/api/rule-proposals', 'GET')).toBe(true);
		expect(requiresAuth('/api/chat/messages', 'GET')).toBe(true);
	});

	it('gates member-only surfaces', () => {
		expect(requiresAuth('/settings', 'GET')).toBe(true);
		expect(requiresAuth('/admin/members', 'GET')).toBe(true);
		// Chat is league-internal: the page is gated, not just the API behind it.
		// /this-season is public and embeds a chat preview, which is why the
		// preview must render a signed-out state rather than fetch and 401.
		expect(requiresAuth('/chat', 'GET')).toBe(true);
		// The bet board is a ledger of what managers owe each other. It stays off
		// the public archive, so it must never be added to PUBLIC_PREFIXES.
		expect(requiresAuth('/bets', 'GET')).toBe(true);
		expect(requiresAuth('/bets', 'POST')).toBe(true);
	});

	it('leaves the sign-in flow reachable, including its POST', () => {
		expect(requiresAuth('/login', 'GET')).toBe(false);
		// The load-bearing one. POST /login is the form that mails the link;
		// requiring a session to reach it means nobody can ever obtain one.
		// An earlier "gate every write" rule locked the whole league out here.
		expect(requiresAuth('/login', 'POST')).toBe(false);
		expect(requiresAuth('/login/verify', 'GET')).toBe(false);
		expect(requiresAuth('/login/check-email', 'GET')).toBe(false);
		expect(requiresAuth('/logout', 'POST')).toBe(false);
	});

	it('matches on path segments, not string prefixes', () => {
		// '/managers' is public; '/managers-secret' is a different route and must
		// not inherit that.
		expect(requiresAuth('/managers', 'GET')).toBe(false);
		expect(requiresAuth('/managers/luke-s', 'GET')).toBe(false);
		expect(requiresAuth('/managers-secret', 'GET')).toBe(true);
		expect(requiresAuth('/loginsomething', 'GET')).toBe(true);
	});
});
