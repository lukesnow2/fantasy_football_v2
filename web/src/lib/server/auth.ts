import type { RequestEvent } from '@sveltejs/kit';
import { eq } from 'drizzle-orm';
import { sha256 } from '@oslojs/crypto/sha2';
import { encodeBase64url, encodeHexLowerCase } from '@oslojs/encoding';
import { db, type Tx } from '$lib/server/db';
import * as table from '$lib/server/db/schema';
import { dev } from '$app/environment';

const DAY_IN_MS = 1000 * 60 * 60 * 24;

export const sessionCookieName = 'auth-session';

export function generateSessionToken() {
	const bytes = crypto.getRandomValues(new Uint8Array(18));
	const token = encodeBase64url(bytes);
	return token;
}

/**
 * Takes an optional `Tx` so a caller can create the session in the same
 * transaction that consumed the login token — the token must not be burned
 * unless the session it authorises is durably written, and vice versa.
 */
export async function createSession(token: string, userId: string, tx: Tx = db) {
	const sessionId = encodeHexLowerCase(sha256(new TextEncoder().encode(token)));
	const session = {
		id: sessionId,
		userId,
		expiresAt: new Date(Date.now() + DAY_IN_MS * 30)
	} satisfies typeof table.session.$inferInsert;
	await tx.insert(table.session).values(session);
	return session;
}

export async function validateSessionToken(token: string) {
	const sessionId = encodeHexLowerCase(sha256(new TextEncoder().encode(token)));

	// Joins the allowlist so every request has the caller's manager key and role
	// without a second query. LEFT join, not INNER: a user whose league_member
	// row was removed still resolves as a logged-in user, but with member === null,
	// so route guards refuse them rather than crashing.
	const [result] = await db
		.select({
			// Adjust user table here to tweak returned data
			user: { id: table.user.id, username: table.user.username },
			session: table.session,
			member: {
				managerKey: table.leagueMember.managerKey,
				role: table.leagueMember.role,
				active: table.leagueMember.active
			}
		})
		.from(table.session)
		.innerJoin(table.user, eq(table.session.userId, table.user.id))
		.leftJoin(table.leagueMember, eq(table.user.managerKey, table.leagueMember.managerKey))
		.where(eq(table.session.id, sessionId));

	if (!result) {
		return { session: null, user: null, member: null };
	}
	const { session, user, member } = result;

	const sessionExpired = Date.now() >= session.expiresAt.getTime();
	if (sessionExpired) {
		await db.delete(table.session).where(eq(table.session.id, session.id));
		return { session: null, user: null, member: null };
	}

	const renewSession = Date.now() >= session.expiresAt.getTime() - DAY_IN_MS * 15;
	if (renewSession) {
		session.expiresAt = new Date(Date.now() + DAY_IN_MS * 30);
		await db
			.update(table.session)
			.set({ expiresAt: session.expiresAt })
			.where(eq(table.session.id, session.id));
	}

	return {
		session,
		user,
		member: member?.managerKey != null ? (member as App.SessionMember) : null
	};
}

export type SessionValidationResult = Awaited<ReturnType<typeof validateSessionToken>>;

export async function invalidateSession(sessionId: string) {
	await db.delete(table.session).where(eq(table.session.id, sessionId));
}

/**
 * Drop every session belonging to a user.
 *
 * Deactivating a member blocks future logins but says nothing about the 30-day
 * session they may already be holding, so revocation has to reach in and delete
 * them or it is only prospective.
 */
export async function invalidateUserSessions(userId: string, tx: Tx = db) {
	await tx.delete(table.session).where(eq(table.session.userId, userId));
}

export function setSessionTokenCookie(event: RequestEvent, token: string, expiresAt: Date) {
	event.cookies.set(sessionCookieName, token, {
		expires: expiresAt,
		path: '/',
		httpOnly: true,
		secure: !dev, // Use secure cookies in production
		sameSite: 'lax' // Protect against CSRF while allowing navigation
	});
}

export function deleteSessionTokenCookie(event: RequestEvent) {
	event.cookies.delete(sessionCookieName, {
		path: '/',
		httpOnly: true,
		secure: !dev,
		sameSite: 'lax'
	});
}
