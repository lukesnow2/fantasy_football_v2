import type { RequestEvent } from '@sveltejs/kit';
import { eq, and, lt } from 'drizzle-orm';
import { sha256 } from '@oslojs/crypto/sha2';
import { encodeBase64url, encodeHexLowerCase } from '@oslojs/encoding';
import { db } from '$lib/server/db';
import { session as sessionTable, user as userTable } from '$lib/server/db/schema';
import { dev } from '$app/environment';

const SESSION_DURATION_MS = 1000 * 60 * 60 * 24 * 30; // 30 days
const SESSION_RENEWAL_THRESHOLD_MS = 1000 * 60 * 60 * 24 * 15; // 15 days
const MAX_CONCURRENT_SESSIONS = 5; // Limit sessions per user

export interface WebAuthnSession {
	id: string;
	userId: string;
	expiresAt: Date;
	createdAt: Date;
	lastUsedAt: Date;
	userAgent?: string | null;
	ipAddress?: string | null;
	deviceType?: string | null;
}

export function generateWebAuthnSessionToken(): string {
	const bytes = crypto.getRandomValues(new Uint8Array(32)); // Increased entropy
	return encodeBase64url(bytes);
}

export async function createWebAuthnSession(
	token: string, 
	userId: string, 
	metadata?: { userAgent?: string; ipAddress?: string; deviceType?: string }
): Promise<WebAuthnSession> {
	const sessionId = encodeHexLowerCase(sha256(new TextEncoder().encode(token)));
	const now = new Date();
	const expiresAt = new Date(now.getTime() + SESSION_DURATION_MS);

	// Check for existing sessions and enforce limits
	const existingSessions = await db
		.select({ id: sessionTable.id })
		.from(sessionTable)
		.where(eq(sessionTable.userId, userId));

	if (existingSessions.length >= MAX_CONCURRENT_SESSIONS) {
		// Remove oldest session
		const oldestSession = await db
			.select({ id: sessionTable.id, createdAt: sessionTable.createdAt })
			.from(sessionTable)
			.where(eq(sessionTable.userId, userId))
			.orderBy(sessionTable.createdAt)
			.limit(1);

		if (oldestSession.length > 0) {
			await db.delete(sessionTable).where(eq(sessionTable.id, oldestSession[0].id));
		}
	}

	const session: WebAuthnSession = {
		id: sessionId,
		userId,
		expiresAt,
		createdAt: now,
		lastUsedAt: now,
		userAgent: metadata?.userAgent,
		ipAddress: metadata?.ipAddress,
		deviceType: metadata?.deviceType
	};

	await db.insert(sessionTable).values({
		id: sessionId,
		userId,
		expiresAt,
		createdAt: now,
		lastUsedAt: now,
		userAgent: metadata?.userAgent,
		ipAddress: metadata?.ipAddress,
		deviceType: metadata?.deviceType
	});

	return session;
}

export async function validateWebAuthnSession(token: string): Promise<{
	session: WebAuthnSession | null;
	user: { id: string; username: string } | null;
}> {
	const sessionId = encodeHexLowerCase(sha256(new TextEncoder().encode(token)));
	
	const [result] = await db
		.select({
			session: sessionTable,
			user: { id: userTable.id, username: userTable.username }
		})
		.from(sessionTable)
		.innerJoin(userTable, eq(sessionTable.userId, userTable.id))
		.where(eq(sessionTable.id, sessionId));

	if (!result) {
		return { session: null, user: null };
	}

	const { session, user } = result;
	const now = new Date();

	// Check if session is expired
	if (now >= session.expiresAt) {
		await db.delete(sessionTable).where(eq(sessionTable.id, session.id));
		return { session: null, user: null };
	}

	// Update last used timestamp
	await db
		.update(sessionTable)
		.set({ lastUsedAt: now })
		.where(eq(sessionTable.id, session.id));

	// Auto-renew session if approaching expiration
	const renewalThreshold = new Date(session.expiresAt.getTime() - SESSION_RENEWAL_THRESHOLD_MS);
	if (now >= renewalThreshold) {
		const newExpiresAt = new Date(now.getTime() + SESSION_DURATION_MS);
		await db
			.update(sessionTable)
			.set({ expiresAt: newExpiresAt })
			.where(eq(sessionTable.id, session.id));
		
		session.expiresAt = newExpiresAt;
	}

	// Convert database session to WebAuthnSession interface
	const webAuthnSession: WebAuthnSession = {
		id: session.id,
		userId: session.userId,
		expiresAt: session.expiresAt,
		createdAt: session.createdAt || now,
		lastUsedAt: session.lastUsedAt || now,
		userAgent: session.userAgent,
		ipAddress: session.ipAddress,
		deviceType: session.deviceType
	};

	return { session: webAuthnSession, user };
}

export async function invalidateWebAuthnSession(sessionId: string): Promise<void> {
	await db.delete(sessionTable).where(eq(sessionTable.id, sessionId));
}

export async function invalidateAllUserSessions(userId: string): Promise<void> {
	await db.delete(sessionTable).where(eq(sessionTable.userId, userId));
}

export async function cleanupExpiredSessions(): Promise<number> {
	await db
		.delete(sessionTable)
		.where(lt(sessionTable.expiresAt, new Date()));
	
	// Drizzle doesn't return row count, so we'll return 0
	// In a real implementation, you might want to track this separately
	return 0;
}

export function setWebAuthnSessionCookie(
	event: RequestEvent, 
	token: string, 
	expiresAt: Date
): void {
	event.cookies.set('webauthn-session', token, {
		expires: expiresAt,
		path: '/',
		httpOnly: true,
		secure: !dev,
		sameSite: 'lax',
		maxAge: SESSION_DURATION_MS / 1000
	});
}

export function deleteWebAuthnSessionCookie(event: RequestEvent): void {
	event.cookies.delete('webauthn-session', {
		path: '/',
		httpOnly: true,
		secure: !dev,
		sameSite: 'lax'
	});
}

export async function getActiveSessionsForUser(userId: string): Promise<WebAuthnSession[]> {
	const sessions = await db
		.select()
		.from(sessionTable)
		.where(and(
			eq(sessionTable.userId, userId),
			lt(sessionTable.expiresAt, new Date())
		))
		.orderBy(sessionTable.lastUsedAt);

	return sessions.map(s => ({
		id: s.id,
		userId: s.userId,
		expiresAt: s.expiresAt,
		createdAt: s.createdAt || new Date(),
		lastUsedAt: s.lastUsedAt || new Date(),
		userAgent: s.userAgent,
		ipAddress: s.ipAddress,
		deviceType: s.deviceType
	}));
} 