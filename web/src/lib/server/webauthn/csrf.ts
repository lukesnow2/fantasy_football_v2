import { randomBytes } from 'crypto';
import { encodeBase64url } from '@oslojs/encoding';
import { db } from '$lib/server/db';
import { webauthnChallenges } from '$lib/server/db/schema';
import { eq, lt } from 'drizzle-orm';

const CSRF_TOKEN_LENGTH = 32;
const CSRF_TOKEN_EXPIRY_MS = 1000 * 60 * 15; // 15 minutes

export interface CSRFToken {
	id: string;
	token: string;
	userId?: string;
	expiresAt: Date;
	createdAt: Date;
}

export function generateCSRFToken(): string {
	return encodeBase64url(randomBytes(CSRF_TOKEN_LENGTH));
}

export async function createCSRFToken(userId?: string): Promise<CSRFToken> {
	const token = generateCSRFToken();
	const now = new Date();
	const expiresAt = new Date(now.getTime() + CSRF_TOKEN_EXPIRY_MS);

	const csrfToken: CSRFToken = {
		id: encodeBase64url(randomBytes(16)),
		token,
		userId,
		expiresAt,
		createdAt: now
	};

	// Store in challenges table (reusing existing infrastructure)
	await db.insert(webauthnChallenges).values({
		id: csrfToken.id,
		challenge: token,
		userId: userId || null,
		type: 'csrf',
		expiresAt
	});

	return csrfToken;
}

export async function validateCSRFToken(token: string, userId?: string): Promise<boolean> {
	const now = new Date();

	// Find the CSRF token
	const [result] = await db
		.select()
		.from(webauthnChallenges)
		.where(eq(webauthnChallenges.challenge, token));

	if (!result) {
		return false;
	}

	// Check if token is expired
	if (now >= result.expiresAt) {
		await db.delete(webauthnChallenges).where(eq(webauthnChallenges.id, result.id));
		return false;
	}

	// Check if token type is CSRF
	if (result.type !== 'csrf') {
		return false;
	}

	// If userId is provided, verify it matches
	if (userId && result.userId !== userId) {
		return false;
	}

	// Delete the token after use (one-time use)
	await db.delete(webauthnChallenges).where(eq(webauthnChallenges.id, result.id));

	return true;
}

export async function cleanupExpiredCSRFTokens(): Promise<number> {
	await db
		.delete(webauthnChallenges)
		.where(lt(webauthnChallenges.expiresAt, new Date()));
	
	// Drizzle doesn't return row count, so we'll return 0
	// In a real implementation, you might want to track this separately
	return 0;
}

export function getCSRFTokenFromRequest(request: Request): string | null {
	const token = request.headers.get('x-csrf-token') || 
				 request.headers.get('csrf-token') ||
				 new URL(request.url).searchParams.get('csrf_token');
	
	return token;
}

export function validateCSRFHeaders(request: Request): boolean {
	const origin = request.headers.get('origin');
	const referer = request.headers.get('referer');
	
	// For same-origin requests, CSRF is not a concern
	if (!origin && !referer) {
		return true;
	}

	// Validate origin/referer against expected domains
	const expectedDomains = [
		'http://localhost:5173', // Dev
		'https://your-domain.com', // Production
		'https://www.your-domain.com' // Production with www
	];

	if (origin && !expectedDomains.some(domain => origin.startsWith(domain))) {
		return false;
	}

	if (referer && !expectedDomains.some(domain => referer.startsWith(domain))) {
		return false;
	}

	return true;
} 