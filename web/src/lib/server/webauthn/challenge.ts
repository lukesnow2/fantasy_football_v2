import { randomBytes } from 'crypto';
import { encodeBase64url } from '@oslojs/encoding';
import { db } from '$lib/server/db';
import { webauthnChallenges } from '$lib/server/db/schema';
import { eq, lt, and, gt, sql } from 'drizzle-orm';
import { logAuditEvent, AuditEventType, AuditSeverity } from './audit';

const CHALLENGE_LENGTH = 32;
const CHALLENGE_EXPIRY_MS = 1000 * 60 * 5; // 5 minutes
const MAX_CHALLENGES_PER_USER = 10; // Prevent challenge flooding

export interface Challenge {
	id: string;
	challenge: string;
	userId?: string | null;
	type: 'registration' | 'authentication' | 'csrf';
	expiresAt: Date;
	createdAt: Date | null;
	usedAt?: Date | null;
	ipAddress?: string | null;
	userAgent?: string | null;
}

export function generateChallenge(): string {
	return encodeBase64url(randomBytes(CHALLENGE_LENGTH));
}

export async function createChallenge(
	userId?: string,
	type: 'registration' | 'authentication' | 'csrf' = 'registration',
	metadata?: { ipAddress?: string; userAgent?: string }
): Promise<Challenge> {
	const challenge = generateChallenge();
	const now = new Date();
	const expiresAt = new Date(now.getTime() + CHALLENGE_EXPIRY_MS);

	// Check for existing challenges and enforce limits
	if (userId) {
		const existingChallenges = await db
			.select({ id: webauthnChallenges.id })
			.from(webauthnChallenges)
			.where(and(
				eq(webauthnChallenges.userId, userId),
				eq(webauthnChallenges.type, type)
			));

		if (existingChallenges.length >= MAX_CHALLENGES_PER_USER) {
			// Remove oldest challenge
			const oldestChallenge = await db
				.select({ id: webauthnChallenges.id, createdAt: webauthnChallenges.createdAt })
				.from(webauthnChallenges)
				.where(and(
					eq(webauthnChallenges.userId, userId),
					eq(webauthnChallenges.type, type)
				))
				.orderBy(webauthnChallenges.createdAt)
				.limit(1);

			if (oldestChallenge.length > 0) {
				await db.delete(webauthnChallenges).where(eq(webauthnChallenges.id, oldestChallenge[0].id));
			}
		}
	}

	const challengeId = crypto.randomUUID();
	const challengeRecord: Challenge = {
		id: challengeId,
		challenge,
		userId,
		type,
		expiresAt,
		createdAt: now,
		ipAddress: metadata?.ipAddress,
		userAgent: metadata?.userAgent
	};

	await db.insert(webauthnChallenges).values({
		id: challengeId,
		challenge,
		userId: userId || null,
		type,
		expiresAt,
		ipAddress: metadata?.ipAddress,
		userAgent: metadata?.userAgent
	});

	// Log challenge creation
	await logAuditEvent(
		AuditEventType.CHALLENGE_CREATED,
		AuditSeverity.INFO,
		{
			challengeId,
			type,
			userId,
			expiresAt,
			ipAddress: metadata?.ipAddress
		},
		{ userId, ipAddress: metadata?.ipAddress }
	);

	return challengeRecord;
}

export async function validateChallenge(
	challenge: string,
	type: 'registration' | 'authentication' | 'csrf',
	userId?: string
): Promise<{ valid: boolean; challengeId?: string; error?: string }> {
	const now = new Date();

	// Find the challenge
	const [result] = await db
		.select()
		.from(webauthnChallenges)
		.where(eq(webauthnChallenges.challenge, challenge));

	if (!result) {
		await logAuditEvent(
			AuditEventType.CHALLENGE_EXPIRED,
			AuditSeverity.WARNING,
			{
				reason: 'challenge_not_found',
				challenge: challenge.substring(0, 8) + '...',
				type
			},
			{ userId }
		);
		
		return {
			valid: false,
			error: 'Challenge not found'
		};
	}

	// Check if challenge is expired
	if (now >= result.expiresAt) {
		await logAuditEvent(
			AuditEventType.CHALLENGE_EXPIRED,
			AuditSeverity.WARNING,
			{
				reason: 'challenge_expired',
				challengeId: result.id,
				expiresAt: result.expiresAt,
				type
			},
			{ userId: result.userId || undefined, ipAddress: result.ipAddress || undefined }
		);

		// Clean up expired challenge
		await db.delete(webauthnChallenges).where(eq(webauthnChallenges.id, result.id));
		
		return {
			valid: false,
			error: 'Challenge has expired'
		};
	}

	// Check if challenge type matches
	if (result.type !== type) {
		await logAuditEvent(
			AuditEventType.CHALLENGE_EXPIRED,
			AuditSeverity.WARNING,
			{
				reason: 'challenge_type_mismatch',
				challengeId: result.id,
				expectedType: type,
				actualType: result.type
			},
			{ userId: result.userId || undefined, ipAddress: result.ipAddress || undefined }
		);
		
		return {
			valid: false,
			error: 'Challenge type mismatch'
		};
	}

	// Check if challenge is already used
	if (result.usedAt) {
		await logAuditEvent(
			AuditEventType.CHALLENGE_EXPIRED,
			AuditSeverity.WARNING,
			{
				reason: 'challenge_already_used',
				challengeId: result.id,
				usedAt: result.usedAt,
				type
			},
			{ userId: result.userId || undefined, ipAddress: result.ipAddress || undefined }
		);
		
		return {
			valid: false,
			error: 'Challenge already used'
		};
	}

	// Check user ID if provided
	if (userId && result.userId !== userId) {
		await logAuditEvent(
			AuditEventType.SECURITY_VIOLATION,
			AuditSeverity.CRITICAL,
			{
				reason: 'challenge_user_mismatch',
				challengeId: result.id,
				expectedUserId: userId,
				actualUserId: result.userId,
				type
			},
			{ userId: result.userId || undefined, ipAddress: result.ipAddress || undefined }
		);
		
		return {
			valid: false,
			error: 'Challenge user mismatch'
		};
	}

	// Mark challenge as used
	await db
		.update(webauthnChallenges)
		.set({ usedAt: now })
		.where(eq(webauthnChallenges.id, result.id));

	// Log successful challenge validation
	await logAuditEvent(
		AuditEventType.CHALLENGE_VALIDATED,
		AuditSeverity.INFO,
		{
			challengeId: result.id,
			type,
			userId: result.userId,
			ageMs: result.createdAt ? now.getTime() - result.createdAt.getTime() : 0
		},
		{ userId: result.userId || undefined, ipAddress: result.ipAddress || undefined }
	);

	return {
		valid: true,
		challengeId: result.id
	};
}

export async function cleanupExpiredChallenges(): Promise<number> {
	const now = new Date();
	
	// Get count before deletion for reporting
	const expiredChallenges = await db
		.select({ id: webauthnChallenges.id })
		.from(webauthnChallenges)
		.where(lt(webauthnChallenges.expiresAt, now));

	// Delete expired challenges
	await db
		.delete(webauthnChallenges)
		.where(lt(webauthnChallenges.expiresAt, now));

	return expiredChallenges.length;
}

export async function getActiveChallengesForUser(
	userId: string,
	type?: 'registration' | 'authentication' | 'csrf'
): Promise<Challenge[]> {
	const whereConditions = [eq(webauthnChallenges.userId, userId)];
	
	if (type) {
		whereConditions.push(eq(webauthnChallenges.type, type));
	}

	const challenges = await db
		.select()
		.from(webauthnChallenges)
		.where(and(...whereConditions))
		.orderBy(webauthnChallenges.createdAt);

	return challenges.map(c => ({
		id: c.id,
		challenge: c.challenge,
		userId: c.userId,
		type: c.type as 'registration' | 'authentication' | 'csrf',
		expiresAt: c.expiresAt,
		createdAt: c.createdAt,
		usedAt: c.usedAt,
		ipAddress: c.ipAddress,
		userAgent: c.userAgent
	}));
}

export function isChallengeExpired(expiresAt: Date): boolean {
	return new Date() >= expiresAt;
}

export function getChallengeAge(createdAt: Date): number {
	return Date.now() - createdAt.getTime();
}

// Rate limiting for challenge creation
export async function checkChallengeRateLimit(
	userId?: string,
	ipAddress?: string
): Promise<{ allowed: boolean; retryAfter?: number }> {
	const now = new Date();
	const windowStart = new Date(now.getTime() - 1000 * 60 * 5); // 5 minute window

	// Check challenges created in the last 5 minutes
	const recentChallenges = await db
		.select({ createdAt: webauthnChallenges.createdAt })
		.from(webauthnChallenges)
		.where(and(
			sql`${webauthnChallenges.createdAt} > ${windowStart}`,
			userId ? eq(webauthnChallenges.userId, userId) : undefined
		));

	const maxChallenges = userId ? 5 : 10; // More lenient for anonymous users
	
	if (recentChallenges.length >= maxChallenges) {
		const oldestChallenge = recentChallenges[0];
		const retryAfter = oldestChallenge.createdAt ? 
			Math.ceil((oldestChallenge.createdAt.getTime() + 1000 * 60 * 5 - now.getTime()) / 1000) : 
			300; // Default to 5 minutes if createdAt is null
		
		await logAuditEvent(
			AuditEventType.RATE_LIMIT_TRIGGERED,
			AuditSeverity.WARNING,
			{
				reason: 'challenge_rate_limit',
				userId,
				ipAddress,
				challengeCount: recentChallenges.length,
				maxChallenges,
				retryAfter
			},
			{ userId, ipAddress }
		);

		return {
			allowed: false,
			retryAfter
		};
	}

	return { allowed: true };
} 