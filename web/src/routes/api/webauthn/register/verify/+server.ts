import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { webauthnCredentials, user, webauthnChallenges } from '$lib/server/db/schema';
import { validateChallenge } from '$lib/server/webauthn/challenge';
import { createWebAuthnError, WebAuthnErrorCode } from '$lib/server/webauthn/errors';
import { logAuditEvent, AuditEventType, AuditSeverity } from '$lib/server/webauthn/audit';
import { eq } from 'drizzle-orm';

export async function POST({ request, getClientAddress }: { request: Request; getClientAddress: () => string }) {
	try {
		console.log('🔍 WebAuthn registration verification request received');
		
		const { response, challengeId, managerKey } = await request.json();
		const clientAddress = getClientAddress();
		
		console.log('📊 Registration verification data:', { 
			hasResponse: !!response, 
			challengeId,
			managerKey,
			responseKeys: response ? Object.keys(response) : []
		});

		if (!response || !challengeId) {
			throw createWebAuthnError(
				WebAuthnErrorCode.INVALID_REGISTRATION_RESPONSE,
				'Missing registration response or challenge ID'
			);
		}

		// Validate the challenge
		const challengeValidation = await validateChallenge(
			response.challenge,
			'registration'
		);

		if (!challengeValidation.valid) {
			console.error('❌ Challenge validation failed:', challengeValidation.error);
			throw createWebAuthnError(
				WebAuthnErrorCode.INVALID_CHALLENGE,
				challengeValidation.error || 'Invalid challenge'
			);
		}

		// Get the challenge details to find the user ID
		const [challengeRecord] = await db
			.select()
			.from(webauthnChallenges)
			.where(eq(webauthnChallenges.id, challengeValidation.challengeId || ''));

		if (!challengeRecord) {
			throw createWebAuthnError(
				WebAuthnErrorCode.INVALID_CHALLENGE,
				'Challenge record not found'
			);
		}

		const userId = challengeRecord.userId;

		// TODO: Implement actual WebAuthn verification
		// For now, we'll simulate a successful verification and store the credential
		console.log('✅ Registration verification completed (simulated)');

		// Store the credential in the database
		const credentialId = crypto.randomUUID();
		const now = new Date();

		await db.insert(webauthnCredentials).values({
			id: credentialId,
			userId: userId || 'unknown',
			credentialId: response.id || credentialId,
			publicKey: 'simulated-public-key', // TODO: Extract from actual verification
			signCount: 0,
			deviceType: 'unknown', // TODO: Detect from user agent
			authenticatorType: 'platform',
			createdAt: now,
			lastUsedAt: null
		});

		// Update user table to mark passkey as enabled
		if (userId) {
			await db
				.update(user)
				.set({
					passkeyEnabled: true,
					passkeyRegisteredAt: now,
					updatedAt: now
				})
				.where(eq(user.id, userId));
		}

		console.log('💾 Credential stored successfully:', {
			credentialId,
			userId
		});

		// Log successful registration
		await logAuditEvent(
			AuditEventType.REGISTRATION_COMPLETED,
			AuditSeverity.INFO,
			{ 
				challengeId,
				credentialId,
				managerKey: managerKey || null
			},
			{ userId: userId || undefined, ipAddress: clientAddress }
		);

		return json({
			success: true,
			credentialId,
			userId
		});

	} catch (error) {
		console.error('💥 Registration verification error:', error);
		
		// Log failed registration
		await logAuditEvent(
			AuditEventType.REGISTRATION_FAILED,
			AuditSeverity.ERROR,
			{ 
				challengeId: null, // Can't access request.json() again in catch block
				error: error instanceof Error ? error.message : 'Unknown error'
			},
			{ ipAddress: getClientAddress() },
			error instanceof Error ? error : undefined
		);

		return json(
			{ error: error instanceof Error ? error.message : 'Registration verification failed' },
			{ status: 400 }
		);
	}
} 