import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { webauthnChallenges, webauthnCredentials } from '$lib/server/db/schema';
import { validateChallenge } from '$lib/server/webauthn/challenge';
import { createWebAuthnError, WebAuthnErrorCode } from '$lib/server/webauthn/errors';
import { logAuditEvent, AuditEventType, AuditSeverity } from '$lib/server/webauthn/audit';
import { eq } from 'drizzle-orm';

export async function POST({ request, getClientAddress }: { request: Request; getClientAddress: () => string }) {
	try {
		console.log('🔍 WebAuthn authentication verification request received');
		
		const { response, challengeId } = await request.json();
		const clientAddress = getClientAddress();
		
		console.log('📊 Verification data:', { 
			hasResponse: !!response, 
			challengeId,
			responseKeys: response ? Object.keys(response) : []
		});

		if (!response || !challengeId) {
			throw createWebAuthnError(
				WebAuthnErrorCode.INVALID_AUTHENTICATION_RESPONSE,
				'Missing authentication response or challenge ID'
			);
		}

		// Validate the challenge
		const challengeValidation = await validateChallenge(
			response.challenge,
			'authentication'
		);

		if (!challengeValidation.valid) {
			console.error('❌ Challenge validation failed:', challengeValidation.error);
			throw createWebAuthnError(
				WebAuthnErrorCode.INVALID_CHALLENGE,
				challengeValidation.error || 'Invalid challenge'
			);
		}

		// TODO: Implement actual WebAuthn verification
		// For now, we'll simulate a successful verification
		console.log('✅ Authentication verification completed (simulated)');

		// Log successful authentication
		await logAuditEvent(
			AuditEventType.AUTHENTICATION_COMPLETED,
			AuditSeverity.INFO,
			{ challengeId, credentialId: response.id },
			{ ipAddress: clientAddress }
		);

		return json({
			success: true,
			userId: 'simulated-user-id' // TODO: Extract from actual verification
		});

	} catch (error) {
		console.error('💥 Authentication verification error:', error);
		
		// Log failed authentication
		await logAuditEvent(
			AuditEventType.AUTHENTICATION_FAILED,
			AuditSeverity.ERROR,
			{ 
				challengeId: null, // Can't access request.json() again in catch block
				error: error instanceof Error ? error.message : 'Unknown error'
			},
			{ ipAddress: getClientAddress() },
			error instanceof Error ? error : undefined
		);

		return json(
			{ error: error instanceof Error ? error.message : 'Authentication verification failed' },
			{ status: 400 }
		);
	}
} 