import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { createChallenge } from '$lib/server/webauthn/challenge';
import { createWebAuthnError, WebAuthnErrorCode } from '$lib/server/webauthn/errors';
import { logAuditEvent, AuditEventType, AuditSeverity } from '$lib/server/webauthn/audit';

export async function POST({ request, getClientAddress }: { request: Request; getClientAddress: () => string }) {
	try {
		console.log('🔐 WebAuthn authentication options request received');
		
		const { userId } = await request.json();
		const clientAddress = getClientAddress();
		
		console.log('📊 Request data:', { userId, clientAddress });

		// Generate authentication challenge
		const challenge = await createChallenge(userId, 'authentication', {
			ipAddress: clientAddress,
			userAgent: request.headers.get('user-agent') || undefined
		});
		
		if (!challenge) {
			console.error('❌ Failed to generate authentication challenge');
			throw createWebAuthnError(
				WebAuthnErrorCode.INTERNAL_ERROR,
				'Failed to generate authentication challenge'
			);
		}

		// Create WebAuthn authentication options
		const options = {
			rpId: 'localhost', // TODO: Configure for production
			challenge: challenge.challenge,
			timeout: 60000, // 60 seconds
			userVerification: 'preferred',
			allowCredentials: userId ? [
				{
					id: '', // Will be populated by the client
					type: 'public-key',
					transports: ['internal']
				}
			] : undefined
		};

		console.log('✅ Authentication options generated:', {
			challengeId: challenge.id,
			hasOptions: !!options,
			optionsKeys: Object.keys(options)
		});

		// Log audit event
		await logAuditEvent(
			AuditEventType.AUTHENTICATION_STARTED,
			AuditSeverity.INFO,
			{ challengeId: challenge.id },
			{ userId, ipAddress: clientAddress }
		);

		return json({
			options,
			challengeId: challenge.id
		});

	} catch (error) {
		console.error('💥 Authentication options error:', error);
		
		// Log audit event
		await logAuditEvent(
			AuditEventType.AUTHENTICATION_FAILED,
			AuditSeverity.ERROR,
			{ error: error instanceof Error ? error.message : 'Unknown error' },
			{ ipAddress: getClientAddress() },
			error instanceof Error ? error : undefined
		);

		return json(
			{ error: error instanceof Error ? error.message : 'Failed to generate authentication options' },
			{ status: 400 }
		);
	}
} 