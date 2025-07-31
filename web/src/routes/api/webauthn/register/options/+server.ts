import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { createChallenge } from '$lib/server/webauthn/challenge';
import { createWebAuthnError, WebAuthnErrorCode } from '$lib/server/webauthn/errors';
import { logAuditEvent, AuditEventType, AuditSeverity } from '$lib/server/webauthn/audit';

export async function POST({ request, getClientAddress }: { request: Request; getClientAddress: () => string }) {
	try {
		console.log('🔐 WebAuthn registration options request received');
		
		const { userId, username, managerKey } = await request.json();
		const clientAddress = getClientAddress();
		
		// Get the actual domain from the request
		const origin = request.headers.get('origin') || request.headers.get('host') || 'localhost';
		const rpId = origin.includes('localhost') ? 'localhost' : origin.replace(/^https?:\/\//, '').split(':')[0];
		
		console.log('📊 Registration request data:', { 
			userId, 
			username, 
			managerKey, 
			clientAddress,
			origin,
			rpId
		});

		// Validate required fields
		if (!userId || !username) {
			throw createWebAuthnError(
				WebAuthnErrorCode.INVALID_USER_ID,
				'User ID and username are required for registration'
			);
		}

		// Generate registration challenge
		const challenge = await createChallenge(userId, 'registration', {
			ipAddress: clientAddress,
			userAgent: request.headers.get('user-agent') || undefined
		});
		
		if (!challenge) {
			console.error('❌ Failed to generate registration challenge');
			throw createWebAuthnError(
				WebAuthnErrorCode.INTERNAL_ERROR,
				'Failed to generate registration challenge'
			);
		}

		// Create WebAuthn registration options
		const options = {
			rp: {
				name: 'The League',
				id: rpId // Use dynamic RP ID based on actual domain
			},
			user: {
				id: userId,
				name: username,
				displayName: username
			},
			challenge: challenge.challenge,
			pubKeyCredParams: [
				{
					type: 'public-key',
					alg: -7 // ES256
				}
			],
			timeout: 60000, // 60 seconds
			attestation: 'none', // Don't require attestation for now
			authenticatorSelection: {
				authenticatorAttachment: 'platform', // Biometric only
				userVerification: 'preferred',
				requireResidentKey: false
			},
			excludeCredentials: [] // No existing credentials to exclude for new registration
		};

		console.log('✅ Registration options generated:', {
			challengeId: challenge.id,
			hasOptions: !!options,
			optionsKeys: Object.keys(options),
			userId,
			username,
			rpId: options.rp.id
		});

		// Log audit event
		await logAuditEvent(
			AuditEventType.REGISTRATION_STARTED,
			AuditSeverity.INFO,
			{ 
				challengeId: challenge.id,
				userId,
				username,
				managerKey: managerKey || null,
				rpId: options.rp.id
			},
			{ userId, ipAddress: clientAddress }
		);

		return json({
			options,
			challengeId: challenge.id
		});

	} catch (error) {
		console.error('💥 Registration options error:', error);
		
		// Log audit event
		await logAuditEvent(
			AuditEventType.REGISTRATION_FAILED,
			AuditSeverity.ERROR,
			{ 
				error: error instanceof Error ? error.message : 'Unknown error',
				userId: null // Can't access request.json() again in catch block
			},
			{ ipAddress: getClientAddress() },
			error instanceof Error ? error : undefined
		);

		return json(
			{ error: error instanceof Error ? error.message : 'Failed to generate registration options' },
			{ status: 400 }
		);
	}
} 