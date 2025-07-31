import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { user, dimManager } from '$lib/server/db/schema';
import { createChallenge } from '$lib/server/webauthn/challenge';
import { createWebAuthnError, WebAuthnErrorCode } from '$lib/server/webauthn/errors';
import { logAuditEvent, AuditEventType, AuditSeverity } from '$lib/server/webauthn/audit';
import { eq, sql } from 'drizzle-orm';

export async function POST({ request, getClientAddress }: { request: Request; getClientAddress: () => string }) {
	try {
		console.log('🔐 WebAuthn registration options request received');
		
		const { username } = await request.json();
		const clientAddress = getClientAddress();
		
		// Get the actual domain from the request
		const origin = request.headers.get('origin') || request.headers.get('host') || 'localhost';
		const rpId = origin.includes('localhost') ? 'localhost' : origin.replace(/^https?:\/\//, '').split(':')[0];
		
		console.log('📊 Registration request data:', { 
			username, 
			clientAddress,
			origin,
			rpId
		});

		// Validate required fields
		if (!username) {
			throw createWebAuthnError(
				WebAuthnErrorCode.INVALID_USER_ID,
				'Username is required for registration'
			);
		}

		// DEBUG: Check if user table exists and has data
		console.log('🔍 Checking user table for registration...');
		const allUsers = await db
			.select({
				id: user.id,
				username: user.username,
				managerKey: user.managerKey,
				passkeyEnabled: user.passkeyEnabled
			})
			.from(user)
			.limit(5);

		console.log('📋 All users in database (registration):', allUsers);

		// Look up user by username with detailed logging
		console.log('🔍 Looking up user by username for registration:', username);
		let userQuery = await db
			.select({
				id: user.id,
				username: user.username,
				managerKey: user.managerKey,
				passkeyEnabled: user.passkeyEnabled
			})
			.from(user)
			.where(eq(user.username, username));

		console.log('🔍 User query result (registration):', {
			query: `WHERE username = '${username}'`,
			result: userQuery,
			count: userQuery.length
		});

		if (userQuery.length === 0) {
			// Try case-insensitive search
			console.log('🔍 Trying case-insensitive search for registration...');
			const caseInsensitiveQuery = await db
				.select({
					id: user.id,
					username: user.username,
					managerKey: user.managerKey,
					passkeyEnabled: user.passkeyEnabled
				})
				.from(user)
				.where(sql`LOWER(${user.username}) = LOWER(${username})`);

			console.log('🔍 Case-insensitive query result (registration):', {
				query: `WHERE LOWER(username) = LOWER('${username}')`,
				result: caseInsensitiveQuery,
				count: caseInsensitiveQuery.length
			});

			if (caseInsensitiveQuery.length === 0) {
				throw createWebAuthnError(
					WebAuthnErrorCode.INVALID_USER_ID,
					`No account found for username: ${username}`
				);
			} else {
				console.log('✅ Found user with case-insensitive search (registration)');
				userQuery = caseInsensitiveQuery;
			}
		}

		const userRecord = userQuery[0];
		const userId = userRecord.id;
		const usernameValue = userRecord.username;

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
				name: usernameValue,
				displayName: userRecord.managerKey // Assuming managerKey is the display name for now
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
			managerName: userRecord.managerKey, // Assuming managerKey is the display name for now
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
				managerName: userRecord.managerKey, // Assuming managerKey is the display name for now
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
				managerName: null // Can't access request.json() again in catch block
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