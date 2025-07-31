import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { webauthnCredentials, user, dimManager } from '$lib/server/db/schema';
import { createChallenge } from '$lib/server/webauthn/challenge';
import { createWebAuthnError, WebAuthnErrorCode } from '$lib/server/webauthn/errors';
import { logAuditEvent, AuditEventType, AuditSeverity } from '$lib/server/webauthn/audit';
import { eq, sql } from 'drizzle-orm';

export async function GET() {
	return json({ message: 'WebAuthn authenticate options endpoint is working' });
}

export async function POST({ request, getClientAddress }: { request: Request; getClientAddress: () => string }) {
	try {
		console.log('🔐 WebAuthn authentication options request received');
		
		const { username } = await request.json();
		const clientAddress = getClientAddress();
		
		// Get the actual domain from the request
		const origin = request.headers.get('origin') || request.headers.get('host') || 'localhost';
		const rpId = origin.includes('localhost') ? 'localhost' : origin.replace(/^https?:\/\//, '').split(':')[0];
		
		console.log('📊 Request data:', { 
			username, 
			clientAddress,
			origin,
			rpId,
			headers: Object.fromEntries(request.headers.entries())
		});

		// Validate required fields
		if (!username) {
			throw createWebAuthnError(
				WebAuthnErrorCode.INVALID_USER_ID,
				'Username is required for authentication'
			);
		}

		// DEBUG: Check if user table exists and has data
		console.log('🔍 Checking user table...');
		const allUsers = await db
			.select({
				id: user.id,
				username: user.username,
				managerKey: user.managerKey,
				passkeyEnabled: user.passkeyEnabled
			})
			.from(user)
			.limit(5);

		console.log('📋 All users in database:', allUsers);

		// Look up user by username with detailed logging
		console.log('🔍 Looking up user by username:', username);
		let userQuery = await db
			.select({
				id: user.id,
				username: user.username,
				managerKey: user.managerKey,
				passkeyEnabled: user.passkeyEnabled
			})
			.from(user)
			.where(eq(user.username, username));

		console.log('🔍 User query result:', {
			query: `WHERE username = '${username}'`,
			result: userQuery,
			count: userQuery.length
		});

		if (userQuery.length === 0) {
			// Try case-insensitive search
			console.log('🔍 Trying case-insensitive search...');
			const caseInsensitiveQuery = await db
				.select({
					id: user.id,
					username: user.username,
					managerKey: user.managerKey,
					passkeyEnabled: user.passkeyEnabled
				})
				.from(user)
				.where(sql`LOWER(${user.username}) = LOWER(${username})`);

			console.log('🔍 Case-insensitive query result:', {
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
				console.log('✅ Found user with case-insensitive search');
				userQuery = caseInsensitiveQuery;
			}
		}

		const userRecord = userQuery[0];
		const userId = userRecord.id;
		console.log('👤 Found user:', { userId, username: userRecord.username, passkeyEnabled: userRecord.passkeyEnabled });

		// Get existing credentials for the user
		let allowCredentials: any[] | undefined = undefined;
		const existingCredentials = await db
			.select({
				id: webauthnCredentials.credentialId,
				type: webauthnCredentials.authenticatorType
			})
			.from(webauthnCredentials)
			.where(eq(webauthnCredentials.userId, userId));

		console.log('🔑 Found existing credentials:', {
			count: existingCredentials.length,
			credentialIds: existingCredentials.map(c => c.id)
		});

		// If no credentials exist, don't include allowCredentials (triggers registration)
		if (existingCredentials.length > 0) {
			allowCredentials = existingCredentials.map(cred => ({
				id: cred.id,
				type: 'public-key',
				transports: ['internal']
			}));
		}

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
			rpId: rpId, // Use dynamic RP ID based on actual domain
			challenge: challenge.challenge,
			timeout: 60000, // 60 seconds
			userVerification: 'preferred',
			allowCredentials // Will be undefined if no credentials exist (triggers registration)
		};

		console.log('✅ Authentication options generated:', {
			challengeId: challenge.id,
			hasOptions: !!options,
			optionsKeys: Object.keys(options),
			rpId: options.rpId,
			origin,
			hasAllowCredentials: !!allowCredentials,
			credentialsCount: allowCredentials?.length || 0,
			flow: allowCredentials ? 'authentication' : 'registration'
		});

		// Log audit event
		await logAuditEvent(
			AuditEventType.AUTHENTICATION_STARTED,
			AuditSeverity.INFO,
			{ 
				challengeId: challenge.id, 
				rpId, 
				credentialsCount: allowCredentials?.length || 0,
				flow: allowCredentials ? 'authentication' : 'registration',
				managerName: userRecord.username // Assuming username is the manager name for audit
			},
			{ userId: userId, ipAddress: clientAddress }
		);

		return json({
			options,
			challengeId: challenge.id,
			flow: allowCredentials ? 'authentication' : 'registration',
			userId: userRecord.id,
			managerKey: userRecord.managerKey,
			message: allowCredentials 
				? `Sign in as ${userRecord.username}` 
				: `Set up your first passkey for ${userRecord.username}`
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