import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { webauthnCredentials, user } from '$lib/server/db/schema';
import { eq, and } from 'drizzle-orm';
import { logAuditEvent, AuditEventType, AuditSeverity } from '$lib/server/webauthn/audit';

export async function POST({ request, getClientAddress }: { request: Request; getClientAddress: () => string }) {
	try {
		console.log('🗑️ WebAuthn credential deletion request received');
		
		const { userId, credentialId } = await request.json();
		const clientAddress = getClientAddress();
		
		console.log('📊 Deletion request data:', { userId, credentialId, clientAddress });

		if (!userId || !credentialId) {
			return json(
				{ error: 'User ID and credential ID are required' },
				{ status: 400 }
			);
		}

		// Check if credential exists and belongs to user
		const existingCredential = await db
			.select({
				id: webauthnCredentials.id,
				deviceType: webauthnCredentials.deviceType,
				authenticatorType: webauthnCredentials.authenticatorType
			})
			.from(webauthnCredentials)
			.where(and(
				eq(webauthnCredentials.id, credentialId),
				eq(webauthnCredentials.userId, userId)
			))
			.limit(1);

		if (existingCredential.length === 0) {
			console.error('❌ Credential not found or does not belong to user');
			return json(
				{ error: 'Credential not found or does not belong to user' },
				{ status: 404 }
			);
		}

		// Delete the credential
		await db
			.delete(webauthnCredentials)
			.where(eq(webauthnCredentials.id, credentialId));

		// Check if user has any remaining credentials
		const remainingCredentials = await db
			.select({ id: webauthnCredentials.id })
			.from(webauthnCredentials)
			.where(eq(webauthnCredentials.userId, userId));

		// If no credentials remain, update user table
		if (remainingCredentials.length === 0) {
			await db
				.update(user)
				.set({
					passkeyEnabled: false,
					passkeyRegisteredAt: null,
					updatedAt: new Date()
				})
				.where(eq(user.id, userId));

			console.log('⚠️ No credentials remaining for user, disabled passkey');
		}

		console.log('✅ Credential deleted successfully:', {
			credentialId,
			userId,
			deviceType: existingCredential[0].deviceType,
			remainingCredentials: remainingCredentials.length
		});

		// Log credential deletion
		await logAuditEvent(
			AuditEventType.CREDENTIAL_DELETED,
			AuditSeverity.INFO,
			{ 
				credentialId,
				deviceType: existingCredential[0].deviceType,
				authenticatorType: existingCredential[0].authenticatorType,
				remainingCredentials: remainingCredentials.length
			},
			{ userId, ipAddress: clientAddress }
		);

		return json({
			success: true,
			message: 'Credential deleted successfully',
			remainingCredentials: remainingCredentials.length
		});

	} catch (error) {
		console.error('💥 Credential deletion error:', error);
		
		// Log failed deletion
		await logAuditEvent(
			AuditEventType.SECURITY_VIOLATION,
			AuditSeverity.ERROR,
			{ 
				credentialId: null, // Can't access request.json() again in catch block
				error: error instanceof Error ? error.message : 'Unknown error'
			},
			{ userId: undefined, ipAddress: getClientAddress() },
			error instanceof Error ? error : undefined
		);

		return json(
			{ error: error instanceof Error ? error.message : 'Failed to delete credential' },
			{ status: 500 }
		);
	}
} 