import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { webauthnCredentials } from '$lib/server/db/schema';
import { eq } from 'drizzle-orm';

export async function GET({ url }: { url: URL }) {
	try {
		console.log('📋 WebAuthn credentials listing request received');
		
		const userId = url.searchParams.get('userId');
		
		if (!userId) {
			return json(
				{ error: 'User ID is required' },
				{ status: 400 }
			);
		}

		console.log('📊 Fetching credentials for user:', userId);

		// Get all credentials for the user
		const credentials = await db
			.select({
				id: webauthnCredentials.id,
				credentialId: webauthnCredentials.credentialId,
				deviceType: webauthnCredentials.deviceType,
				authenticatorType: webauthnCredentials.authenticatorType,
				createdAt: webauthnCredentials.createdAt,
				lastUsedAt: webauthnCredentials.lastUsedAt,
				signCount: webauthnCredentials.signCount
			})
			.from(webauthnCredentials)
			.where(eq(webauthnCredentials.userId, userId))
			.orderBy(webauthnCredentials.createdAt);

		console.log('✅ Credentials retrieved:', {
			count: credentials.length,
			userIds: credentials.map(c => c.id)
		});

		return json({
			success: true,
			credentials
		});

	} catch (error) {
		console.error('💥 Credentials listing error:', error);
		
		return json(
			{ error: error instanceof Error ? error.message : 'Failed to fetch credentials' },
			{ status: 500 }
		);
	}
} 