import { json } from '@sveltejs/kit';
import type { RequestHandler } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { webauthnCredentials } from '$lib/server/db/schema';
import { eq } from 'drizzle-orm';

export const POST: RequestHandler = async ({ request }) => {
	try {
		const { userId, newCredentialId } = await request.json();

		if (!userId || !newCredentialId) {
			return json({
				success: false,
				message: 'User ID and new credential ID are required'
			}, { status: 400 });
		}

		// Get user's existing credentials
		const existingCredentials = await db
			.select({
				id: webauthnCredentials.id,
				credentialId: webauthnCredentials.credentialId
			})
			.from(webauthnCredentials)
			.where(eq(webauthnCredentials.userId, userId));

		if (existingCredentials.length === 0) {
			return json({
				success: false,
				message: 'No existing credentials found for rotation'
			}, { status: 404 });
		}

		// Delete all existing credentials for this user
		await db
			.delete(webauthnCredentials)
			.where(eq(webauthnCredentials.userId, userId));

		console.log(`Rotated credentials for user ${userId}: deleted ${existingCredentials.length} old credentials`);

		return json({
			success: true,
			message: 'Credentials rotated successfully',
			deletedCount: existingCredentials.length
		});

	} catch (error) {
		console.error('Credential rotation error:', error);
		return json({
			success: false,
			message: error instanceof Error ? error.message : 'Failed to rotate credentials',
			error: 'Credential rotation failed'
		}, { status: 500 });
	}
}; 