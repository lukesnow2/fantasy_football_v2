import { json } from '@sveltejs/kit';
import type { RequestHandler } from '@sveltejs/kit';
import { generateSessionToken, createSession, setSessionTokenCookie } from '$lib/server/auth';
import { db } from '$lib/server/db';
import { user } from '$lib/server/db/schema';
import { eq } from 'drizzle-orm';

export const POST: RequestHandler = async ({ request, cookies }) => {
	try {
		const { userId } = await request.json();

		if (!userId) {
			return json({
				success: false,
				message: 'User ID is required'
			}, { status: 400 });
		}

		// Verify user exists and has passkeys enabled
		const userData = await db
			.select({
				id: user.id,
				username: user.username,
				passkeyEnabled: user.passkeyEnabled,
				passkeyRegisteredAt: user.passkeyRegisteredAt
			})
			.from(user)
			.where(eq(user.id, userId))
			.limit(1);

		if (!userData.length) {
			return json({
				success: false,
				message: 'User not found'
			}, { status: 404 });
		}

		const userRecord = userData[0];

		if (!userRecord.passkeyEnabled || !userRecord.passkeyRegisteredAt) {
			return json({
				success: false,
				message: 'User must have passkeys enabled to complete setup'
			}, { status: 400 });
		}

		// Create session for the user
		const sessionToken = generateSessionToken();
		const session = await createSession(sessionToken, userRecord.id);
		setSessionTokenCookie({ cookies } as any, sessionToken, session.expiresAt);

		return json({
			success: true,
			message: 'Session created successfully',
			redirectUrl: '/dashboard'
		});

	} catch (error) {
		console.error('Setup completion error:', error);
		return json({
			success: false,
			message: error instanceof Error ? error.message : 'Failed to complete setup',
			error: 'Setup completion failed'
		}, { status: 500 });
	}
}; 