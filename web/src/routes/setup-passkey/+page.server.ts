import { redirect } from '@sveltejs/kit';
import type { ServerLoad } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { user } from '$lib/server/db/schema';
import { eq } from 'drizzle-orm';

export const load: ServerLoad = async ({ url }) => {
	const userId = url.searchParams.get('userId');
	const username = url.searchParams.get('username');
	const managerKey = url.searchParams.get('managerKey');

	if (!userId || !username || !managerKey) {
		throw redirect(302, '/login');
	}

	// Verify user exists and hasn't already set up passkeys
	const userData = await db
		.select({
			id: user.id,
			username: user.username,
			email: user.email,
			managerKey: user.managerKey,
			passkeyEnabled: user.passkeyEnabled,
			passkeyRegisteredAt: user.passkeyRegisteredAt
		})
		.from(user)
		.where(eq(user.id, userId))
		.limit(1);

	if (!userData.length) {
		throw redirect(302, '/login');
	}

	const userRecord = userData[0];

	// If user already has passkeys enabled, redirect to dashboard
	if (userRecord.passkeyEnabled && userRecord.passkeyRegisteredAt) {
		throw redirect(302, '/');
	}

	return {
		userId: userRecord.id,
		username: userRecord.username,
		email: userRecord.email,
		managerKey: userRecord.managerKey?.toString() || managerKey
	};
}; 