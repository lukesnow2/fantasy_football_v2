import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { session } from '$lib/server/db/schema';
import { desc } from 'drizzle-orm';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async () => {
	try {
		const sessions = await db
			.select({
				id: session.id,
				userId: session.userId,
				expiresAt: session.expiresAt
			})
			.from(session)
			.orderBy(desc(session.expiresAt))
			.limit(10);

		const now = new Date();
		const sessionInfo = sessions.map(s => ({
			id: s.id.substring(0, 8) + '...',
			userId: s.userId,
			expiresAt: s.expiresAt.toISOString(),
			expired: s.expiresAt < now,
			validFor: s.expiresAt > now ? 
				Math.round((s.expiresAt.getTime() - now.getTime()) / 1000 / 60) + ' minutes' : 
				'expired'
		}));

		return json({
			totalSessions: sessions.length,
			sessions: sessionInfo,
			currentTime: now.toISOString()
		});
	} catch (error) {
		console.error('Error fetching sessions:', error);
		return json({ error: 'Failed to fetch sessions' }, { status: 500 });
	}
}; 