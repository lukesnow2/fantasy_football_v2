import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { dimManager } from '$lib/server/db/schema';
import { eq, desc } from 'drizzle-orm';

export async function GET({ url }) {
	try {
		const active = url.searchParams.get('active');
		
		let managers;
		
		if (active === 'true') {
			managers = await db
				.select()
				.from(dimManager)
				.where(eq(dimManager.isActive, true))
				.orderBy(desc(dimManager.createdAt));
		} else {
			managers = await db
				.select()
				.from(dimManager)
				.orderBy(desc(dimManager.createdAt));
		}
		
		return json({ managers });
	} catch (error) {
		console.error('Error fetching managers:', error);
		return json({ error: 'Failed to fetch managers' }, { status: 500 });
	}
} 