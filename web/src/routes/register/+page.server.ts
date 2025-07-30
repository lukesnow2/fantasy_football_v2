import type { ServerLoad } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { dimManager } from '$lib/server/db/schema';

export const load: ServerLoad = async () => {
	// Get available managers (not yet linked to users)
	const availableManagers = await db
		.select({
			managerKey: dimManager.managerKey,
			managerName: dimManager.managerName,
			displayName: dimManager.displayName
		})
		.from(dimManager)
		.orderBy(dimManager.displayName);

	return {
		availableManagers
	};
}; 