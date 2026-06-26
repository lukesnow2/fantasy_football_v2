import type { ServerLoad } from '@sveltejs/kit';
import { getAvailableManagers } from '$lib/server/auth-manager';

export const load: ServerLoad = async () => {
	// Get available managers (not yet linked to users)
	const availableManagers = await getAvailableManagers();

	return {
		availableManagers
	};
}; 