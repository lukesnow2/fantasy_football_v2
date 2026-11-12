import { getManagerNameByUserId } from '$lib/server/auth-manager';
import type { PageServerLoad } from './$types';

export const load: PageServerLoad = async ({ locals }) => {
	// If user is authenticated, get their manager name for default selection
	let userManagerName: string | null = null;
	if (locals.user) {
		try {
			userManagerName = await getManagerNameByUserId(locals.user.id);
		} catch (error) {
			console.error('Error getting manager name:', error);
		}
	}
	
	return {
		userManagerName
	};
}; 