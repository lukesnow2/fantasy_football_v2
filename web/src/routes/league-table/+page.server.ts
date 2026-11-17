import { getManagerNameByUserId } from '$lib/server/auth-manager';
import type { PageServerLoad } from './$types';

/**
 * The signed-in member's manager name, so their row can be marked "You"
 * (S14.3). The table's numbers load in the browser from the same three APIs
 * the Managers page reads, as every other page here does, so the e2e suite
 * can stub them.
 */
export const load: PageServerLoad = async ({ locals }) => {
	let userManagerName: string | null = null;
	if (locals.user) {
		try {
			userManagerName = await getManagerNameByUserId(locals.user.id);
		} catch (error) {
			console.error('Error getting manager name:', error);
		}
	}
	return { userManagerName };
};
