import { fail, redirect } from '@sveltejs/kit';
import { createPasswordResetToken } from '$lib/server/password-reset';
import type { Actions, PageServerLoad } from './$types';

export const load: PageServerLoad = async (event) => {
	// If already logged in, redirect to homepage
	if (event.locals.user) {
		return redirect(302, '/');
	}
	
	return {};
};

export const actions: Actions = {
	default: async ({ request }) => {
		const formData = await request.formData();
		const email = formData.get('email');

		if (!validateEmail(email)) {
			return fail(400, {
				message: 'Please provide a valid email address'
			});
		}

		// Create password reset token (this handles security internally)
		const result = await createPasswordResetToken(email as string);
		
		if (!result.success) {
			return fail(500, {
				message: result.message
			});
		}

		// Always return success message for security
		return {
			success: true,
			message: result.message
		};
	}
};

function validateEmail(email: unknown): email is string {
	return (
		typeof email === 'string' &&
		email.length > 0 &&
		/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)
	);
} 