import { fail, redirect } from '@sveltejs/kit';
import { validateResetToken, resetPassword } from '$lib/server/password-reset';
import type { Actions, PageServerLoad } from './$types';

export const load: PageServerLoad = async (event) => {
	// If already logged in, redirect to homepage
	if (event.locals.user) {
		return redirect(302, '/');
	}

	const token = event.url.searchParams.get('token');

	if (!token) {
		return {
			error: 'Invalid reset link. Please request a new password reset.',
			token: null,
			email: null
		};
	}

	// Validate the token
	const tokenValidation = await validateResetToken(token);

	if (!tokenValidation.valid) {
		return {
			error: 'Invalid or expired reset link. Please request a new password reset.',
			token: null,
			email: null
		};
	}

	return {
		token,
		email: tokenValidation.email,
		error: null
	};
};

export const actions: Actions = {
	default: async ({ request }) => {
		const formData = await request.formData();
		const token = formData.get('token');
		const password = formData.get('password');
		const confirmPassword = formData.get('confirmPassword');

		if (!token || typeof token !== 'string') {
			return fail(400, {
				message: 'Invalid reset token'
			});
		}

		if (!validatePassword(password)) {
			return fail(400, {
				message: 'Password must be at least 6 characters long'
			});
		}

		if (password !== confirmPassword) {
			return fail(400, {
				message: 'Passwords do not match'
			});
		}

		// Reset the password
		const result = await resetPassword(token, password as string);

		if (!result.success) {
			return fail(400, {
				message: result.message
			});
		}

		// Redirect to login page with success message
		return redirect(302, '/login?message=password-reset-success');
	}
};

function validatePassword(password: unknown): password is string {
	return typeof password === 'string' && password.length >= 6 && password.length <= 255;
} 