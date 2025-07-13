import { hash, verify } from '@node-rs/argon2';
import { fail, redirect } from '@sveltejs/kit';
import { eq } from 'drizzle-orm';
import * as auth from '$lib/server/auth';
import { db } from '$lib/server/db';
import { user as userTable, dimManager } from '$lib/server/db/schema';
import { getAvailableManagers } from '$lib/server/auth-manager';
import type { Actions, PageServerLoad } from './$types';

export const load: PageServerLoad = async (event) => {
	// If already logged in, redirect to homepage
	if (event.locals.user) {
		return redirect(302, '/');
	}

	// Check for success messages
	const message = event.url.searchParams.get('message');
	let successMessage = null;
	
	if (message === 'password-reset-success') {
		successMessage = 'Your password has been successfully reset. You can now log in with your new password.';
	}

	// Get available managers for the registration dropdown
	const availableManagers = await getAvailableManagers();
	
	return {
		availableManagers,
		successMessage
	};
};

export const actions: Actions = {
	login: async (event) => {
		const formData = await event.request.formData();
		const username = formData.get('username');
		const password = formData.get('password');

		if (!validateUsername(username)) {
			return fail(400, {
				message: 'Invalid username (min 3, max 31 characters, alphanumeric only)'
			});
		}
		if (!validatePassword(password)) {
			return fail(400, { message: 'Invalid password (min 6, max 255 characters)' });
		}

		// Get user with manager information
		const results = await db
			.select({
				user: userTable,
				manager: dimManager
			})
			.from(userTable)
			.leftJoin(dimManager, eq(userTable.managerKey, dimManager.managerKey))
			.where(eq(userTable.username, username));

		const userData = results.at(0);
		if (!userData?.user) {
			return fail(400, { message: 'Incorrect username or password' });
		}

		const validPassword = await verify(userData.user.passwordHash, password, {
			memoryCost: 19456,
			timeCost: 2,
			outputLen: 32,
			parallelism: 1
		});
		if (!validPassword) {
			return fail(400, { message: 'Incorrect username or password' });
		}

		// Check if user has a linked manager
		if (!userData.manager) {
			return fail(400, { 
				message: 'Your account is not linked to a fantasy manager. Please contact the commissioner.' 
			});
		}

		const sessionToken = auth.generateSessionToken();
		const session = await auth.createSession(sessionToken, userData.user.id);
		auth.setSessionTokenCookie(event, sessionToken, session.expiresAt);

		return redirect(302, '/');
	},
	
	register: async (event) => {
		const formData = await event.request.formData();
		const username = formData.get('username');
		const password = formData.get('password');
		const email = formData.get('email');
		const managerKey = formData.get('managerKey');

		if (!validateUsername(username)) {
			return fail(400, { message: 'Invalid username (3-31 characters, alphanumeric only)' });
		}
		if (!validatePassword(password)) {
			return fail(400, { message: 'Invalid password (minimum 6 characters)' });
		}
		if (!validateEmail(email)) {
			return fail(400, { message: 'Please provide a valid email address' });
		}
		if (!managerKey) {
			return fail(400, { message: 'Please select your fantasy manager profile' });
		}

		// Check if manager is already taken
		const existingUser = await db
			.select()
			.from(userTable)
			.where(eq(userTable.managerKey, parseInt(managerKey as string)))
			.limit(1);

		let isClaimingPlaceholder = false;
		let existingUserId = null;

		if (existingUser.length > 0) {
			const user = existingUser[0];
			
			// Check if this is a placeholder account that can be claimed
			if (user.accountStatus === 'placeholder') {
				// This is a placeholder account - allow claiming it
				isClaimingPlaceholder = true;
				existingUserId = user.id;
				console.log(`🎯 Claiming placeholder account for manager key ${managerKey}: ${user.displayName || 'Unknown'}`);
			} else {
				// This is an active account - deny registration
				return fail(400, { 
					message: 'This manager profile is already registered. Contact the commissioner if this is an error.' 
				});
			}
		}

		// Get manager info for display name
		const managerInfo = await db
			.select()
			.from(dimManager)
			.where(eq(dimManager.managerKey, parseInt(managerKey as string)))
			.limit(1);

		if (managerInfo.length === 0) {
			return fail(400, { message: 'Invalid manager selection' });
		}

		const passwordHash = await hash(password, {
			memoryCost: 19456,
			timeCost: 2,
			outputLen: 32,
			parallelism: 1
		});

		let userId: string;

		try {
			if (isClaimingPlaceholder && existingUserId) {
				// Update existing placeholder account
				userId = existingUserId;
				await db
					.update(userTable)
					.set({
						username,
						passwordHash,
						email,
						displayName: managerInfo[0].displayName || managerInfo[0].managerName,
						accountStatus: 'active',
						notificationPreferences: JSON.stringify({
							emailOnNewProposal: true,
							emailOnVoteResults: true,
							emailOnTradeOffers: false
						}),
						profileSettings: JSON.stringify({}),
						updatedAt: new Date()
					})
					.where(eq(userTable.id, existingUserId));
				
				console.log(`✅ Successfully claimed placeholder account for ${username}`);
			} else {
				// Create new user account
				userId = generateUserId();
				await db.insert(userTable).values({ 
					id: userId, 
					username, 
					passwordHash,
					email,
					managerKey: parseInt(managerKey as string),
					displayName: managerInfo[0].displayName || managerInfo[0].managerName,
					notificationPreferences: JSON.stringify({
						emailOnNewProposal: true,
						emailOnVoteResults: true,
						emailOnTradeOffers: false
					}),
					profileSettings: JSON.stringify({})
				});
				
				console.log(`✅ Successfully created new account for ${username}`);
			}

			const sessionToken = auth.generateSessionToken();
			const session = await auth.createSession(sessionToken, userId);
			auth.setSessionTokenCookie(event, sessionToken, session.expiresAt);
		} catch (error: any) {
			if (error.code === '23505') { // Unique constraint violation
				if (error.constraint === 'user_username_unique') {
					return fail(400, { message: 'Username already taken' });
				}
				if (error.constraint === 'user_email_unique') {
					return fail(400, { message: 'Email already registered' });
				}
				if (error.constraint === 'unique_user_manager_key') {
					return fail(400, { message: 'This manager profile is already registered' });
				}
			}
			
			const actionType = isClaimingPlaceholder ? 'claiming placeholder account' : 'registration';
			console.error(`${actionType} error:`, error);
			return fail(500, { message: `${isClaimingPlaceholder ? 'Account claiming' : 'Registration'} failed. Please try again.` });
		}
		
		return redirect(302, '/');
	}
};

function validateUsername(username: unknown): username is string {
	return (
		typeof username === 'string' &&
		username.length >= 3 &&
		username.length <= 31 &&
		/^[a-zA-Z0-9_]+$/.test(username)
	);
}

function validatePassword(password: unknown): password is string {
	return typeof password === 'string' && password.length >= 6 && password.length <= 255;
}

function validateEmail(email: unknown): email is string {
	return (
		typeof email === 'string' &&
		email.length > 0 &&
		/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)
	);
}

function generateUserId(): string {
	// Generate a random user ID
	const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
	let result = '';
	for (let i = 0; i < 21; i++) {
		result += chars.charAt(Math.floor(Math.random() * chars.length));
	}
	return result;
} 