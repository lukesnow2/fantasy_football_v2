import { eq, and, lt } from 'drizzle-orm';
import { encodeBase64url } from '@oslojs/encoding';
import { hash } from '@node-rs/argon2';
import { db } from './db';
import { user as userTable, passwordResetToken as tokenTable } from './db/schema';
import { emailService, generatePasswordResetEmail } from './email';
import { BCRYPT_ROUNDS } from './env';

const RESET_TOKEN_LIFETIME_HOURS = 1;
const RESET_TOKEN_LIFETIME_MS = RESET_TOKEN_LIFETIME_HOURS * 60 * 60 * 1000;

export interface PasswordResetResult {
	success: boolean;
	message: string;
}

/**
 * Generate a secure reset token
 */
function generateResetToken(): string {
	const bytes = crypto.getRandomValues(new Uint8Array(32));
	return encodeBase64url(bytes);
}

/**
 * Create a password reset token and send email
 */
export async function createPasswordResetToken(email: string): Promise<PasswordResetResult> {
	try {
		// Find user by email
		const userData = await db
			.select({ id: userTable.id, username: userTable.username, email: userTable.email })
			.from(userTable)
			.where(eq(userTable.email, email))
			.limit(1);

		if (userData.length === 0) {
			// Don't reveal whether email exists for security
			return {
				success: true,
				message: 'If an account with that email exists, a password reset link has been sent.'
			};
		}

		const user = userData[0];

		// Delete any existing reset tokens for this user
		await db
			.delete(tokenTable)
			.where(eq(tokenTable.userId, user.id));

		// Create new reset token
		const resetToken = generateResetToken();
		const expiresAt = new Date(Date.now() + RESET_TOKEN_LIFETIME_MS);

		await db.insert(tokenTable).values({
			id: resetToken,
			userId: user.id,
			email: user.email!,
			expiresAt,
		});

		// Send password reset email
		const emailOptions = generatePasswordResetEmail(resetToken, user.email!);
		const emailSent = await emailService.sendEmail(emailOptions);

		if (!emailSent) {
			console.error('Failed to send password reset email to:', user.email);
			// Clean up the token if email failed
			await db.delete(tokenTable).where(eq(tokenTable.id, resetToken));
			
			return {
				success: false,
				message: 'Failed to send password reset email. Please try again later.'
			};
		}

		console.log(`🔑 Password reset token created for user: ${user.username} (${user.email})`);

		return {
			success: true,
			message: 'If an account with that email exists, a password reset link has been sent.'
		};

	} catch (error) {
		console.error('Error creating password reset token:', error);
		return {
			success: false,
			message: 'An error occurred while processing your request. Please try again later.'
		};
	}
}

/**
 * Validate a reset token and return user info if valid
 */
export async function validateResetToken(token: string): Promise<{ valid: boolean; userId?: string; email?: string }> {
	try {
		// Clean up expired tokens first
		await db
			.delete(tokenTable)
			.where(lt(tokenTable.expiresAt, new Date()));

		// Find valid token
		const tokenData = await db
			.select({
				userId: tokenTable.userId,
				email: tokenTable.email,
				expiresAt: tokenTable.expiresAt
			})
			.from(tokenTable)
			.where(eq(tokenTable.id, token))
			.limit(1);

		if (tokenData.length === 0) {
			return { valid: false };
		}

		const tokenInfo = tokenData[0];

		// Check if token is expired
		if (tokenInfo.expiresAt.getTime() < Date.now()) {
			// Clean up expired token
			await db.delete(tokenTable).where(eq(tokenTable.id, token));
			return { valid: false };
		}

		return {
			valid: true,
			userId: tokenInfo.userId,
			email: tokenInfo.email
		};

	} catch (error) {
		console.error('Error validating reset token:', error);
		return { valid: false };
	}
}

/**
 * Reset user's password using a valid token
 */
export async function resetPassword(token: string, newPassword: string): Promise<PasswordResetResult> {
	try {
		// Validate the token
		const tokenValidation = await validateResetToken(token);
		
		if (!tokenValidation.valid || !tokenValidation.userId) {
			return {
				success: false,
				message: 'Invalid or expired reset token. Please request a new password reset link.'
			};
		}

		// Hash the new password
		const passwordHash = await hash(newPassword, {
			memoryCost: 19456,
			timeCost: 2,
			outputLen: 32,
			parallelism: 1
		});

		// Update user's password
		await db
			.update(userTable)
			.set({ 
				passwordHash,
				updatedAt: new Date()
			})
			.where(eq(userTable.id, tokenValidation.userId));

		// Delete the used token and any other tokens for this user
		await db
			.delete(tokenTable)
			.where(eq(tokenTable.userId, tokenValidation.userId));

		console.log(`🔐 Password reset completed for user: ${tokenValidation.userId}`);

		return {
			success: true,
			message: 'Your password has been successfully reset. You can now log in with your new password.'
		};

	} catch (error) {
		console.error('Error resetting password:', error);
		return {
			success: false,
			message: 'An error occurred while resetting your password. Please try again.'
		};
	}
}

/**
 * Clean up expired reset tokens (run periodically)
 */
export async function cleanupExpiredTokens(): Promise<number> {
	try {
		const result = await db
			.delete(tokenTable)
			.where(lt(tokenTable.expiresAt, new Date()));

		console.log(`🧹 Cleaned up expired password reset tokens`);
		return 0; // Drizzle doesn't return affected rows count easily
	} catch (error) {
		console.error('Error cleaning up expired tokens:', error);
		return 0;
	}
} 