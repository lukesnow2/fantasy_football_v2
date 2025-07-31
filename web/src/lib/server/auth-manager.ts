import { db } from './db';
import { user as userTable, dimManager, type User } from './db/schema';
import { eq, and, or, isNull } from 'drizzle-orm';

export interface AuthenticatedManager {
	userId: string;
	username: string;
	managerKey: number;
	managerName: string;
	managerId: string;
	displayName: string;
	email: string;
	profileImageUrl?: string;
	notificationPreferences: {
		emailOnNewProposal: boolean;
		emailOnVoteResults: boolean;
		emailOnTradeOffers: boolean;
	};
	profileSettings: Record<string, any>;
}

/**
 * Get full manager profile for authenticated user
 */
export async function getAuthenticatedManager(user: User): Promise<AuthenticatedManager | null> {
	try {
		// Get user with manager information
		const userWithManager = await db
			.select({
				userId: userTable.id,
				username: userTable.username,
				userDisplayName: userTable.displayName,
				userEmail: userTable.email,
				managerKey: userTable.managerKey,
				notificationPreferences: userTable.notificationPreferences,
				profileSettings: userTable.profileSettings,
				// Manager fields
				managerName: dimManager.managerName,
				managerId: dimManager.managerId,
				managerDisplayName: dimManager.displayName,
				profileImageUrl: dimManager.profileImageUrl,
				email: dimManager.email
			})
			.from(userTable)
			.leftJoin(dimManager, eq(userTable.managerKey, dimManager.managerKey))
			.where(eq(userTable.id, user.id))
			.limit(1);

		const userData = userWithManager[0];
		if (!userData || !userData.managerKey) {
			return null;
		}

		return {
			userId: userData.userId,
			username: userData.username,
			managerKey: userData.managerKey,
			managerName: userData.managerName!,
			managerId: userData.managerId!,
			displayName: userData.managerDisplayName || userData.userDisplayName || userData.managerName!,
			email: userData.email || userData.userEmail || '',
			profileImageUrl: userData.profileImageUrl || undefined,
			notificationPreferences: userData.notificationPreferences as any || {
				emailOnNewProposal: true,
				emailOnVoteResults: true,
				emailOnTradeOffers: false
			},
			profileSettings: userData.profileSettings as any || {}
		};
	} catch (error) {
		console.error('Error getting authenticated manager:', error);
		return null;
	}
}

/**
 * Get manager key for current authenticated user
 */
export async function getCurrentManagerKey(user: User | null): Promise<number | null> {
	if (!user) return null;
	
	const manager = await getAuthenticatedManager(user);
	return manager?.managerKey || null;
}

/**
 * Get manager name for a user by their user ID (for redirects)
 */
export async function getManagerNameByUserId(userId: string): Promise<string | null> {
	try {
		const userWithManager = await db
			.select({
				managerName: dimManager.managerName
			})
			.from(userTable)
			.leftJoin(dimManager, eq(userTable.managerKey, dimManager.managerKey))
			.where(eq(userTable.id, userId))
			.limit(1);

		return userWithManager[0]?.managerName || null;
	} catch (error) {
		console.error('Error getting manager name by user ID:', error);
		return null;
	}
}

/**
 * Check if user is authenticated and has manager access
 */
export function requireManagerAuth(user: User | null, managerKey?: number): boolean {
	if (!user) return false;
	
	// If specific manager key provided, verify user has access to it
	if (managerKey) {
		// In the future, we could allow commissioners to manage other accounts
		return true; // For now, trust the session
	}
	
	return true;
}

/**
 * Get all managers available for registration (unclaimed)
 */
export async function getAvailableManagers() {
	try {
		console.log('🔍 getAvailableManagers: Starting query...');
		
		// Get managers that either have no user account or have placeholder accounts
		const availableManagers = await db
			.select({
				managerKey: dimManager.managerKey,
				managerName: dimManager.managerName,
				managerId: dimManager.managerId,
				displayName: dimManager.displayName,
				profileImageUrl: dimManager.profileImageUrl,
				isActive: dimManager.isActive,
				isCurrent: dimManager.isCurrent
			})
			.from(dimManager)
			.leftJoin(userTable, eq(dimManager.managerKey, userTable.managerKey))
			.where(
				and(
					eq(dimManager.isCurrent, true),
					eq(dimManager.isActive, true), // Only include active managers
					or(
						isNull(userTable.managerKey), // No user account linked
						eq(userTable.accountStatus, 'placeholder') // Or placeholder account that can be claimed
					)
				)
			)
			.orderBy(dimManager.managerName);

		console.log('📋 getAvailableManagers: Query result:', availableManagers);
		
		// Debug: Check Luke S specifically
		const lukeSManager = await db
			.select({
				managerKey: dimManager.managerKey,
				managerName: dimManager.managerName,
				managerId: dimManager.managerId,
				displayName: dimManager.displayName,
				isActive: dimManager.isActive,
				isCurrent: dimManager.isCurrent,
				userAccountStatus: userTable.accountStatus,
				userUsername: userTable.username
			})
			.from(dimManager)
			.leftJoin(userTable, eq(dimManager.managerKey, userTable.managerKey))
			.where(eq(dimManager.managerName, 'Luke S'));

		console.log('🔍 getAvailableManagers: Luke S manager data:', lukeSManager);
		
		// Debug: Check all users
		const allUsers = await db
			.select({
				username: userTable.username,
				managerKey: userTable.managerKey,
				accountStatus: userTable.accountStatus
			})
			.from(userTable);

		console.log('👥 getAvailableManagers: All users:', allUsers);

		return availableManagers;
	} catch (error) {
		console.error('Error getting available managers:', error);
		return [];
	}
}

/**
 * Helper to get manager name from manager key
 */
export async function getManagerNameByKey(managerKey: number): Promise<string | null> {
	try {
		const result = await db
			.select({ managerName: dimManager.managerName })
			.from(dimManager)
			.where(eq(dimManager.managerKey, managerKey))
			.limit(1);

		return result[0]?.managerName || null;
	} catch (error) {
		console.error('Error getting manager name:', error);
		return null;
	}
}

/**
 * Update user notification preferences
 */
export async function updateNotificationPreferences(
	userId: string, 
	preferences: Partial<AuthenticatedManager['notificationPreferences']>
): Promise<boolean> {
	try {
		await db
			.update(userTable)
			.set({ 
				notificationPreferences: JSON.stringify(preferences)
			})
			.where(eq(userTable.id, userId));
		
		return true;
	} catch (error) {
		console.error('Error updating notification preferences:', error);
		return false;
	}
}

/**
 * Update user profile settings
 */
export async function updateProfileSettings(
	userId: string, 
	settings: Record<string, any>
): Promise<boolean> {
	try {
		await db
			.update(userTable)
			.set({ 
				profileSettings: JSON.stringify(settings)
			})
			.where(eq(userTable.id, userId));
		
		return true;
	} catch (error) {
		console.error('Error updating profile settings:', error);
		return false;
	}
} 