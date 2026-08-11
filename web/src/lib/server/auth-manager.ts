import { error } from '@sveltejs/kit';
import { and, eq, isNull, or } from 'drizzle-orm';
import { db } from './db';
import { user as userTable, dimManager, leagueMember, type User } from './db/schema';

export interface NotificationPreferences {
	emailOnNewProposal: boolean;
	emailOnVoteResults: boolean;
	emailOnTradeOffers: boolean;
}

export const DEFAULT_NOTIFICATION_PREFERENCES: NotificationPreferences = {
	emailOnNewProposal: true,
	emailOnVoteResults: true,
	emailOnTradeOffers: false
};

export interface AuthenticatedManager {
	userId: string;
	username: string;
	managerKey: number;
	managerName: string;
	managerId: string;
	displayName: string;
	email: string;
	role: 'member' | 'commissioner';
	profileImageUrl?: string;
	notificationPreferences: NotificationPreferences;
	profileSettings: Record<string, unknown>;
}

/**
 * The manager key of the caller, or a 403.
 *
 * There is deliberately no nullable variant of this and no default value. The
 * bug this replaces was `authenticatedManager?.managerKey || 1`, which attributed
 * every proposal from a signed-out visitor to manager_key 1 — who turns out to be
 * '-- hidden --', a retired Yahoo-private manager from 2010. A nullable accessor
 * invites exactly that fallback at every call site, so the nullable accessor is
 * gone.
 *
 * Write paths call this. Read paths that legitimately tolerate anonymity use
 * `locals.member` and handle null explicitly.
 */
export function requireManagerKey(locals: App.Locals): number {
	if (!locals.user) throw error(401, 'You need to be signed in to do that.');
	if (!locals.member?.active) {
		throw error(403, 'Your account is not on the league roster.');
	}
	return locals.member.managerKey;
}

export function requireCommissioner(locals: App.Locals): number {
	const managerKey = requireManagerKey(locals);
	if (locals.member?.role !== 'commissioner') {
		throw error(403, 'Commissioner access required.');
	}
	return managerKey;
}

/**
 * Parse the notification_preferences JSON blob.
 *
 * The column is `text` holding JSON.stringify output. The previous code did
 * `column as any || defaults`, which never parsed and never fell back — a
 * non-empty string is truthy, so every flag read as `undefined` and every
 * preference check silently failed closed.
 */
export function parsePreferences(raw: string | null | undefined): NotificationPreferences {
	if (!raw) return { ...DEFAULT_NOTIFICATION_PREFERENCES };
	try {
		const parsed = JSON.parse(raw);
		if (!parsed || typeof parsed !== 'object') return { ...DEFAULT_NOTIFICATION_PREFERENCES };
		return { ...DEFAULT_NOTIFICATION_PREFERENCES, ...parsed };
	} catch {
		console.warn('[auth-manager] Unparseable notification_preferences; using defaults.');
		return { ...DEFAULT_NOTIFICATION_PREFERENCES };
	}
}

/**
 * Full manager profile for display. Returns null when the user has no linked
 * manager — callers must handle that rather than substituting a default.
 */
export async function getAuthenticatedManager(
	user: Pick<User, 'id'>
): Promise<AuthenticatedManager | null> {
	try {
		const [userData] = await db
			.select({
				userId: userTable.id,
				username: userTable.username,
				userDisplayName: userTable.displayName,
				managerKey: userTable.managerKey,
				notificationPreferences: userTable.notificationPreferences,
				profileSettings: userTable.profileSettings,
				managerName: dimManager.managerName,
				managerId: dimManager.managerId,
				managerDisplayName: dimManager.displayName,
				profileImageUrl: dimManager.profileImageUrl,
				// The allowlist is the authoritative address — it is what a sign-in
				// link is mailed to. dim_manager.email is ETL-written and unreliable.
				memberEmail: leagueMember.email,
				role: leagueMember.role
			})
			.from(userTable)
			.leftJoin(dimManager, eq(userTable.managerKey, dimManager.managerKey))
			.leftJoin(leagueMember, eq(userTable.managerKey, leagueMember.managerKey))
			.where(eq(userTable.id, user.id))
			.limit(1);

		if (!userData?.managerKey || !userData.memberEmail) return null;

		return {
			userId: userData.userId,
			username: userData.username,
			managerKey: userData.managerKey,
			managerName: userData.managerName!,
			managerId: userData.managerId!,
			displayName: userData.managerDisplayName || userData.userDisplayName || userData.managerName!,
			email: userData.memberEmail,
			role: (userData.role as 'member' | 'commissioner') ?? 'member',
			profileImageUrl: userData.profileImageUrl || undefined,
			notificationPreferences: parsePreferences(userData.notificationPreferences),
			profileSettings: parseProfileSettings(userData.profileSettings)
		};
	} catch (err) {
		console.error('Error getting authenticated manager:', err);
		return null;
	}
}

function parseProfileSettings(raw: string | null | undefined): Record<string, unknown> {
	if (!raw) return {};
	try {
		const parsed = JSON.parse(raw);
		return parsed && typeof parsed === 'object' ? parsed : {};
	} catch {
		return {};
	}
}

export async function getManagerNameByUserId(userId: string): Promise<string | null> {
	try {
		const [row] = await db
			.select({ managerName: dimManager.managerName })
			.from(userTable)
			.leftJoin(dimManager, eq(userTable.managerKey, dimManager.managerKey))
			.where(eq(userTable.id, userId))
			.limit(1);

		return row?.managerName || null;
	} catch (err) {
		console.error('Error getting manager name by user ID:', err);
		return null;
	}
}

export async function getManagerNameByKey(managerKey: number): Promise<string | null> {
	try {
		const [row] = await db
			.select({ managerName: dimManager.managerName })
			.from(dimManager)
			.where(eq(dimManager.managerKey, managerKey))
			.limit(1);

		return row?.managerName || null;
	} catch (err) {
		console.error('Error getting manager name:', err);
		return null;
	}
}

/**
 * Current managers with no league_member row yet — the commissioner's "who is
 * still missing from the allowlist" list.
 */
export async function getUnclaimedManagers() {
	// Deliberately unguarded. Returning [] on a query error tells the
	// commissioner every manager is on the allowlist — the exact opposite of the
	// truth, on the one screen that exists to surface who is missing. Let it
	// throw; the surrounding load renders an error page.
	return db
		.select({
			managerKey: dimManager.managerKey,
			managerName: dimManager.managerName,
			displayName: dimManager.displayName,
			profileImageUrl: dimManager.profileImageUrl
		})
		.from(dimManager)
		.leftJoin(leagueMember, eq(dimManager.managerKey, leagueMember.managerKey))
		.where(
			and(
				eq(dimManager.isCurrent, true),
				eq(dimManager.isActive, true),
				isNull(leagueMember.managerKey)
			)
		)
		.orderBy(dimManager.managerName);
}

export async function updateNotificationPreferences(
	userId: string,
	preferences: Partial<NotificationPreferences>
): Promise<boolean> {
	try {
		const [existing] = await db
			.select({ notificationPreferences: userTable.notificationPreferences })
			.from(userTable)
			.where(eq(userTable.id, userId))
			.limit(1);

		// Merge rather than replace: a partial update must not silently reset the
		// flags the caller did not mention.
		const merged = { ...parsePreferences(existing?.notificationPreferences), ...preferences };

		await db
			.update(userTable)
			.set({ notificationPreferences: JSON.stringify(merged), updatedAt: new Date() })
			.where(eq(userTable.id, userId));

		return true;
	} catch (err) {
		console.error('Error updating notification preferences:', err);
		return false;
	}
}

export async function updateProfileSettings(
	userId: string,
	settings: Record<string, unknown>
): Promise<boolean> {
	try {
		await db
			.update(userTable)
			.set({ profileSettings: JSON.stringify(settings), updatedAt: new Date() })
			.where(eq(userTable.id, userId));

		return true;
	} catch (err) {
		console.error('Error updating profile settings:', err);
		return false;
	}
}
