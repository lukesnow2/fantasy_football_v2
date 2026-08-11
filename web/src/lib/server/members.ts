import { and, eq, isNull, sql } from 'drizzle-orm';
import { nanoid } from 'nanoid';
import { db, type Tx } from '$lib/server/db';
import { leagueMember, user as userTable } from '$lib/server/db/schema';

export type MemberRole = 'member' | 'commissioner';

export interface LeagueMember {
	id: string;
	email: string;
	managerKey: number;
	role: MemberRole;
	active: boolean;
	displayName: string | null;
}

const MEMBER_COLUMNS = {
	id: leagueMember.id,
	email: leagueMember.email,
	managerKey: leagueMember.managerKey,
	role: leagueMember.role,
	active: leagueMember.active,
	displayName: leagueMember.displayName
} as const;

/**
 * Look a member up by email, case-insensitively.
 *
 * Matching on `lower(email)` rather than `=` is not cosmetic: a manager who
 * types "Bob@Gmail.com" into the login form when their row says "bob@gmail.com"
 * must resolve to that row, not miss and get silently refused. The matching
 * `lower(email)` unique index is what stops two rows differing only in case from
 * existing in the first place.
 */
export async function findMemberByEmail(
	email: string,
	dbOrTx: Tx = db
): Promise<LeagueMember | null> {
	const address = email?.toLowerCase().trim();
	if (!address) return null;

	const [row] = await dbOrTx
		.select(MEMBER_COLUMNS)
		.from(leagueMember)
		.where(sql`lower(${leagueMember.email}) = ${address}`)
		.limit(1);

	return (row as LeagueMember) ?? null;
}

export async function listActiveMembers(dbOrTx: Tx = db): Promise<LeagueMember[]> {
	const rows = await dbOrTx
		.select(MEMBER_COLUMNS)
		.from(leagueMember)
		.where(eq(leagueMember.active, true))
		.orderBy(leagueMember.managerKey);

	return rows as LeagueMember[];
}

/**
 * The number a constitutional threshold is measured against.
 *
 * Derived from the allowlist rather than from a hardcoded 10 or from
 * edw.dim_manager. dim_manager is written by the ETL, so sourcing it there would
 * let a data pipeline run silently change what counts as a super-majority.
 */
export async function countEligibleVoters(dbOrTx: Tx = db): Promise<number> {
	const [row] = await dbOrTx
		.select({ n: sql<number>`count(*)::int` })
		.from(leagueMember)
		.where(eq(leagueMember.active, true));

	return row?.n ?? 0;
}

/**
 * Turn a manager name into a stable, URL-safe handle.
 *
 * Matches the keys already used for profile photos in utils/managerUtils.ts, so
 * "Gabe the Younger" becomes "gabe-the-younger" in both places.
 */
export function slugifyManagerName(name: string): string {
	return name
		.toLowerCase()
		.normalize('NFKD')
		.replace(/[̀-ͯ]/g, '')
		.replace(/[^a-z0-9]+/g, '-')
		.replace(/^-+|-+$/g, '');
}

/**
 * Resolve the app.user row backing a member, creating it on first sign-in.
 *
 * Users are never auto-provisioned from an email address alone — this is only
 * ever reached with a member already resolved from the allowlist. That is what
 * makes the league invite-only rather than open-registration.
 */
export async function getOrCreateUserForMember(
	tx: Tx,
	member: LeagueMember
): Promise<{ userId: string }> {
	const [existing] = await tx
		.select({ id: userTable.id })
		.from(userTable)
		.where(eq(userTable.managerKey, member.managerKey))
		.limit(1);

	if (existing) {
		// Keep the login address authoritative: the allowlist is the source of
		// truth, so a commissioner correcting an address there must not leave the
		// user row pointing at the old one.
		await tx
			.update(userTable)
			.set({ email: member.email, updatedAt: new Date() })
			.where(eq(userTable.id, existing.id));

		return { userId: existing.id };
	}

	const baseUsername = slugifyManagerName(member.displayName || member.email.split('@')[0]);

	const [created] = await tx
		.insert(userTable)
		.values({
			id: nanoid(),
			username: baseUsername,
			managerKey: member.managerKey,
			email: member.email,
			displayName: member.displayName,
			accountStatus: 'active'
		})
		.returning({ id: userTable.id });

	return { userId: created.id };
}

/**
 * Stamp the first successful sign-in, once.
 *
 * The "only if not already set" lives in the WHERE clause rather than in a
 * coalesce() expression: passing a Date into a raw SQL fragment hands
 * postgres.js a value it cannot infer a type for, which fails at bind time with
 * an unhelpful ERR_INVALID_ARG_TYPE.
 */
export async function markFirstLogin(tx: Tx, memberId: string, now: Date): Promise<void> {
	await tx
		.update(leagueMember)
		.set({ firstLoginAt: now, updatedAt: now })
		.where(and(eq(leagueMember.id, memberId), isNull(leagueMember.firstLoginAt)));
}
