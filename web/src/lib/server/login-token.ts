import { and, eq, gt, isNull, lt } from 'drizzle-orm';
import { sha256 } from '@oslojs/crypto/sha2';
import { encodeBase64url, encodeHexLowerCase } from '@oslojs/encoding';
import { db, type Tx } from '$lib/server/db';
import { loginToken } from '$lib/server/db/schema';

/**
 * 15 minutes. Short on purpose: a magic link in an inbox is a bearer credential,
 * and a mailbox that is later compromised should not hand over a working key. A
 * manager who takes too long just asks for another one.
 */
export const LOGIN_TOKEN_TTL_MS = 15 * 60 * 1000;

export type LoginTokenPurpose = 'login' | 'invite';

export interface LoginTokenRow {
	tokenHash: string;
	email: string;
	purpose: string;
	redirectTo: string | null;
	expiresAt: Date;
	consumedAt: Date | null;
}

export type ConsumeResult =
	| { ok: true; row: LoginTokenRow }
	| { ok: false; reason: 'not_found' | 'expired' | 'consumed' };

/** 32 bytes from the CSPRNG. Never `Math.random()`. */
export function generateLoginToken(): string {
	return encodeBase64url(crypto.getRandomValues(new Uint8Array(32)));
}

/** Same primitive the session store uses: the raw token never touches the DB. */
export function hashLoginToken(token: string): string {
	return encodeHexLowerCase(sha256(new TextEncoder().encode(token)));
}

export async function issueLoginToken(input: {
	email: string;
	purpose: LoginTokenPurpose;
	redirectTo: string | null;
	ip: string | null;
	userAgent: string | null;
}): Promise<{ token: string; expiresAt: Date }> {
	const token = generateLoginToken();
	const expiresAt = new Date(Date.now() + LOGIN_TOKEN_TTL_MS);

	await db.insert(loginToken).values({
		tokenHash: hashLoginToken(token),
		email: input.email.toLowerCase().trim(),
		purpose: input.purpose,
		redirectTo: input.redirectTo,
		expiresAt,
		requestIp: input.ip,
		userAgent: input.userAgent
	});

	return { token, expiresAt };
}

/**
 * Redeem a token, atomically.
 *
 * The single-use guard is the WHERE clause, not a read followed by a write: two
 * concurrent requests carrying the same token (a double-click, or a mail client
 * that prefetches the link while the human also clicks it) both reach the
 * UPDATE, and Postgres lets exactly one of them match `consumed_at IS NULL`. A
 * read-then-write would let both observe an unconsumed row and mint two
 * sessions.
 *
 * Takes a `Tx` so the caller can create the session in the same transaction.
 */
export async function consumeLoginToken(
	tx: Tx,
	token: string,
	now: Date
): Promise<ConsumeResult> {
	const tokenHash = hashLoginToken(token);

	const [claimed] = await tx
		.update(loginToken)
		.set({ consumedAt: now })
		.where(
			and(
				eq(loginToken.tokenHash, tokenHash),
				isNull(loginToken.consumedAt),
				gt(loginToken.expiresAt, now)
			)
		)
		.returning();

	if (claimed) return { ok: true, row: claimed as LoginTokenRow };

	// Nothing was claimed. Re-read to tell the three failure modes apart — this
	// is only ever used to pick the wording on the failure page. Reaching this
	// point requires already holding a token, so there is no roster to leak.
	const [existing] = await tx
		.select({ consumedAt: loginToken.consumedAt, expiresAt: loginToken.expiresAt })
		.from(loginToken)
		.where(eq(loginToken.tokenHash, tokenHash))
		.limit(1);

	if (!existing) return { ok: false, reason: 'not_found' };
	if (existing.consumedAt) return { ok: false, reason: 'consumed' };
	return { ok: false, reason: 'expired' };
}

/**
 * Drop tokens that are past their expiry.
 *
 * Consumed-but-unexpired rows are deliberately kept: they are what lets a second
 * click report "this link was already used" rather than "invalid link", which is
 * the difference between a manager understanding what happened and filing a bug.
 * They age out on the same 15-minute clock as everything else.
 */
export async function purgeExpiredLoginTokens(now: Date = new Date()): Promise<number> {
	const deleted = await db
		.delete(loginToken)
		.where(lt(loginToken.expiresAt, now))
		.returning({ tokenHash: loginToken.tokenHash });
	return deleted.length;
}
