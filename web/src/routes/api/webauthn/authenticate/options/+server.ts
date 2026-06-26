import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { webauthnCredentials, user } from '$lib/server/db/schema';
import { WebAuthnService } from '$lib/server/webauthn/service';
import { webauthnConfig } from '$lib/server/webauthn/config';
import { getValidationConfigForEnvironment, validateOrigin, extractRelyingPartyId } from '$lib/server/webauthn/validation';
import { createWebAuthnError, WebAuthnErrorCode } from '$lib/server/webauthn/errors';
import { checkMultiLevelRateLimit, getRateLimitHeaders } from '$lib/server/webauthn/rate-limiting';
import { eq } from 'drizzle-orm';

export async function GET() {
	return json({ message: 'WebAuthn authenticate options endpoint is working' });
}

export async function POST({ request, getClientAddress }: { request: Request; getClientAddress: () => string }) {
	try {
		const { username } = await request.json();
		const ip = getClientAddress();

		if (!username) {
			throw createWebAuthnError(WebAuthnErrorCode.INVALID_USER_ID, 'Username is required for authentication');
		}

		// Rate limit by IP and (later) user
		const rate = await checkMultiLevelRateLimit(ip, undefined, 'auth');
		if (!rate.allowed) {
			return new Response(JSON.stringify({ error: 'Too many attempts. Please try again later.' }), {
				status: 429,
				headers: { 'Content-Type': 'application/json', ...getRateLimitHeaders(rate) }
			});
		}

		const origin = request.headers.get('origin') || webauthnConfig.rpOrigin;
		const originValidation = validateOrigin(origin, getValidationConfigForEnvironment());
		if (!originValidation.valid) {
			throw createWebAuthnError(WebAuthnErrorCode.ORIGIN_NOT_ALLOWED, 'Origin not allowed');
		}
		const rpId = extractRelyingPartyId(origin);
		if (rpId !== webauthnConfig.rpID) {
			throw createWebAuthnError(WebAuthnErrorCode.CONFIGURATION_ERROR, 'RP ID mismatch');
		}

		const userQuery = await db
			.select({ id: user.id, username: user.username })
			.from(user)
			.where(eq(user.username, username));
		if (userQuery.length === 0) {
			throw createWebAuthnError(WebAuthnErrorCode.INVALID_USER_ID, `No account found for username: ${username}`);
		}
		const userRecord = userQuery[0];

		const { options, challengeId } = await WebAuthnService.startAuthentication(userRecord.id);

		const existingCredentials = await db
			.select({ id: webauthnCredentials.credentialId, transports: webauthnCredentials.transports })
			.from(webauthnCredentials)
			.where(eq(webauthnCredentials.userId, userRecord.id));

		if (existingCredentials.length > 0) {
			(options as any).allowCredentials = existingCredentials.map((cred) => ({
				id: cred.id,
				type: 'public-key',
				transports: (cred.transports as any) || undefined
			}));
		}

		const flow = existingCredentials.length > 0 ? 'authentication' : 'registration';

		return new Response(JSON.stringify({
			options,
			challengeId,
			flow,
			userId: userRecord.id,
			managerKey: undefined,
			message: flow === 'authentication' ? `Sign in as ${userRecord.username}` : `Set up your first passkey for ${userRecord.username}`
		}), {
			status: 200,
			headers: { 'Content-Type': 'application/json', ...getRateLimitHeaders(rate) }
		});
	} catch (error) {
		return json({ error: error instanceof Error ? error.message : 'Failed to generate authentication options' }, { status: 400 });
	}
} 