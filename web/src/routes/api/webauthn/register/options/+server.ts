import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { user } from '$lib/server/db/schema';
import { WebAuthnService } from '$lib/server/webauthn/service';
import { webauthnConfig } from '$lib/server/webauthn/config';
import { getValidationConfigForEnvironment, validateOrigin, extractRelyingPartyId } from '$lib/server/webauthn/validation';
import { createWebAuthnError, WebAuthnErrorCode } from '$lib/server/webauthn/errors';
import { checkMultiLevelRateLimit, getRateLimitHeaders } from '$lib/server/webauthn/rate-limiting';
import { eq } from 'drizzle-orm';

export async function GET() {
	return json({ message: 'WebAuthn register options endpoint is working' });
}

export async function POST({ request, getClientAddress }: { request: Request; getClientAddress: () => string }) {
	try {
		const { username } = await request.json();
		if (!username) {
			throw createWebAuthnError(WebAuthnErrorCode.INVALID_USER_ID, 'Username is required for registration');
		}

		const ip = getClientAddress();
		const rate = await checkMultiLevelRateLimit(ip, undefined, 'registration');
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

		const result = await db
			.select({ id: user.id, username: user.username })
			.from(user)
			.where(eq(user.username, username));
		if (result.length === 0) {
			throw createWebAuthnError(WebAuthnErrorCode.INVALID_USER_ID, `No account found for username: ${username}`);
		}
		const record = result[0];

		const { options, challengeId } = await WebAuthnService.startRegistration(record.id, record.username);
		return new Response(JSON.stringify({ options, challengeId }), {
			status: 200,
			headers: { 'Content-Type': 'application/json', ...getRateLimitHeaders(rate) }
		});
	} catch (error) {
		return json({ error: error instanceof Error ? error.message : 'Failed to generate registration options' }, { status: 400 });
	}
} 