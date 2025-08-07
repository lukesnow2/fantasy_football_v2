import { json } from '@sveltejs/kit';
import { WebAuthnService } from '$lib/server/webauthn/service';
import { webauthnConfig } from '$lib/server/webauthn/config';
import { getValidationConfigForEnvironment, validateOrigin } from '$lib/server/webauthn/validation';
import { generateSessionToken, createSession, setSessionTokenCookie } from '$lib/server/auth';

export async function POST({ request, getClientAddress, cookies }: { request: Request; getClientAddress: () => string; cookies: any }) {
	try {
		const { response, challengeId } = await request.json();

		if (!response || !challengeId) {
			return json({ error: 'Missing authentication response or challenge ID' }, { status: 400 });
		}

		const origin = request.headers.get('origin') || webauthnConfig.rpOrigin;
		const originValidation = validateOrigin(origin, getValidationConfigForEnvironment());
		if (!originValidation.valid) {
			return json({ error: 'Origin not allowed' }, { status: 400 });
		}

		const result = await WebAuthnService.completeAuthentication(response, challengeId, origin);
		if (!result.success) {
			return json({ error: 'Authentication verification failed' }, { status: 400 });
		}

		// Create a session on successful authentication
		const sessionToken = generateSessionToken();
		const session = await createSession(sessionToken, result.userId);
		setSessionTokenCookie({ cookies } as any, sessionToken, session.expiresAt);

		return json({ success: true, userId: result.userId });
	} catch (error) {
		return json({ error: error instanceof Error ? error.message : 'Authentication verification failed' }, { status: 400 });
	}
} 