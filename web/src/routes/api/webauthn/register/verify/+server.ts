import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { webauthnChallenges } from '$lib/server/db/schema';
import { WebAuthnService } from '$lib/server/webauthn/service';
import { webauthnConfig } from '$lib/server/webauthn/config';
import { getValidationConfigForEnvironment, validateOrigin } from '$lib/server/webauthn/validation';
import { eq } from 'drizzle-orm';

export async function POST({ request }: { request: Request }) {
	try {
		const { response, challengeId, username } = await request.json();
		if (!response || !challengeId) {
			return json({ error: 'Missing registration response or challenge ID' }, { status: 400 });
		}

		const origin = request.headers.get('origin') || webauthnConfig.rpOrigin;
		const originValidation = validateOrigin(origin, getValidationConfigForEnvironment());
		if (!originValidation.valid) {
			return json({ error: 'Origin not allowed' }, { status: 400 });
		}

		// Find userId from challenge record
		const [challengeRecord] = await db
			.select({ id: webauthnChallenges.id, userId: webauthnChallenges.userId })
			.from(webauthnChallenges)
			.where(eq(webauthnChallenges.id, challengeId))
			.limit(1);
		if (!challengeRecord?.userId) {
			return json({ error: 'Challenge record not found' }, { status: 400 });
		}

		const result = await WebAuthnService.completeRegistration(response, challengeId, challengeRecord.userId, origin);
		if (!result.success) {
			return json({ error: 'Registration verification failed' }, { status: 400 });
		}

		return json({ success: true, userId: challengeRecord.userId });
	} catch (error) {
		return json({ error: error instanceof Error ? error.message : 'Registration verification failed' }, { status: 400 });
	}
} 