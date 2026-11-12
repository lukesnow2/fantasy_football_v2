import { json } from '@sveltejs/kit';
import { requireManagerKey } from '$lib/server/auth-manager';
import { upsertRead } from '$lib/server/chat/queries';
import { parseChannelId, parseOptionalKey } from '$lib/server/chat/validate';
import type { RequestHandler } from './$types';

/**
 * The last-read watermark, which drives the "New" divider.
 *
 * Reached two ways: a debounced fetch while reading, and a `sendBeacon` on
 * pagehide (fetch during pagehide is unreliable). Beacons arrive as a Blob with
 * no JSON content type, hence the tolerant body parse.
 *
 * The upsert takes GREATEST of old and new, so a beacon that lands late from a
 * background tab cannot rewind a watermark a foreground tab already advanced.
 */
export const PUT: RequestHandler = async ({ request, locals }) => {
	const me = requireManagerKey(locals);

	let body: Record<string, unknown> = {};
	try {
		body = JSON.parse(await request.text());
	} catch {
		return json({ error: 'Invalid body.' }, { status: 400 });
	}

	const channelId = parseChannelId(typeof body.channelId === 'string' ? body.channelId : null);
	const lastReadMessageKey = parseOptionalKey(body.lastReadMessageKey);
	if (lastReadMessageKey === null) {
		return json({ error: 'lastReadMessageKey is required.' }, { status: 400 });
	}

	await upsertRead({ managerKey: me, channelId, lastReadMessageKey });
	return json({ ok: true });
};

// POST is accepted because navigator.sendBeacon can only issue POSTs.
export const POST = PUT;
