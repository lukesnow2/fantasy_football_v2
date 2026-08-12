import { json } from '@sveltejs/kit';
import { asc, eq } from 'drizzle-orm';
import { db } from '$lib/server/db';
import { chatCustomEmoji } from '$lib/server/db/schema';
import { requireManagerKey } from '$lib/server/auth-manager';
import type { RequestHandler } from './$types';

/**
 * Custom emoji, read-only.
 *
 * The write handlers that used to live here required the caller to supply an
 * already-hosted `imageUrl` — there is no upload path, no blob storage and no UI
 * that could produce one, and the table has never held a row. They were ~180
 * lines of surface area implementing nothing. When uploads exist, the endpoint
 * that stores the file can bring its own POST back.
 *
 * GET stays because `emoji-picker-element` takes a `customEmoji` array, so the
 * moment there is a row to serve the picker can use it.
 */
export const GET: RequestHandler = async ({ locals }) => {
	requireManagerKey(locals);

	const emojis = await db
		.select({
			emojiId: chatCustomEmoji.emojiId,
			name: chatCustomEmoji.name,
			imageUrl: chatCustomEmoji.imageUrl,
			category: chatCustomEmoji.category
		})
		.from(chatCustomEmoji)
		.where(eq(chatCustomEmoji.isActive, true))
		.orderBy(asc(chatCustomEmoji.name));

	// Shaped for emoji-picker-element's customEmoji property: { name, shortcodes, url }.
	return json({
		customEmoji: emojis.map((e) => ({
			name: e.name,
			shortcodes: [e.name],
			url: e.imageUrl,
			category: e.category ?? 'custom'
		}))
	});
};
