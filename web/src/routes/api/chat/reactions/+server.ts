import { json } from '@sveltejs/kit';
import { requireManagerKey } from '$lib/server/auth-manager';
import { toggleReaction } from '$lib/server/chat/queries';
import { parseChannelId, parseEmoji, parseExistingMessageId } from '$lib/server/chat/validate';
import { consumeChatReactionBudget } from '$lib/server/rate-limit';
import type { RequestHandler } from './$types';

/**
 * Reactions: one verb, toggling.
 *
 * The previous version had POST, DELETE and GET. POST answered a repeated
 * reaction with a 400 that the client checked for and silently ignored, and the
 * client had to know which direction it was going and then re-GET to find out
 * what happened — two round trips before the pill count moved.
 *
 * Now the server decides add-or-remove from the current state and returns the
 * message's complete grouped reactions, so the common case never waits for a
 * poll. There is no GET: reactions ride along with the messages payload.
 */
export const POST: RequestHandler = async ({ request, locals }) => {
	const me = requireManagerKey(locals);

	if (!consumeChatReactionBudget(me)) {
		return json(
			{ error: 'Slow down a moment.' },
			{ status: 429, headers: { 'Retry-After': '60' } }
		);
	}

	const body = await request.json();
	const channelId = parseChannelId(body?.channelId);
	const messageId = parseExistingMessageId(body?.messageId);
	const { emoji, emojiType } = parseEmoji(body?.emoji, body?.emojiType);

	const result = await toggleReaction({ channelId, messageId, me, emoji, emojiType });
	if (!result) return json({ error: 'Message not found.' }, { status: 404 });

	return json(result);
};
