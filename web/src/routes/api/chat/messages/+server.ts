import { json } from '@sveltejs/kit';
import { PAGE_SIZE } from '$lib/chat/constants';
import { findMentions } from '$lib/chat/mentions';
import { requireManagerKey } from '$lib/server/auth-manager';
import {
	insertMessage,
	selectDelta,
	selectLastRead,
	selectMessageById,
	selectReplies,
	selectRoster,
	selectRootMessages,
	softDeleteMessage,
	updateMessage
} from '$lib/server/chat/queries';
import {
	parseChannelId,
	parseClientMessageId,
	parseContent,
	parseExistingMessageId,
	parseIntParam,
	parseOptionalKey
} from '$lib/server/chat/validate';
import { consumeChatEditBudget, consumeChatPostBudget } from '$lib/server/rate-limit';
import type { RequestHandler } from './$types';

/**
 * Chat messages.
 *
 * `hooks.server.ts` already 401s every /api/chat request from a signed-out
 * caller. `requireManagerKey` is called here anyway: a guard you cannot see from
 * the file it protects is a guard someone eventually deletes.
 *
 * GET has three modes, chosen by query parameter:
 *   (none)            first load — messages, roster, read watermark, cursor
 *   ?since=&sinceTs=  poll — what changed, plus full reaction state
 *   ?parent=          one thread's replies
 *   ?before=          older history, for scrolling up
 */

const RETRY_AFTER_SECONDS = 60;

function tooManyRequests(message: string) {
	return json({ error: message }, { status: 429, headers: { 'Retry-After': `${RETRY_AFTER_SECONDS}` } });
}

export const GET: RequestHandler = async ({ url, locals }) => {
	const me = requireManagerKey(locals);
	const channelId = parseChannelId(url.searchParams.get('channelId'));

	const parentParam = url.searchParams.get('parent');
	if (parentParam !== null) {
		const parentMessageKey = parseOptionalKey(parentParam);
		if (parentMessageKey === null) return json({ error: 'Invalid parent.' }, { status: 400 });
		return json({
			mode: 'thread',
			channelId,
			parentMessageKey,
			messages: await selectReplies({ channelId, me, parentMessageKey })
		});
	}

	const sinceParam = url.searchParams.get('since');
	if (sinceParam !== null) {
		const sinceKey = parseIntParam(sinceParam, 0, 0, Number.MAX_SAFE_INTEGER);
		// Falls back to "now" rather than 0 when absent: a sinceTs of 0 would make
		// the edit arm of the delta match every message in the channel, every poll.
		const sinceTs = parseIntParam(
			url.searchParams.get('sinceTs'),
			Date.now(),
			0,
			Number.MAX_SAFE_INTEGER
		);
		const windowSize = parseIntParam(url.searchParams.get('window'), PAGE_SIZE, 1, 200);

		const delta = await selectDelta({ channelId, me, sinceKey, sinceTs, window: windowSize });
		return json({
			mode: 'delta',
			channelId,
			messages: delta.messages,
			reactions: delta.reactions,
			cursor: delta.cursor,
			serverTime: Date.now()
		});
	}

	const limit = parseIntParam(url.searchParams.get('limit'), PAGE_SIZE, 1, 200);
	const beforeKey = parseOptionalKey(url.searchParams.get('before'));

	// Scrolling up: history only, no roster or cursor churn.
	if (beforeKey !== null) {
		const page = await selectRootMessages({ channelId, me, limit, beforeKey });
		return json({ mode: 'history', channelId, messages: page.messages, hasMore: page.hasMore });
	}

	const [page, roster, lastReadMessageKey] = await Promise.all([
		selectRootMessages({ channelId, me, limit }),
		selectRoster(),
		selectLastRead({ managerKey: me, channelId })
	]);

	const newest = page.messages[page.messages.length - 1];

	return json({
		mode: 'bootstrap',
		channelId,
		messages: page.messages,
		hasMore: page.hasMore,
		roster,
		lastReadMessageKey,
		cursor: { messageKey: newest?.messageKey ?? 0, ts: Date.now() },
		serverTime: Date.now(),
		me: {
			managerKey: me,
			displayName: roster.find((r) => r.managerKey === me)?.displayName ?? 'You'
		}
	});
};

export const POST: RequestHandler = async ({ request, locals }) => {
	const me = requireManagerKey(locals);

	if (!consumeChatPostBudget(me)) {
		return tooManyRequests('You are sending messages too quickly. Try again in a minute.');
	}

	const body = await request.json();
	const channelId = parseChannelId(body?.channelId);
	const messageId = parseClientMessageId(body?.messageId);
	const content = parseContent(body?.content);
	const parentMessageKey = parseOptionalKey(body?.parentMessageKey);

	// Mentions are derived here, never taken from the request. A client that gets
	// to declare who it mentioned gets to make the server email anyone it likes.
	const mentions = findMentions(content, await selectRoster());

	const outcome = await insertMessage({
		messageId,
		channelId,
		content,
		authorKey: me,
		parentMessageKey,
		mentions
	});

	if (outcome.status === 'bad-parent') {
		return json({ error: 'That message is no longer available to reply to.' }, { status: 400 });
	}

	const message = await selectMessageById({ channelId, me, messageId });
	if (!message) return json({ error: 'Failed to create message' }, { status: 500 });

	// A duplicate is a retry, not an error: same id, same row, 200. The client
	// cannot tell the difference and should not have to.
	return json({ message }, { status: outcome.status === 'created' ? 201 : 200 });
};

export const PATCH: RequestHandler = async ({ request, locals }) => {
	const me = requireManagerKey(locals);

	if (!consumeChatEditBudget(me)) {
		return tooManyRequests('You are editing too quickly. Try again in a minute.');
	}

	const body = await request.json();
	const channelId = parseChannelId(body?.channelId);
	const messageId = parseExistingMessageId(body?.messageId);
	const content = parseContent(body?.content);

	const mentions = findMentions(content, await selectRoster());
	const updated = await updateMessage({ messageId, channelId, authorKey: me, content, mentions });

	if (!updated) {
		return json({ error: 'Message not found, or not yours to edit.' }, { status: 404 });
	}

	const message = await selectMessageById({ channelId, me, messageId });
	return json({ message });
};

export const DELETE: RequestHandler = async ({ request, locals }) => {
	const me = requireManagerKey(locals);

	const body = await request.json();
	const channelId = parseChannelId(body?.channelId);
	const messageId = parseExistingMessageId(body?.messageId);

	const deleted = await softDeleteMessage({ messageId, channelId, authorKey: me });
	if (!deleted) {
		return json({ error: 'Message not found, or not yours to delete.' }, { status: 404 });
	}

	// The tombstone comes back rather than a bare success, so the client can
	// render "message deleted" in place instead of yanking the row out from under
	// whatever the reader was looking at.
	const message = await selectMessageById({ channelId, me, messageId });
	return json({ message });
};
