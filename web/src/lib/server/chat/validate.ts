import { error } from '@sveltejs/kit';
import {
	CHANNELS,
	DEFAULT_CHANNEL,
	MAX_EMOJI_BYTES,
	MAX_MESSAGE_LENGTH
} from '$lib/chat/constants';
import type { EmojiType } from '$lib/chat/types';

/**
 * Request validation for the chat endpoints.
 *
 * Everything here throws a typed SvelteKit error rather than returning a flag —
 * the handlers are thin on purpose, and a validator whose result can be ignored
 * eventually is.
 */

const graphemeSegmenter =
	typeof Intl !== 'undefined' && 'Segmenter' in Intl
		? new Intl.Segmenter(undefined, { granularity: 'grapheme' })
		: null;

/**
 * Length as a human would count it.
 *
 * `'👨‍👩‍👧‍👦'.length` is 11 — four people and three zero-width joiners — but it is
 * one character on screen. Counting UTF-16 units would let a cap of 2000 reject
 * a message of 200 emoji.
 */
export function messageLength(content: string): number {
	if (!graphemeSegmenter) return [...content].length;
	let count = 0;
	for (const _ of graphemeSegmenter.segment(content)) count++;
	return count;
}

export function parseChannelId(raw: string | null | undefined): string {
	const value = raw ?? DEFAULT_CHANNEL;
	if (!(CHANNELS as readonly string[]).includes(value)) {
		throw error(400, 'Unknown channel.');
	}
	return value;
}

export function parseContent(raw: unknown): string {
	if (typeof raw !== 'string') throw error(400, 'Message content is required.');

	const content = raw
		.replace(/\r\n/g, '\n')
		// Strip control characters except newline and tab. A lone NUL makes
		// Postgres reject the insert outright, with an error nobody can act on.
		.replace(/[\u0000-\u0008\u000B-\u001F\u007F]/g, '')
		.trim();

	if (content === '') throw error(400, 'Message content is required.');
	if (messageLength(content) > MAX_MESSAGE_LENGTH) {
		throw error(400, `Messages are capped at ${MAX_MESSAGE_LENGTH} characters.`);
	}
	return content;
}

/**
 * The client mints message ids so its optimistic row and the polled row share an
 * identity. Bounded and shaped here so `message_id` (varchar(100)) can't be used
 * as a scratch column.
 */
const MESSAGE_ID_PATTERN = /^m_[A-Za-z0-9_-]{21}$/;

export function parseClientMessageId(raw: unknown): string {
	if (typeof raw !== 'string' || !MESSAGE_ID_PATTERN.test(raw)) {
		throw error(400, 'Invalid message id.');
	}
	return raw;
}

/** Ids already in the table predate the client-minted format; only new ones are checked. */
export function parseExistingMessageId(raw: unknown): string {
	if (typeof raw !== 'string' || raw === '' || raw.length > 100) {
		throw error(400, 'Invalid message id.');
	}
	return raw;
}

/**
 * Does this look like a reaction rather than a sentence?
 *
 * Three accepted openings, because `\p{Extended_Pictographic}` alone is not
 * enough:
 *   - pictographic: 🔥, and any ZWJ sequence built from one (👨‍👩‍👧‍👦)
 *   - regional indicators: flags are pairs of these and match no pictographic
 *     property at all, so an Extended_Pictographic-only check rejected 🇺🇸
 *   - keycaps: 1️⃣ and #️⃣ begin with a plain ASCII character
 *
 * The grapheme cap is what actually keeps prose out.
 */
function looksLikeEmoji(value: string): boolean {
	if (messageLength(value) > 8) return false;
	return (
		/^\p{Extended_Pictographic}/u.test(value) ||
		/^\p{Regional_Indicator}/u.test(value) ||
		/^[0-9#*]️?⃣/u.test(value)
	);
}

export function parseEmoji(
	rawEmoji: unknown,
	rawType: unknown
): { emoji: string; emojiType: EmojiType } {
	const emojiType: EmojiType = rawType === 'custom' ? 'custom' : 'unicode';

	if (typeof rawEmoji !== 'string' || rawEmoji === '') {
		throw error(400, 'Emoji is required.');
	}
	if (new TextEncoder().encode(rawEmoji).length > MAX_EMOJI_BYTES) {
		throw error(400, 'Emoji is too long.');
	}

	if (emojiType === 'custom') {
		if (!/^[a-z0-9_]{1,50}$/.test(rawEmoji)) throw error(400, 'Invalid custom emoji name.');
	} else {
		if (!looksLikeEmoji(rawEmoji)) throw error(400, 'That is not an emoji.');
	}

	return { emoji: rawEmoji, emojiType };
}

export function parseIntParam(
	raw: string | null,
	fallback: number,
	min: number,
	max: number
): number {
	const parsed = Number.parseInt(raw ?? '', 10);
	if (!Number.isFinite(parsed)) return fallback;
	return Math.min(max, Math.max(min, parsed));
}

export function parseOptionalKey(raw: unknown): number | null {
	if (raw === null || raw === undefined || raw === '') return null;
	const parsed = Number(raw);
	if (!Number.isInteger(parsed) || parsed <= 0) throw error(400, 'Invalid message key.');
	return parsed;
}
