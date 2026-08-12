import { GROUPING_WINDOW_MS } from './constants';
import type { ChatMessageView, ReactionGroup } from './types';

/** Pure formatting helpers, kept out of components so they can be tested in node. */

/** `YYYY-MM-DD` in the viewer's own zone — the key a day divider groups on. */
export function dayKey(timestamp: number): string {
	const date = new Date(timestamp);
	const month = `${date.getMonth() + 1}`.padStart(2, '0');
	const day = `${date.getDate()}`.padStart(2, '0');
	return `${date.getFullYear()}-${month}-${day}`;
}

/** `Today` / `Yesterday` / weekday within the last week / full date. */
export function formatDayDivider(timestamp: number, now: number = Date.now()): string {
	const today = dayKey(now);
	const yesterday = dayKey(now - 24 * 60 * 60 * 1000);
	const key = dayKey(timestamp);

	if (key === today) return 'Today';
	if (key === yesterday) return 'Yesterday';

	const date = new Date(timestamp);
	const ageDays = (now - timestamp) / (24 * 60 * 60 * 1000);
	if (ageDays < 7) {
		return date.toLocaleDateString(undefined, { weekday: 'long' });
	}
	return date.toLocaleDateString(undefined, {
		month: 'long',
		day: 'numeric',
		year: date.getFullYear() === new Date(now).getFullYear() ? undefined : 'numeric'
	});
}

/** `14:32` — the per-message stamp, shown in full rows and on hover in compact ones. */
export function formatMessageTime(timestamp: number): string {
	return new Date(timestamp).toLocaleTimeString(undefined, {
		hour: '2-digit',
		minute: '2-digit'
	});
}

/** "just now" / "4m" / "3h" / a date. Used for thread footers. */
export function formatRelative(timestamp: number, now: number = Date.now()): string {
	const seconds = Math.max(0, Math.round((now - timestamp) / 1000));
	if (seconds < 45) return 'just now';
	const minutes = Math.round(seconds / 60);
	if (minutes < 60) return `${minutes}m ago`;
	const hours = Math.round(minutes / 60);
	if (hours < 24) return `${hours}h ago`;
	const days = Math.round(hours / 24);
	if (days < 7) return `${days}d ago`;
	return new Date(timestamp).toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

/**
 * Markdown source to something readable in one line.
 *
 * For the preview panel on the public This Season page, which shows a glance at
 * the chat and should not pull marked + DOMPurify onto a public route just to
 * avoid printing literal asterisks.
 */
export function toPlainText(source: string): string {
	return source
		.replace(/```[\s\S]*?```/g, '[code]')
		.replace(/`([^`]+)`/g, '$1')
		.replace(/!\[[^\]]*\]\([^)]*\)/g, '')
		.replace(/\[([^\]]+)\]\([^)]*\)/g, '$1')
		.replace(/(\*\*|__)(.*?)\1/g, '$2')
		.replace(/(\*|_)(.*?)\1/g, '$2')
		.replace(/~~(.*?)~~/g, '$1')
		.replace(/^\s*[>*-]\s+/gm, '')
		.replace(/\s+/g, ' ')
		.trim();
}

/**
 * Should `next` render as a continuation of `prev` — no avatar, no name?
 *
 * Every clause matters. Same author and inside the window are the obvious ones;
 * the day check stops a message at 23:59 absorbing one at 00:01, and the deleted
 * and system checks stop a tombstone or a join notice being silently folded into
 * someone's block.
 */
export function shouldGroupWith(
	prev: ChatMessageView | null | undefined,
	next: ChatMessageView
): boolean {
	if (!prev) return false;
	if (prev.authorKey !== next.authorKey) return false;
	if (prev.deletedAt !== null || next.deletedAt !== null) return false;
	if (prev.messageType !== 'message' || next.messageType !== 'message') return false;
	if (next.createdAt - prev.createdAt > GROUPING_WINDOW_MS) return false;
	if (dayKey(prev.createdAt) !== dayKey(next.createdAt)) return false;
	return true;
}

/**
 * "Trevor, Nick and you reacted with 🔥".
 *
 * "you" always goes last, however the server ordered the users — reading your own
 * name first in a list of other people is jarring.
 */
export function formatReactionTooltip(group: ReactionGroup, meManagerKey: number | null): string {
	const others = group.users.filter((u) => u.managerKey !== meManagerKey).map((u) => u.name);
	const includesMe = group.users.some((u) => u.managerKey === meManagerKey);

	const names = [...others];
	if (includesMe) names.push('you');

	let subject: string;
	if (names.length === 0) {
		subject = 'Someone';
	} else if (names.length === 1) {
		subject = names[0];
	} else if (names.length <= 3) {
		subject = `${names.slice(0, -1).join(', ')} and ${names[names.length - 1]}`;
	} else {
		const shown = names.slice(0, 2).join(', ');
		const remaining = names.length - 2;
		subject = `${shown} and ${remaining} other${remaining === 1 ? '' : 's'}`;
	}

	// "You reacted", not "you reacted", when it opens the sentence.
	const opener = subject === 'you' ? 'You' : subject;
	return `${opener} reacted with ${group.emoji}`;
}
