import { describe, expect, it } from 'vitest';
import {
	dayKey,
	formatDayDivider,
	formatReactionTooltip,
	formatRelative,
	shouldGroupWith,
	toPlainText
} from './format';
import { GROUPING_WINDOW_MS } from './constants';
import type { ChatMessageView, ReactionGroup } from './types';

function message(overrides: Partial<ChatMessageView> = {}): ChatMessageView {
	return {
		messageKey: 1,
		messageId: 'm_aaaaaaaaaaaaaaaaaaaaa',
		content: 'hi',
		channelId: 'general',
		parentMessageKey: null,
		messageType: 'message',
		authorKey: 14,
		authorName: 'Luke S',
		authorDisplayName: 'Luke S',
		authorProfileImage: null,
		mentions: [],
		createdAt: 1_700_000_000_000,
		updatedAt: 1_700_000_000_000,
		editedAt: null,
		deletedAt: null,
		reactions: [],
		replyCount: 0,
		lastReplyAt: null,
		sendState: 'sent',
		...overrides
	};
}

describe('shouldGroupWith', () => {
	const base = message();

	it('groups consecutive messages from one author inside the window', () => {
		const next = message({ createdAt: base.createdAt + GROUPING_WINDOW_MS - 1 });
		expect(shouldGroupWith(base, next)).toBe(true);
	});

	it('does not group once the window has elapsed', () => {
		// Exactly at the boundary is still grouped; one millisecond past is not.
		expect(shouldGroupWith(base, message({ createdAt: base.createdAt + GROUPING_WINDOW_MS }))).toBe(
			true
		);
		expect(
			shouldGroupWith(base, message({ createdAt: base.createdAt + GROUPING_WINDOW_MS + 1 }))
		).toBe(false);
	});

	it('never groups across authors', () => {
		expect(shouldGroupWith(base, message({ authorKey: 18 }))).toBe(false);
	});

	it('never groups a deleted message, in either position', () => {
		expect(shouldGroupWith(base, message({ deletedAt: base.createdAt }))).toBe(false);
		expect(shouldGroupWith(message({ deletedAt: base.createdAt }), message())).toBe(false);
	});

	it('never groups system messages', () => {
		expect(shouldGroupWith(base, message({ messageType: 'system' }))).toBe(false);
	});

	it('has nothing to group against at the top of the list', () => {
		expect(shouldGroupWith(null, base)).toBe(false);
	});

	it('does not group across a day boundary even inside the time window', () => {
		// One minute either side of local midnight — well within the five-minute
		// grouping window, but they belong under different day dividers.
		const justBefore = new Date(2024, 0, 1, 23, 59, 0).getTime();
		const justAfter = new Date(2024, 0, 2, 0, 1, 0).getTime();
		expect(justAfter - justBefore).toBeLessThan(GROUPING_WINDOW_MS);
		expect(shouldGroupWith(message({ createdAt: justBefore }), message({ createdAt: justAfter }))).toBe(
			false
		);
	});
});

describe('dayKey', () => {
	it('keys on the viewer’s local day, not UTC', () => {
		const local = new Date(2024, 5, 15, 12, 0, 0);
		expect(dayKey(local.getTime())).toBe('2024-06-15');
	});

	it('zero-pads month and day', () => {
		expect(dayKey(new Date(2024, 0, 5, 12).getTime())).toBe('2024-01-05');
	});
});

describe('formatDayDivider', () => {
	const now = new Date(2024, 5, 15, 12, 0, 0).getTime();

	it('names today and yesterday', () => {
		expect(formatDayDivider(now, now)).toBe('Today');
		expect(formatDayDivider(new Date(2024, 5, 14, 9).getTime(), now)).toBe('Yesterday');
	});

	it('uses a weekday inside the last week', () => {
		// 2024-06-11 was a Tuesday.
		expect(formatDayDivider(new Date(2024, 5, 11, 9).getTime(), now)).toBe('Tuesday');
	});

	it('falls back to a date for anything older', () => {
		const label = formatDayDivider(new Date(2024, 2, 3, 9).getTime(), now);
		expect(label).toMatch(/March/);
		expect(label).toMatch(/3/);
	});
});

describe('formatRelative', () => {
	const now = 1_700_000_000_000;

	it('reads naturally at each scale', () => {
		expect(formatRelative(now, now)).toBe('just now');
		expect(formatRelative(now - 4 * 60_000, now)).toBe('4m ago');
		expect(formatRelative(now - 3 * 3_600_000, now)).toBe('3h ago');
		expect(formatRelative(now - 2 * 86_400_000, now)).toBe('2d ago');
	});

	it('never reports a negative age when a clock runs ahead', () => {
		expect(formatRelative(now + 60_000, now)).toBe('just now');
	});
});

describe('formatReactionTooltip', () => {
	const me = 14;

	function group(users: { managerKey: number; name: string }[]): ReactionGroup {
		return { emoji: '🔥', emojiType: 'unicode', count: users.length, mine: users.some((u) => u.managerKey === me), users };
	}

	it('addresses you directly when you are the only reactor', () => {
		expect(formatReactionTooltip(group([{ managerKey: me, name: 'Luke S' }]), me)).toBe(
			'You reacted with 🔥'
		);
	});

	it('names a single other reactor', () => {
		expect(formatReactionTooltip(group([{ managerKey: 18, name: 'Trevor' }]), me)).toBe(
			'Trevor reacted with 🔥'
		);
	});

	it('puts "you" last however the server ordered the users', () => {
		const groups = group([
			{ managerKey: me, name: 'Luke S' },
			{ managerKey: 18, name: 'Trevor' },
			{ managerKey: 15, name: 'Nick' }
		]);
		expect(formatReactionTooltip(groups, me)).toBe('Trevor, Nick and you reacted with 🔥');
	});

	it('collapses to a count past three', () => {
		const groups = group([
			{ managerKey: 18, name: 'Trevor' },
			{ managerKey: 15, name: 'Nick' },
			{ managerKey: 16, name: 'Omar' },
			{ managerKey: 5, name: 'Craig' }
		]);
		expect(formatReactionTooltip(groups, me)).toBe('Trevor, Nick and 2 others reacted with 🔥');
	});
});

describe('toPlainText', () => {
	it('strips the markdown a preview should not print literally', () => {
		expect(toPlainText('**Nick** started a kicker in `week 9`')).toBe(
			'Nick started a kicker in week 9'
		);
		expect(toPlainText('see [the league](https://example.com)')).toBe('see the league');
		expect(toPlainText('~~never~~ mind')).toBe('never mind');
		expect(toPlainText('> quoted line')).toBe('quoted line');
	});

	it('collapses a fenced block rather than dumping it inline', () => {
		expect(toPlainText('before\n```\nlots\nof\ncode\n```\nafter')).toBe('before [code] after');
	});
});
