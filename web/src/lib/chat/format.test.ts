import { describe, expect, it } from 'vitest';
import {
	dayKey,
	formatDayDivider,
	connectionPill,
	formatReactionTooltip,
	formatRelative,
	newestRepliers,
	threadChipLabel,
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

	it('puts you first however the server ordered the users (D12)', () => {
		const groups = group([
			{ managerKey: 5, name: 'Craig' },
			{ managerKey: 16, name: 'Omar' },
			{ managerKey: me, name: 'Luke S' }
		]);
		expect(formatReactionTooltip(groups, me)).toBe('You, Craig and Omar reacted with 🔥');
	});

	it('keeps you first when the list collapses to a count', () => {
		const groups = group([
			{ managerKey: 18, name: 'Trevor' },
			{ managerKey: 15, name: 'Nick' },
			{ managerKey: me, name: 'Luke S' },
			{ managerKey: 16, name: 'Omar' }
		]);
		expect(formatReactionTooltip(groups, me)).toBe('You, Trevor and 2 others reacted with 🔥');
	});

	it('names two people with "and"', () => {
		const groups = group([
			{ managerKey: 18, name: 'Trevor' },
			{ managerKey: me, name: 'Luke S' }
		]);
		expect(formatReactionTooltip(groups, me)).toBe('You and Trevor reacted with 🔥');
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

describe('connectionPill', () => {
	it.each([
		['live', 'Live', 'live'],
		['connecting', 'Connecting…', 'connecting'],
		['reconnecting', 'Reconnecting…', 'trouble'],
		['offline', 'Offline', 'trouble']
	] as const)('%s reads %s in the %s tone', (state, label, tone) => {
		expect(connectionPill(state)).toEqual({ label, tone });
	});

	it.each(['idle', 'unauthorized'] as const)('%s shows no pill', (state) => {
		expect(connectionPill(state)).toBeNull();
	});
});

describe('threadChipLabel', () => {
	const now = 1_700_000_000_000;

	it('reads "· hide" while the thread is open', () => {
		expect(threadChipLabel(3, true, now - 60_000, now)).toBe('3 replies · hide');
	});

	it('reads when the last reply came while closed', () => {
		expect(threadChipLabel(3, false, now - 4 * 60_000, now)).toBe('3 replies · last 4m ago');
	});

	it('says "1 reply" for one', () => {
		expect(threadChipLabel(1, false, now - 3 * 3_600_000, now)).toBe('1 reply · last 3h ago');
		expect(threadChipLabel(1, true, null, now)).toBe('1 reply · hide');
	});

	it('drops the "last" part when there is no last reply time', () => {
		expect(threadChipLabel(12, false, null, now)).toBe('12 replies');
	});
});

describe('newestRepliers', () => {
	const reply = (authorKey: number, name: string, createdAt: number) => ({
		authorKey,
		authorName: name,
		authorDisplayName: name,
		createdAt
	});

	it('takes the newest distinct repliers, newest first, capped at two', () => {
		const replies = [
			reply(18, 'Trevor', 1),
			reply(20, 'Troy Colvin', 2),
			reply(18, 'Trevor', 3),
			reply(7, 'Gabe Flores', 4),
			reply(7, 'Gabe Flores', 5)
		];
		expect(newestRepliers(replies)).toEqual([
			{ authorKey: 7, name: 'Gabe Flores' },
			{ authorKey: 18, name: 'Trevor' }
		]);
	});

	it('shows one replier once, however many times they replied', () => {
		expect(newestRepliers([reply(18, 'Trevor', 1), reply(18, 'Trevor', 2)])).toEqual([
			{ authorKey: 18, name: 'Trevor' }
		]);
	});

	it('is empty before any replies have loaded', () => {
		expect(newestRepliers([])).toEqual([]);
	});
});
