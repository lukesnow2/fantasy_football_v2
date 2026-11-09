import { describe, expect, it } from 'vitest';
import { directLinks, isActivePath, navGroups, nextMenuIndex } from './nav';

describe('isActivePath', () => {
	it('matches the exact path', () => {
		expect(isActivePath('/managers', '/managers')).toBe(true);
		expect(isActivePath('/this-season', '/this-season')).toBe(true);
	});

	it('matches a page nested under the link', () => {
		expect(isActivePath('/managers/Trevor', '/managers')).toBe(true);
		expect(isActivePath('/managers/Gabe%20the%20Younger', '/managers')).toBe(true);
	});

	it("doesn't match a lookalike that only shares a prefix", () => {
		expect(isActivePath('/managersx', '/managers')).toBe(false);
		expect(isActivePath('/chat-archive', '/chat')).toBe(false);
	});

	it('never lets Home match every path', () => {
		expect(isActivePath('/', '/')).toBe(true);
		expect(isActivePath('/managers', '/')).toBe(false);
		expect(isActivePath('/this-season', '/')).toBe(false);
	});

	it('ignores a trailing slash', () => {
		expect(isActivePath('/managers/', '/managers')).toBe(true);
		expect(isActivePath('/managers', '/managers/')).toBe(true);
	});

	it("doesn't match a parent page from a link to a child", () => {
		expect(isActivePath('/managers', '/managers/Trevor')).toBe(false);
	});
});

describe('nextMenuIndex', () => {
	it('moves down one and wraps from the last to the first', () => {
		expect(nextMenuIndex(0, 3, 'ArrowDown')).toBe(1);
		expect(nextMenuIndex(2, 3, 'ArrowDown')).toBe(0);
	});

	it('moves up one and wraps from the first to the last', () => {
		expect(nextMenuIndex(2, 3, 'ArrowUp')).toBe(1);
		expect(nextMenuIndex(0, 3, 'ArrowUp')).toBe(2);
	});

	it('lands on the first item with ↓ when nothing has focus', () => {
		expect(nextMenuIndex(-1, 3, 'ArrowDown')).toBe(0);
	});

	it('lands on the last item with ↑ when nothing has focus', () => {
		expect(nextMenuIndex(-1, 3, 'ArrowUp')).toBe(2);
	});

	it('leaves focus alone for other keys and for an empty menu', () => {
		expect(nextMenuIndex(1, 3, 'Enter')).toBeNull();
		expect(nextMenuIndex(-1, 0, 'ArrowDown')).toBeNull();
	});
});

describe('the nav copy', () => {
	it('keeps the three direct links in order', () => {
		expect(directLinks.map((l) => l.name)).toEqual(['This Season', 'Chat', 'Managers']);
	});

	it('matches the Navigation board word for word', () => {
		expect(navGroups.map((g) => [g.label, g.items.map((i) => [i.name, i.blurb, i.href])])).toEqual([
			[
				'Rankings',
				[
					['Power Rankings', "Who's actually good right now", '/power-rankings'],
					['Hall of Fame', 'The all-time leaderboard', '/hall-of-fame'],
					['League History', 'Twenty-two seasons of analysis', '/historical']
				]
			],
			[
				'Activity',
				[
					['Trade Center', 'Every trade ever made', '/trades'],
					['Draft Central', 'Draft boards and grades', '/draft'],
					['Bet Board', 'Side bets, props, and who owes who', '/bets']
				]
			],
			[
				'League',
				[
					['Constitution', 'Rules, proposals and voting', '/constitution'],
					['Data Dictionary', 'What every metric means', '/data-dictionary']
				]
			]
		]);
	});
});
