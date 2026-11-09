import { describe, expect, it } from 'vitest';
import {
	avatarInitial,
	clampPercent,
	formatPercent,
	movement,
	placeCard,
	segmentedTarget
} from './parts';

describe('movement', () => {
	it('reads a gain as up, a loss as down and no change as Same', () => {
		expect(movement(4)).toEqual({ direction: 'up', text: '4' });
		expect(movement(-2)).toEqual({ direction: 'down', text: '2' });
		expect(movement(0)).toEqual({ direction: 'same', text: 'Same' });
	});
});

describe('clampPercent and formatPercent', () => {
	it('holds a bar between 0 and 100', () => {
		expect(clampPercent(85)).toBe(85);
		expect(clampPercent(-3)).toBe(0);
		expect(clampPercent(140)).toBe(100);
		expect(clampPercent(Number.NaN)).toBe(0);
	});

	it('prints a whole percentage', () => {
		expect(formatPercent(85)).toBe('85%');
		expect(formatPercent(84.6)).toBe('85%');
		expect(formatPercent(0.2)).toBe('0%');
		expect(formatPercent(101)).toBe('100%');
	});
});

describe('avatarInitial', () => {
	it('takes the first letter of the name, capitalized', () => {
		expect(avatarInitial('Bobby')).toBe('B');
		expect(avatarInitial('  gabe the younger')).toBe('G');
		expect(avatarInitial('')).toBe('');
		expect(avatarInitial(null)).toBe('');
		expect(avatarInitial('🦅 Craig')).toBe('🦅');
	});
});

describe('segmentedTarget', () => {
	it('moves with the arrows and wraps at the ends', () => {
		expect(segmentedTarget('ArrowRight', 0, 3)).toBe(1);
		expect(segmentedTarget('ArrowDown', 2, 3)).toBe(0);
		expect(segmentedTarget('ArrowLeft', 0, 3)).toBe(2);
		expect(segmentedTarget('ArrowUp', 1, 3)).toBe(0);
	});

	it('jumps with Home and End and ignores other keys', () => {
		expect(segmentedTarget('Home', 2, 3)).toBe(0);
		expect(segmentedTarget('End', 0, 3)).toBe(2);
		expect(segmentedTarget('Enter', 1, 3)).toBeNull();
		expect(segmentedTarget('ArrowRight', 0, 0)).toBeNull();
	});
});

describe('placeCard', () => {
	const card = { width: 340, height: 200 };

	it('puts the card under its trigger when it fits', () => {
		const trigger = { top: 100, bottom: 120, left: 200, right: 260 };
		expect(placeCard(trigger, card, { width: 1280, height: 800 })).toEqual({ top: 128, left: 200 });
	});

	it('flips above when there is no room below', () => {
		const trigger = { top: 700, bottom: 720, left: 200, right: 260 };
		expect(placeCard(trigger, card, { width: 1280, height: 800 })).toEqual({ top: 492, left: 200 });
	});

	it('stays inside a 390 wide screen', () => {
		const trigger = { top: 100, bottom: 120, left: 330, right: 370 };
		const placed = placeCard(trigger, { width: 358, height: 200 }, { width: 390, height: 800 });
		expect(placed.left).toBe(16);
		expect(placed.left + 358).toBeLessThanOrEqual(390 - 16);
	});

	it('never starts left of the margin', () => {
		const trigger = { top: 100, bottom: 120, left: 4, right: 40 };
		expect(placeCard(trigger, card, { width: 1280, height: 800 }).left).toBe(16);
	});
});
