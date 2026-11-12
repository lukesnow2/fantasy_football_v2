import { describe, expect, it } from 'vitest';
import { createLimiter } from './rate-limit';

describe('createLimiter', () => {
	it('allows up to the limit and then refuses', () => {
		const consume = createLimiter(3, 1000, () => 0);
		expect(consume('a')).toBe(true);
		expect(consume('a')).toBe(true);
		expect(consume('a')).toBe(true);
		expect(consume('a')).toBe(false);
	});

	it('refills the budget once the window has passed', () => {
		// The branch that matters. A regression that never advances resetAt locks
		// every manager out of sign-in permanently after their fifth attempt, and
		// a cap-only test still passes — for a league where an emailed link is the
		// only way in, that is a total outage with no failing test.
		let clock = 0;
		const consume = createLimiter(2, 1000, () => clock);

		expect(consume('a')).toBe(true);
		expect(consume('a')).toBe(true);
		expect(consume('a')).toBe(false);

		clock += 1001;
		expect(consume('a')).toBe(true);
	});

	it('does not refill one millisecond early', () => {
		let clock = 0;
		const consume = createLimiter(1, 1000, () => clock);
		expect(consume('a')).toBe(true);

		clock += 999;
		expect(consume('a')).toBe(false);

		clock += 2; // now past resetAt
		expect(consume('a')).toBe(true);
	});

	it('keeps separate budgets per key', () => {
		const consume = createLimiter(1, 1000, () => 0);
		expect(consume('a')).toBe(true);
		expect(consume('a')).toBe(false);
		// One manager exhausting their budget must not lock out another.
		expect(consume('b')).toBe(true);
	});
});
