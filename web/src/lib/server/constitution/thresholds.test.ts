import { describe, expect, it } from 'vitest';
import {
	PROPOSAL_CATEGORIES,
	resolveThreshold,
	simpleMajority,
	superMajority,
	type ProposalCategory
} from './thresholds';
import { computeOutcome } from './outcome';
import type { Tally } from './outcome';

describe('majority formulas', () => {
	it('simple majority is more than half, not half', () => {
		// The bug this replaces returned Math.ceil(10/2) = 5, which is a tie.
		expect(simpleMajority(10)).toBe(6);
		expect(simpleMajority(9)).toBe(5);
		expect(simpleMajority(2)).toBe(2);
		expect(simpleMajority(1)).toBe(1);
	});

	it('super majority is at least two thirds', () => {
		// 7 of 10 matches Article 5's literal "seven votes to impeach".
		expect(superMajority(10)).toBe(7);
		expect(superMajority(9)).toBe(6);
		expect(superMajority(3)).toBe(2);
	});

	it('a simple majority is never enough to clear a super-majority', () => {
		for (let n = 1; n <= 20; n++) {
			expect(superMajority(n)).toBeGreaterThanOrEqual(simpleMajority(n));
		}
	});
});

describe('resolveThreshold', () => {
	const CASES: Array<{ category: ProposalCategory; at10: number; at9: number }> = [
		{ category: 'scoring', at10: 6, at9: 5 },
		{ category: 'roster_size', at10: 6, at9: 5 },
		{ category: 'league_size', at10: 10, at9: 9 },
		{ category: 'commissioner_impeachment', at10: 7, at9: 6 },
		{ category: 'collusion_pick_penalty', at10: 6, at9: 5 },
		{ category: 'collusion_expulsion', at10: 7, at9: 6 },
		{ category: 'general', at10: 7, at9: 6 }
	];

	for (const { category, at10, at9 } of CASES) {
		it(`${category} needs ${at10} of 10 and ${at9} of 9`, () => {
			expect(resolveThreshold(category, 10).requiredYes).toBe(at10);
			expect(resolveThreshold(category, 9).requiredYes).toBe(at9);
		});
	}

	it('every category resolves to a usable threshold', () => {
		for (const category of PROPOSAL_CATEGORIES) {
			const t = resolveThreshold(category, 10);
			expect(t.requiredYes).toBeGreaterThan(0);
			expect(t.requiredYes).toBeLessThanOrEqual(t.eligibleVoters);
			expect(t.citation).toMatch(/^Article \d/);
		}
	});

	it('manager removal excludes the subject from both sides', () => {
		// Article 8 IV: "unanimous vote minus the manager in question".
		const t = resolveThreshold('manager_removal', 10, { hasSubject: true });
		expect(t.eligibleVoters).toBe(9);
		expect(t.requiredYes).toBe(9);
	});

	it('manager removal without a named subject stays a full unanimous vote', () => {
		const t = resolveThreshold('manager_removal', 10);
		expect(t.eligibleVoters).toBe(10);
		expect(t.requiredYes).toBe(10);
	});

	it('labels report the real numbers', () => {
		expect(resolveThreshold('scoring', 10).label).toBe('Simple majority (6 of 10)');
		expect(resolveThreshold('general', 10).label).toBe('Super-majority (7 of 10)');
		expect(resolveThreshold('league_size', 10).label).toBe('Unanimous (10 of 10)');
	});
});

describe('computeOutcome', () => {
	const now = new Date('2026-09-01T00:00:00Z');
	const future = new Date('2026-09-08T00:00:00Z');
	const past = new Date('2026-08-25T00:00:00Z');
	const scoring = resolveThreshold('scoring', 10); // 6 of 10
	const general = resolveThreshold('general', 10); // 7 of 10

	const tally = (yes: number, no = 0, abstain = 0): Tally => ({ yes, no, abstain });

	it('stays open while the vote is live and winnable', () => {
		const out = computeOutcome({
			status: 'active',
			votingEndDate: future,
			threshold: scoring,
			tally: tally(3, 1),
			now
		});
		expect(out.state).toBe('open');
		expect(out.state === 'open' && out.canStillPass).toBe(true);
	});

	it('passes the moment the bar is met, without waiting for the deadline', () => {
		const out = computeOutcome({
			status: 'active',
			votingEndDate: future,
			threshold: scoring,
			tally: tally(6),
			now
		});
		expect(out.state).toBe('passed');
	});

	it('does not pass one vote short', () => {
		const out = computeOutcome({
			status: 'active',
			votingEndDate: future,
			threshold: scoring,
			tally: tally(5),
			now
		});
		expect(out.state).toBe('open');
	});

	it('rejects early once yes can no longer reach the bar', () => {
		// general needs 7 of 10, so 4 no votes leave only 6 possible yes.
		const out = computeOutcome({
			status: 'active',
			votingEndDate: future,
			threshold: general,
			tally: tally(0, 4),
			now
		});
		expect(out.state).toBe('rejected');
		expect(out.state === 'rejected' && out.reason).toBe('unreachable');
	});

	it('is still open at three no votes on a super-majority', () => {
		const out = computeOutcome({
			status: 'active',
			votingEndDate: future,
			threshold: general,
			tally: tally(0, 3),
			now
		});
		expect(out.state).toBe('open');
	});

	it('counts abstentions against the threshold, like a no', () => {
		// 4 abstentions leave 6 possible yes against a bar of 7.
		const out = computeOutcome({
			status: 'active',
			votingEndDate: future,
			threshold: general,
			tally: tally(0, 0, 4),
			now
		});
		expect(out.state).toBe('rejected');
		expect(out.state === 'rejected' && out.reason).toBe('unreachable');
	});

	it('rejects on the deadline when the bar was never met', () => {
		const out = computeOutcome({
			status: 'active',
			votingEndDate: past,
			threshold: scoring,
			tally: tally(3),
			now
		});
		expect(out.state).toBe('rejected');
		expect(out.state === 'rejected' && out.reason).toBe('deadline');
	});

	it('passes on a met bar even after the deadline', () => {
		// Reaching the bar before expiry must win over a lazy sweep arriving late.
		const out = computeOutcome({
			status: 'active',
			votingEndDate: past,
			threshold: scoring,
			tally: tally(6),
			now
		});
		expect(out.state).toBe('passed');
	});

	it('unanimous fails on a single no', () => {
		const unanimous = resolveThreshold('league_size', 10);
		const out = computeOutcome({
			status: 'active',
			votingEndDate: future,
			threshold: unanimous,
			tally: tally(9, 1),
			now
		});
		expect(out.state).toBe('rejected');
	});

	it('reports terminal states without recomputing them', () => {
		expect(
			computeOutcome({
				status: 'passed',
				votingEndDate: past,
				threshold: scoring,
				tally: tally(0),
				now
			}).state
		).toBe('passed');
	});

	it('a proposal with no deadline stays open indefinitely while winnable', () => {
		const out = computeOutcome({
			status: 'active',
			votingEndDate: null,
			threshold: scoring,
			tally: tally(1),
			now
		});
		expect(out.state).toBe('open');
	});
});
