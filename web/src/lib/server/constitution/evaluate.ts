import { and, eq, lte, sql } from 'drizzle-orm';
import { db, type Tx } from '$lib/server/db';
import { ruleProposal, ruleVote } from '$lib/server/db/schema';
import { computeOutcome, type Outcome, type Tally } from './outcome';
import { isProposalCategory, resolveThreshold, type Threshold } from './thresholds';

export type { Outcome, Tally } from './outcome';

/**
 * Recount from the ballots.
 *
 * The denormalised yes/no/abstain columns on rule_proposal exist for cheap
 * display, but they are never the input to a passage decision — a stale counter
 * would silently amend the constitution.
 */
export async function tallyVotes(tx: Tx, proposalKey: number): Promise<Tally> {
	const rows = await tx
		.select({ vote: ruleVote.vote, n: sql<number>`count(*)::int` })
		.from(ruleVote)
		.where(eq(ruleVote.proposalKey, proposalKey))
		.groupBy(ruleVote.vote);

	const tally: Tally = { yes: 0, no: 0, abstain: 0 };
	for (const row of rows) {
		if (row.vote === 'yes' || row.vote === 'no' || row.vote === 'abstain') {
			tally[row.vote] = row.n;
		}
	}
	return tally;
}

/**
 * The threshold as recorded when the proposal opened.
 *
 * Read from the stored `requiredVotes` / `eligibleVoters` rather than recomputed
 * from today's roster: a manager joining or leaving mid-vote must not silently
 * move the bar under a vote already in progress.
 */
function storedThreshold(row: {
	category: string;
	requiredVotes: number;
	eligibleVoters: number;
	subjectManagerKey: number | null;
}): Threshold {
	const category = isProposalCategory(row.category) ? row.category : 'general';
	const base = resolveThreshold(category, row.eligibleVoters, {
		hasSubject: row.subjectManagerKey != null
	});

	return {
		...base,
		requiredYes: row.requiredVotes,
		eligibleVoters: row.eligibleVoters,
		label: `${base.label.split(' (')[0]} (${row.requiredVotes} of ${row.eligibleVoters})`
	};
}

/**
 * Evaluate one proposal and persist a terminal result if it has reached one.
 *
 * Runs inside the caller's transaction so that passage, the amendment record and
 * the new constitution version commit together with whatever triggered them.
 */
export async function settleProposal(
	tx: Tx,
	proposalKey: number,
	now: Date = new Date()
): Promise<Outcome | null> {
	const [row] = await tx
		.select({
			proposalKey: ruleProposal.proposalKey,
			status: ruleProposal.status,
			category: ruleProposal.category,
			requiredVotes: ruleProposal.requiredVotes,
			eligibleVoters: ruleProposal.eligibleVoters,
			subjectManagerKey: ruleProposal.subjectManagerKey,
			votingEndDate: ruleProposal.votingEndDate
		})
		.from(ruleProposal)
		.where(eq(ruleProposal.proposalKey, proposalKey))
		.limit(1);

	if (!row) return null;

	const tally = await tallyVotes(tx, proposalKey);
	const threshold = storedThreshold(row);
	const outcome = computeOutcome({
		status: row.status,
		votingEndDate: row.votingEndDate,
		threshold,
		tally,
		now
	});

	// Always refresh the display counters, whatever the outcome.
	const counters = {
		yesVotes: tally.yes,
		noVotes: tally.no,
		abstainVotes: tally.abstain,
		updatedAt: now
	};

	if (outcome.state === 'open' || row.status === 'passed' || row.status === 'rejected') {
		await tx.update(ruleProposal).set(counters).where(eq(ruleProposal.proposalKey, proposalKey));
		return outcome;
	}

	if (outcome.state === 'rejected') {
		await tx
			.update(ruleProposal)
			.set({ ...counters, status: 'rejected', settledAt: now })
			.where(eq(ruleProposal.proposalKey, proposalKey));
		return outcome;
	}

	// Passed. Import lazily to keep a cycle from forming: apply.ts needs the
	// tally shape from here, and this needs the applier.
	const { applyPassedProposal } = await import('./apply');
	const applied = await applyPassedProposal(tx, proposalKey, tally, now);

	await tx
		.update(ruleProposal)
		.set({
			...counters,
			status: applied.status,
			settledAt: now,
			appliedAt: applied.status === 'passed' ? now : null,
			amendmentKey: applied.amendmentKey
		})
		.where(eq(ruleProposal.proposalKey, proposalKey));

	return outcome;
}

/**
 * Settle every active proposal whose deadline has passed.
 *
 * Called from the constitution page load, which is what makes the state machine
 * correct without a scheduler: a proposal cannot be observed in a stale `active`
 * state, because looking at it is what settles it. At this row count the sweep
 * is trivial.
 */
export async function settleDueProposals(now: Date = new Date()): Promise<number> {
	const due = await db
		.select({ proposalKey: ruleProposal.proposalKey })
		.from(ruleProposal)
		.where(and(eq(ruleProposal.status, 'active'), lte(ruleProposal.votingEndDate, now)));

	if (due.length === 0) return 0;

	let settled = 0;
	for (const { proposalKey } of due) {
		// One transaction per proposal: a failure applying one amendment must not
		// roll back the others settled in the same sweep.
		try {
			await db.transaction(async (tx) => {
				await settleProposal(tx, proposalKey, now);
			});
			settled += 1;
		} catch (error) {
			console.error(`[constitution] Failed to settle proposal ${proposalKey}:`, error);
		}
	}

	return settled;
}

/** Read-only evaluation for display. Never writes. */
export async function describeOutcome(
	tx: Tx,
	row: {
		status: string;
		category: string;
		requiredVotes: number;
		eligibleVoters: number;
		subjectManagerKey: number | null;
		votingEndDate: Date | null;
		proposalKey: number;
	},
	now: Date = new Date()
): Promise<Outcome> {
	const tally = await tallyVotes(tx, row.proposalKey);
	return computeOutcome({
		status: row.status,
		votingEndDate: row.votingEndDate,
		threshold: storedThreshold(row),
		tally,
		now
	});
}
