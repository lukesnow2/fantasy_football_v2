import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { ruleVote, ruleProposal } from '$lib/server/db/schema';
import { and, desc, eq } from 'drizzle-orm';
import { requireManagerKey } from '$lib/server/auth-manager';
import { settleProposal } from '$lib/server/constitution/evaluate';
import type { RequestHandler } from './$types';

const VALID_VOTES = ['yes', 'no', 'abstain'] as const;

/**
 * Ballots are league-internal. hooks.server.ts gates this route, including GET —
 * who voted which way, and their comments, are not public even though the
 * constitution text is.
 */
export const GET: RequestHandler = async ({ url }) => {
	try {
		const proposalKeyParam = url.searchParams.get('proposalKey');
		const managerKeyParam = url.searchParams.get('managerKey');

		const conditions = [];
		if (proposalKeyParam) {
			const key = Number.parseInt(proposalKeyParam, 10);
			if (Number.isNaN(key)) return json({ error: 'Invalid proposalKey' }, { status: 400 });
			conditions.push(eq(ruleVote.proposalKey, key));
		}
		if (managerKeyParam) {
			const key = Number.parseInt(managerKeyParam, 10);
			if (Number.isNaN(key)) return json({ error: 'Invalid managerKey' }, { status: 400 });
			conditions.push(eq(ruleVote.managerKey, key));
		}

		const votes = await db
			.select()
			.from(ruleVote)
			.where(conditions.length ? and(...conditions) : undefined)
			.orderBy(desc(ruleVote.votedAt));

		return json({ votes });
	} catch (error) {
		console.error('Error fetching rule votes:', error);
		return json({ error: 'Failed to fetch rule votes' }, { status: 500 });
	}
};

export const POST: RequestHandler = async ({ request, locals }) => {
	// Throws 401/403 rather than returning null. The voter is always the session
	// holder; a manager key in the request body is ignored if present.
	const managerKey = requireManagerKey(locals);

	try {
		const { proposalKey, vote, comment } = await request.json();

		if (typeof proposalKey !== 'number' || !vote) {
			return json({ error: 'Missing required fields' }, { status: 400 });
		}
		if (!VALID_VOTES.includes(vote)) {
			return json({ error: 'Invalid vote value' }, { status: 400 });
		}
		if (typeof comment === 'string' && comment.length > 2000) {
			return json({ error: 'Comment is too long' }, { status: 400 });
		}

		const now = new Date();

		const result = await db.transaction(async (tx) => {
			const [proposal] = await tx
				.select({
					status: ruleProposal.status,
					category: ruleProposal.category,
					subjectManagerKey: ruleProposal.subjectManagerKey,
					votingEndDate: ruleProposal.votingEndDate
				})
				.from(ruleProposal)
				.where(eq(ruleProposal.proposalKey, proposalKey))
				.limit(1);

			if (!proposal) return { error: 'Proposal not found', status: 404 };

			// Voting is only open in the `active` state. Without this, a settled
			// proposal could be reopened by a late vote arriving after passage.
			if (proposal.status !== 'active') {
				return { error: `This proposal is ${proposal.status} — voting is closed.`, status: 409 };
			}
			if (proposal.votingEndDate && now >= proposal.votingEndDate) {
				return { error: 'Voting on this proposal has closed.', status: 409 };
			}

			// Article 8 IV: the manager whose removal is being voted on does not vote
			// on it. They are already out of the denominator; this keeps them out of
			// the numerator too.
			if (proposal.subjectManagerKey === managerKey) {
				return { error: 'You cannot vote on a proposal to remove you.', status: 403 };
			}

			// Upsert on the (proposal, manager) unique index. Doing it as one
			// statement rather than select-then-insert closes the race where two
			// concurrent submissions both see "no existing vote".
			const [saved] = await tx
				.insert(ruleVote)
				.values({ proposalKey, managerKey, vote, comment: comment || null, votedAt: now })
				.onConflictDoUpdate({
					target: [ruleVote.proposalKey, ruleVote.managerKey],
					set: { vote, comment: comment || null, votedAt: now }
				})
				.returning();

			// Settle in the same transaction as the vote. If this vote is the one
			// that carries the proposal over the line, the passage, the amendment
			// record and the new constitution version all commit with it or not
			// at all.
			const outcome = await settleProposal(tx, proposalKey, now);

			return { vote: saved, outcome };
		});

		if ('error' in result) {
			return json({ error: result.error }, { status: result.status });
		}

		return json(result);
	} catch (error) {
		console.error('Error creating/updating rule vote:', error);
		return json({ error: 'Failed to process vote' }, { status: 500 });
	}
};
