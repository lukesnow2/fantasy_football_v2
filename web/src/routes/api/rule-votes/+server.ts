import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { ruleVote, ruleProposal } from '$lib/server/db/schema';
import { eq, and, desc } from 'drizzle-orm';
import { getCurrentManagerKey } from '$lib/server/auth-manager';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async ({ url }) => {
	try {
		const proposalKey = url.searchParams.get('proposalKey');
		const managerKey = url.searchParams.get('managerKey');
		
		let whereConditions = [];
		
		if (proposalKey) {
			whereConditions.push(eq(ruleVote.proposalKey, parseInt(proposalKey)));
		}
		
		if (managerKey) {
			whereConditions.push(eq(ruleVote.managerKey, parseInt(managerKey)));
		}
		
		const votes = await db
			.select()
			.from(ruleVote)
			.where(whereConditions.length > 0 ? and(...whereConditions) : undefined)
			.orderBy(desc(ruleVote.votedAt));
		
		return json({ votes });
	} catch (error) {
		console.error('Error fetching rule votes:', error);
		return json({ error: 'Failed to fetch rule votes' }, { status: 500 });
	}
};

export const POST: RequestHandler = async ({ request, locals }) => {
	try {
		// Check if user is authenticated
		if (!locals.user) {
			return json({ error: 'Authentication required to vote on proposals' }, { status: 401 });
		}

		// Get the authenticated manager key
		const managerKey = await getCurrentManagerKey(locals.user);
		if (!managerKey) {
			return json({ error: 'No manager profile linked to your account' }, { status: 400 });
		}

		const body = await request.json();
		const { proposalKey, vote, comment } = body;
		
		// Validate required fields
		if (!proposalKey || !vote) {
			return json({ error: 'Missing required fields' }, { status: 400 });
		}
		
		// Validate vote value
		if (!['yes', 'no', 'abstain'].includes(vote)) {
			return json({ error: 'Invalid vote value' }, { status: 400 });
		}
		
		// Check if vote already exists for this authenticated manager
		const existingVote = await db
			.select()
			.from(ruleVote)
			.where(and(
				eq(ruleVote.proposalKey, proposalKey),
				eq(ruleVote.managerKey, managerKey)
			))
			.limit(1);
		
		if (existingVote.length > 0) {
			// Update existing vote
			const [updatedVote] = await db
				.update(ruleVote)
				.set({ vote, comment, votedAt: new Date() })
				.where(eq(ruleVote.voteKey, existingVote[0].voteKey))
				.returning();
			
			// Update the proposal vote counts
			await updateProposalVoteCounts(proposalKey);
			
			return json({ vote: updatedVote });
		} else {
			// Create new vote
			const [newVote] = await db
				.insert(ruleVote)
				.values({
					proposalKey,
					managerKey, // Use authenticated manager key
					vote,
					comment
				})
				.returning();
			
			// Update the proposal vote counts
			await updateProposalVoteCounts(proposalKey);
			
			return json({ vote: newVote });
		}
	} catch (error) {
		console.error('Error creating/updating rule vote:', error);
		return json({ error: 'Failed to process vote' }, { status: 500 });
	}
};

/**
 * Update the vote counts on a proposal after a vote is cast/changed
 */
async function updateProposalVoteCounts(proposalKey: number) {
	try {
		// Get all votes for this proposal
		const votes = await db
			.select()
			.from(ruleVote)
			.where(eq(ruleVote.proposalKey, proposalKey));
		
		// Count votes by type
		const yesVotes = votes.filter(v => v.vote === 'yes').length;
		const noVotes = votes.filter(v => v.vote === 'no').length;
		const abstainVotes = votes.filter(v => v.vote === 'abstain').length;
		
		// Update the proposal with new counts
		await db
			.update(ruleProposal)
			.set({
				yesVotes,
				noVotes,
				abstainVotes
			})
			.where(eq(ruleProposal.proposalKey, proposalKey));
			
	} catch (error) {
		console.error('Error updating proposal vote counts:', error);
	}
} 