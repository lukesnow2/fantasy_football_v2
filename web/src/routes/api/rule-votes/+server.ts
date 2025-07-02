import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { ruleVote, ruleProposal } from '$lib/server/db/schema';
import { eq, and, desc } from 'drizzle-orm';

export async function GET({ url }) {
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
}

export async function POST({ request }) {
	try {
		const body = await request.json();
		const { proposalKey, managerKey, vote, comment } = body;
		
		// Validate required fields
		if (!proposalKey || !managerKey || !vote) {
			return json({ error: 'Missing required fields' }, { status: 400 });
		}
		
		// Validate vote value
		if (!['yes', 'no', 'abstain'].includes(vote)) {
			return json({ error: 'Invalid vote value' }, { status: 400 });
		}
		
		// Check if vote already exists
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
			
			return json({ vote: updatedVote });
		} else {
			// Create new vote
			const [newVote] = await db
				.insert(ruleVote)
				.values({
					proposalKey,
					managerKey,
					vote,
					comment
				})
				.returning();
			
			return json({ vote: newVote });
		}
	} catch (error) {
		console.error('Error creating/updating rule vote:', error);
		return json({ error: 'Failed to process vote' }, { status: 500 });
	}
} 