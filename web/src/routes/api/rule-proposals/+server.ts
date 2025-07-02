import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { ruleProposal, ruleVote, ruleAmendment, dimManager } from '$lib/server/db/schema';
import { eq, and, desc, count } from 'drizzle-orm';
import { nanoid } from 'nanoid';

export async function GET({ url }) {
	try {
		const status = url.searchParams.get('status');
		const managerKey = url.searchParams.get('managerKey');
		
		let whereConditions = [];
		
		if (status) {
			whereConditions.push(eq(ruleProposal.status, status));
		}
		
		if (managerKey) {
			whereConditions.push(eq(ruleProposal.submittedBy, parseInt(managerKey)));
		}
		
		const proposals = await db
			.select()
			.from(ruleProposal)
			.where(whereConditions.length > 0 ? and(...whereConditions) : undefined)
			.orderBy(desc(ruleProposal.createdAt));
		
		return json({ proposals });
	} catch (error) {
		console.error('Error fetching rule proposals:', error);
		return json({ error: 'Failed to fetch rule proposals' }, { status: 500 });
	}
}

export async function POST({ request }) {
	try {
		const body = await request.json();
		const {
			title,
			description,
			proposalType,
			affectedSection,
			currentLanguage,
			proposedLanguage,
			rationale,
			effectiveSeason,
			submittedBy
		} = body;
		
		// Validate required fields
		if (!title || !description || !proposalType || !proposedLanguage || !rationale || !effectiveSeason || !submittedBy) {
			return json({ error: 'Missing required fields' }, { status: 400 });
		}
		
		// Generate unique proposal ID
		const proposalId = `RP-${nanoid(8)}`;
		
		// Create new proposal (proposalKey is auto-generated)
		const [newProposal] = await db.insert(ruleProposal).values({
			proposalId,
			title,
			description,
			proposalType,
			affectedSection,
			currentLanguage,
			proposedLanguage,
			rationale,
			effectiveSeason,
			submittedBy,
			status: 'draft'
		}).returning();
		
		return json({ proposal: newProposal });
	} catch (error) {
		console.error('Error creating rule proposal:', error);
		return json({ error: 'Failed to create rule proposal' }, { status: 500 });
	}
}

export async function PUT({ request }) {
	try {
		const body = await request.json();
		const { proposalKey, ...updates } = body;
		
		if (!proposalKey) {
			return json({ error: 'Proposal key is required' }, { status: 400 });
		}
		
		const [updatedProposal] = await db
			.update(ruleProposal)
			.set({ ...updates, updatedAt: new Date() })
			.where(eq(ruleProposal.proposalKey, proposalKey))
			.returning();
		
		if (!updatedProposal) {
			return json({ error: 'Proposal not found' }, { status: 404 });
		}
		
		return json({ proposal: updatedProposal });
	} catch (error) {
		console.error('Error updating rule proposal:', error);
		return json({ error: 'Failed to update rule proposal' }, { status: 500 });
	}
} 