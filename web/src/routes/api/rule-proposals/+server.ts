import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { ruleProposal, ruleVote, ruleAmendment, dimManager } from '$lib/server/db/schema';
import { eq, and, desc, count } from 'drizzle-orm';
import { nanoid } from 'nanoid';
import { getCurrentManagerKey } from '$lib/server/auth-manager';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async ({ url }) => {
	try {
		const type = url.searchParams.get('type');
		
		if (type === 'amendments') {
			// Return all amendments
			const amendments = await db
				.select()
				.from(ruleAmendment)
				.orderBy(desc(ruleAmendment.amendmentYear));
			
			return json(amendments);
		}
		
		// Return all proposals with manager names
		const proposals = await db
			.select({
				proposalKey: ruleProposal.proposalKey,
				proposalId: ruleProposal.proposalId,
				title: ruleProposal.title,
				description: ruleProposal.description,
				proposalType: ruleProposal.proposalType,
				affectedSection: ruleProposal.affectedSection,
				ruleIndex: ruleProposal.ruleIndex,
				currentLanguage: ruleProposal.currentLanguage,
				proposedLanguage: ruleProposal.proposedLanguage,
				rationale: ruleProposal.rationale,
				effectiveSeason: ruleProposal.effectiveSeason,
				submittedBy: ruleProposal.submittedBy,
				submittedAt: ruleProposal.submittedAt,
				votingStartDate: ruleProposal.votingStartDate,
				votingEndDate: ruleProposal.votingEndDate,
				status: ruleProposal.status,
				requiredVotes: ruleProposal.requiredVotes,
				yesVotes: ruleProposal.yesVotes,
				noVotes: ruleProposal.noVotes,
				abstainVotes: ruleProposal.abstainVotes,
				createdAt: ruleProposal.createdAt,
				updatedAt: ruleProposal.updatedAt,
				submittedByManager: dimManager.managerName
			})
			.from(ruleProposal)
			.leftJoin(dimManager, eq(ruleProposal.submittedBy, dimManager.managerKey))
			.orderBy(desc(ruleProposal.createdAt));
		
		return json({ proposals });
	} catch (error) {
		console.error('Error fetching rule proposals:', error);
		return json({ error: 'Failed to fetch rule proposals' }, { status: 500 });
	}
};

export const POST: RequestHandler = async ({ request, locals }) => {
	try {
		// Check if user is authenticated
		if (!locals.user) {
			return json({ error: 'Authentication required to create proposals' }, { status: 401 });
		}

		// Get the authenticated manager key
		const managerKey = await getCurrentManagerKey(locals.user as any);
		if (!managerKey) {
			return json({ error: 'No manager profile linked to your account' }, { status: 400 });
		}

		const data = await request.json();
		
		// Debug: Log the received data
		
		// Handle both old format (from constitution page) and new format (from rule-proposals page)
		let proposalData;
		if (data.action === 'create') {
			// Old format from constitution page
			proposalData = {
				title: data.sectionTitle || 'Rule Amendment',
				description: `${data.type === 'edit' ? 'Edit' : 'Add'} rule in ${data.sectionTitle}`,
				proposalType: data.type === 'edit' ? 'edit_language' : 'add_clause',
				affectedSection: data.sectionId,
				ruleIndex: data.ruleIndex,
				currentLanguage: data.originalText,
				proposedLanguage: data.proposedText,
				rationale: data.rationale,
				effectiveSeason: data.effectiveSeason
			};
		} else {
			// New format from rule-proposals page
			proposalData = data;
		}
		
		// Validate required fields
		if (!proposalData.proposalType || !proposalData.proposedLanguage || 
			proposalData.proposedLanguage.trim() === '') {
			return json({ error: 'Missing required fields. Please fill in proposal type and proposed language.' }, { status: 400 });
		}
		
		// Generate a unique proposal ID
		const proposalId = `proposal-${Date.now()}-${nanoid(6)}`;
		
		// Create new proposal (proposalKey will be auto-generated)
		const [newProposal] = await db
			.insert(ruleProposal)
			.values({
				proposalId,
				title: proposalData.title || 'Rule Amendment',
				description: proposalData.description || proposalData.rationale,
				proposalType: proposalData.proposalType,
				affectedSection: proposalData.affectedSection,
				ruleIndex: proposalData.ruleIndex,
				currentLanguage: proposalData.currentLanguage,
				proposedLanguage: proposalData.proposedLanguage,
				rationale: proposalData.rationale,
				effectiveSeason: proposalData.effectiveSeason,
				submittedBy: managerKey,
				status: 'active', // Make new proposals active by default
				requiredVotes: getRequiredVotes(proposalData.proposalType, proposalData.proposedLanguage),
				votingStartDate: new Date(),
				votingEndDate: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000) // 30 days from now
			})
			.returning();
		
		return json({ success: true, proposal: newProposal });
	} catch (error) {
		console.error('Error creating rule proposal:', error);
		return json({ error: 'Failed to create proposal' }, { status: 500 });
	}
};

export const DELETE: RequestHandler = async ({ request, locals }) => {
	try {
		// Check if user is authenticated
		if (!locals.user) {
			return json({ error: 'Authentication required to delete proposals' }, { status: 401 });
		}

		// Get the authenticated manager key
		const managerKey = await getCurrentManagerKey(locals.user as any);
		if (!managerKey) {
			return json({ error: 'No manager profile linked to your account' }, { status: 400 });
		}

		const { proposalKey } = await request.json();
		
		// Check if the proposal exists and if the user has permission to delete it
		const proposal = await db
			.select()
			.from(ruleProposal)
			.where(eq(ruleProposal.proposalKey, proposalKey))
			.limit(1);
		
		if (proposal.length === 0) {
			return json({ error: 'Proposal not found' }, { status: 404 });
		}
		
		// Only allow the author to delete their own proposal
		if (proposal[0].submittedBy !== managerKey) {
			return json({ error: 'You can only delete your own proposals' }, { status: 403 });
		}
		
		// Delete associated votes first
		await db
			.delete(ruleVote)
			.where(eq(ruleVote.proposalKey, proposalKey));
		
		// Delete the proposal
		await db
			.delete(ruleProposal)
			.where(eq(ruleProposal.proposalKey, proposalKey));
		
		return json({ success: true });
	} catch (error) {
		console.error('Error deleting rule proposal:', error);
		return json({ error: 'Failed to delete proposal' }, { status: 500 });
	}
};

/**
 * Helper function to determine required votes based on proposal type and content
 */
function getRequiredVotes(proposalType: string, proposedLanguage: string): number {
	const totalVoters = 10; // 10 team league
	
	// Safety check for undefined/null proposedLanguage
	const languageText = proposedLanguage || '';
	
	// Check if it's scoring or roster related
	if (proposalType === 'tweak_rule' || 
		languageText.toLowerCase().includes('scoring') ||
		languageText.toLowerCase().includes('roster')) {
		return Math.ceil(totalVoters / 2); // Simple majority (6 votes)
	}
	
	// Check if it's league size related
	if (languageText.toLowerCase().includes('league size') ||
		languageText.toLowerCase().includes('10 team') ||
		languageText.toLowerCase().includes('team league')) {
		return totalVoters; // Unanimous (10 votes)
	}
	
	// Default: super-majority for constitutional changes
	return Math.ceil(totalVoters * 2/3); // Super-majority (7 votes)
}

 