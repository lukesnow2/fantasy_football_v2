import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { ruleProposal, ruleVote, ruleAmendment, dimManager } from '$lib/server/db/schema';
import { eq, and, desc, count } from 'drizzle-orm';
import { nanoid } from 'nanoid';
import type { RequestHandler } from './$types';

// Types for rule proposals and amendments
interface RuleProposal {
	id: string;
	type: 'edit' | 'add';
	sectionId: string;
	sectionTitle: string;
	ruleIndex?: number;
	originalText?: string;
	proposedText: string;
	rationale: string;
	submittedBy: string;
	submittedDate: string;
	effectiveSeason: number;
	votes: {
		yes: number;
		no: number;
		abstain: number;
		voters: Record<string, 'yes' | 'no' | 'abstain'>;
	};
	status: 'pending' | 'approved' | 'rejected';
	approvedDate?: string;
}

interface Amendment {
	id: string;
	year: number;
	title: string;
	description: string;
	proposalId?: string;
	voteCount?: {
		yes: number;
		no: number;
		abstain: number;
	};
	type: 'original' | 'amendment';
}

// In-memory storage (in a real app, this would be a database)
let proposals: RuleProposal[] = [];
let amendments: Amendment[] = [
	{
		id: '1',
		year: 2012,
		title: 'Original Constitution',
		description: 'League constitution established with 10-team format',
		type: 'original'
	},
	{
		id: '2',
		year: 2013,
		title: 'Snow Rule Added',
		description: 'Relegation policy implemented for charter member return',
		type: 'original'
	},
	{
		id: '3',
		year: 2023,
		title: 'Draft Cities Updated',
		description: 'Draft city list updated: Denver, Portland, Seattle, Boise, Tahoe',
		type: 'original'
	}
];

// Helper to determine vote threshold based on proposal type
function getVoteThreshold(proposal: RuleProposal): { threshold: number; type: string } {
	const totalVoters = 10; // 10 team league
	
	// Check if it's scoring or roster related
	if (proposal.sectionId === 'appendix1' || 
		proposal.proposedText.toLowerCase().includes('scoring') ||
		proposal.proposedText.toLowerCase().includes('roster')) {
		return { threshold: Math.ceil(totalVoters / 2), type: 'simple majority' };
	}
	
	// Check if it's league size related
	if (proposal.proposedText.toLowerCase().includes('league size') ||
		proposal.proposedText.toLowerCase().includes('10 team') ||
		proposal.proposedText.toLowerCase().includes('team league')) {
		return { threshold: totalVoters, type: 'unanimous' };
	}
	
	// Default: super-majority for constitutional changes
	return { threshold: Math.ceil(totalVoters * 2/3), type: 'super-majority' };
}

// Helper to check if proposal is approved
function checkProposalStatus(proposal: RuleProposal): 'pending' | 'approved' | 'rejected' {
	const totalVotes = proposal.votes.yes + proposal.votes.no + proposal.votes.abstain;
	const { threshold } = getVoteThreshold(proposal);
	
	// Need at least 7 votes to consider it
	if (totalVotes < 7) return 'pending';
	
	if (proposal.votes.yes >= threshold) {
		return 'approved';
	} else if (proposal.votes.no > (10 - threshold)) {
		return 'rejected';
	}
	
	return 'pending';
}

// Convert approved proposal to amendment
function createAmendmentFromProposal(proposal: RuleProposal): Amendment {
	return {
		id: `amendment-${proposal.id}`,
		year: new Date().getFullYear(),
		title: proposal.type === 'add' ? 'New Rule Added' : 'Rule Amendment',
		description: proposal.proposedText.length > 100 
			? proposal.proposedText.substring(0, 100) + '...'
			: proposal.proposedText,
		proposalId: proposal.id,
		voteCount: {
			yes: proposal.votes.yes,
			no: proposal.votes.no,
			abstain: proposal.votes.abstain
		},
		type: 'amendment'
	};
}

export const GET: RequestHandler = async ({ url }) => {
	const type = url.searchParams.get('type');
	
	if (type === 'amendments') {
		// Return all amendments (original + approved proposals)
		const approvedAmendments = proposals
			.filter(p => p.status === 'approved')
			.map(createAmendmentFromProposal);
		
		const allAmendments = [...amendments, ...approvedAmendments]
			.sort((a, b) => b.year - a.year);
		
		return json(allAmendments);
	}
	
	// Return all proposals
	return json(proposals);
};

export const POST: RequestHandler = async ({ request }) => {
	const data = await request.json();
	const action = data.action;
	
	if (action === 'create') {
		// Create new proposal
		const proposal: RuleProposal = {
			id: `proposal-${Date.now()}`,
			type: data.type,
			sectionId: data.sectionId,
			sectionTitle: data.sectionTitle,
			ruleIndex: data.ruleIndex,
			originalText: data.originalText,
			proposedText: data.proposedText,
			rationale: data.rationale,
			submittedBy: data.submittedBy || 'Anonymous',
			submittedDate: new Date().toISOString(),
			effectiveSeason: data.effectiveSeason,
			votes: {
				yes: 0,
				no: 0,
				abstain: 0,
				voters: {}
			},
			status: 'pending'
		};
		
		proposals.push(proposal);
		return json({ success: true, proposal });
	}
	
	if (action === 'vote') {
		// Vote on proposal
		const { proposalId, vote, voterId } = data;
		const proposal = proposals.find(p => p.id === proposalId);
		
		if (!proposal) {
			return json({ success: false, error: 'Proposal not found' }, { status: 404 });
		}
		
		// Validate vote type
		if (!['yes', 'no', 'abstain'].includes(vote)) {
			return json({ success: false, error: 'Invalid vote type' }, { status: 400 });
		}
		
		// Remove previous vote if exists
		const previousVote = proposal.votes.voters[voterId];
		if (previousVote) {
			proposal.votes[previousVote as 'yes' | 'no' | 'abstain']--;
		}
		
		// Add new vote
		proposal.votes[vote as 'yes' | 'no' | 'abstain']++;
		proposal.votes.voters[voterId] = vote as 'yes' | 'no' | 'abstain';
		
		// Check if proposal status changed
		const newStatus = checkProposalStatus(proposal);
		if (newStatus !== proposal.status) {
			proposal.status = newStatus;
			if (newStatus === 'approved') {
				proposal.approvedDate = new Date().toISOString();
			}
		}
		
		return json({ success: true, proposal });
	}
	
	return json({ success: false, error: 'Invalid action' }, { status: 400 });
};

export const DELETE: RequestHandler = async ({ request }) => {
	const { proposalId } = await request.json();
	
	const index = proposals.findIndex(p => p.id === proposalId);
	if (index === -1) {
		return json({ success: false, error: 'Proposal not found' }, { status: 404 });
	}
	
	proposals.splice(index, 1);
	return json({ success: true });
};

 