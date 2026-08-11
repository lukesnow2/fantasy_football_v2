import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { ruleProposal, ruleAmendment, dimManager } from '$lib/server/db/schema';
import { desc, eq } from 'drizzle-orm';
import { nanoid } from 'nanoid';
import { requireManagerKey } from '$lib/server/auth-manager';
import { countEligibleVoters } from '$lib/server/members';
import { isProposalCategory, resolveThreshold } from '$lib/server/constitution/thresholds';
import { settleDueProposals } from '$lib/server/constitution/evaluate';
import type { RequestHandler } from './$types';

/** Article 8 votes run for a week. A month of dead air on a ten-person league helps nobody. */
const VOTING_WINDOW_DAYS = 7;

const PROPOSAL_TYPES = ['edit_clause', 'add_clause', 'delete_clause'] as const;

export const GET: RequestHandler = async ({ url, locals }) => {
	try {
		if (url.searchParams.get('type') === 'amendments') {
			const amendments = await db
				.select()
				.from(ruleAmendment)
				.orderBy(desc(ruleAmendment.amendmentYear), desc(ruleAmendment.amendmentKey));

			// Mapped to a stable shape rather than returned raw: the old UI read
			// `.year`/`.title`/`.description` while the API returned
			// `amendmentYear`/`amendmentTitle`/…, so every field rendered undefined.
			return json(
				amendments.map((a) => ({
					amendmentKey: a.amendmentKey,
					proposalKey: a.proposalKey,
					year: a.amendmentYear,
					title: a.amendmentTitle,
					description: a.amendmentDescription,
					effectiveSeason: a.effectiveSeason,
					voteResults: a.voteResults ? JSON.parse(a.voteResults) : null,
					approvedAt: a.approvedAt
				}))
			);
		}

		// Settle anything past its deadline before reporting status, so a proposal
		// can never be observed sitting in a stale `active` state.
		await settleDueProposals();

		const proposals = await db
			.select({
				proposalKey: ruleProposal.proposalKey,
				proposalId: ruleProposal.proposalId,
				title: ruleProposal.title,
				description: ruleProposal.description,
				proposalType: ruleProposal.proposalType,
				category: ruleProposal.category,
				affectedSection: ruleProposal.affectedSection,
				targetClauseUid: ruleProposal.targetClauseUid,
				subjectManagerKey: ruleProposal.subjectManagerKey,
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
				eligibleVoters: ruleProposal.eligibleVoters,
				yesVotes: ruleProposal.yesVotes,
				noVotes: ruleProposal.noVotes,
				abstainVotes: ruleProposal.abstainVotes,
				settledAt: ruleProposal.settledAt,
				submittedByManager: dimManager.managerName
			})
			.from(ruleProposal)
			.leftJoin(dimManager, eq(ruleProposal.submittedBy, dimManager.managerKey))
			.orderBy(desc(ruleProposal.createdAt));

		// Drafts are private to their author until opened for voting — the same
		// rule the constitution page load applies. This endpoint previously
		// returned every draft to any signed-in member.
		const managerKey = locals.member?.active ? locals.member.managerKey : null;
		const visible = proposals.filter(
			(p) => p.status !== 'draft' || p.submittedBy === managerKey
		);

		return json({ proposals: visible });
	} catch (error) {
		console.error('Error fetching rule proposals:', error);
		return json({ error: 'Failed to fetch rule proposals' }, { status: 500 });
	}
};

export const POST: RequestHandler = async ({ request, locals }) => {
	// The author is always the session holder. Any submittedBy in the body is
	// ignored — clients do not get to assert identity.
	const managerKey = requireManagerKey(locals);

	try {
		const data = await request.json();

		const proposalType = data.proposalType;
		if (!PROPOSAL_TYPES.includes(proposalType)) {
			return json({ error: 'Choose what kind of change this is.' }, { status: 400 });
		}
		if (!isProposalCategory(data.category)) {
			return json({ error: 'Choose which constitutional rule this falls under.' }, { status: 400 });
		}
		if (typeof data.proposedLanguage !== 'string' || !data.proposedLanguage.trim()) {
			if (proposalType !== 'delete_clause') {
				return json({ error: 'Enter the proposed wording.' }, { status: 400 });
			}
		}
		if (typeof data.rationale !== 'string' || !data.rationale.trim()) {
			return json({ error: 'Explain why this change is needed.' }, { status: 400 });
		}
		if (proposalType !== 'add_clause' && !data.targetClauseUid) {
			return json({ error: 'Pick which clause this affects.' }, { status: 400 });
		}

		const effectiveSeason = Number.parseInt(String(data.effectiveSeason), 10);
		if (Number.isNaN(effectiveSeason)) {
			return json({ error: 'Pick the season this takes effect.' }, { status: 400 });
		}

		const subjectManagerKey =
			data.category === 'manager_removal' && data.subjectManagerKey != null
				? Number(data.subjectManagerKey)
				: null;

		if (data.category === 'manager_removal' && subjectManagerKey == null) {
			return json({ error: 'Name the manager this concerns.' }, { status: 400 });
		}

		// The bar is fixed when the proposal opens and stored on the row, so a
		// manager joining or leaving mid-vote cannot move it under a live vote.
		const activeMembers = await countEligibleVoters();
		const threshold = resolveThreshold(data.category, activeMembers, {
			hasSubject: subjectManagerKey != null
		});

		const [created] = await db
			.insert(ruleProposal)
			.values({
				proposalId: `proposal-${nanoid(10)}`,
				title: (data.title || 'Rule Amendment').slice(0, 255),
				description: (data.description || data.rationale).slice(0, 2000),
				proposalType,
				category: data.category,
				affectedSection: data.affectedSection ?? null,
				targetClauseUid: data.targetClauseUid ?? null,
				subjectManagerKey,
				currentLanguage: data.currentLanguage ?? null,
				proposedLanguage: data.proposedLanguage ?? '',
				rationale: data.rationale,
				effectiveSeason,
				submittedBy: managerKey,
				// Created as a draft, not active. The author gets a chance to read it
				// back before the league is emailed about it.
				status: 'draft',
				requiredVotes: threshold.requiredYes,
				eligibleVoters: threshold.eligibleVoters
			})
			.returning();

		return json({ success: true, proposal: created, threshold });
	} catch (error) {
		console.error('Error creating rule proposal:', error);
		return json({ error: 'Failed to create proposal' }, { status: 500 });
	}
};

/** Open a draft for voting. Author only. */
export const PATCH: RequestHandler = async ({ request, locals }) => {
	const managerKey = requireManagerKey(locals);

	try {
		const { proposalKey, action } = await request.json();
		if (typeof proposalKey !== 'number') {
			return json({ error: 'Missing proposalKey' }, { status: 400 });
		}

		const [proposal] = await db
			.select()
			.from(ruleProposal)
			.where(eq(ruleProposal.proposalKey, proposalKey))
			.limit(1);

		if (!proposal) return json({ error: 'Proposal not found' }, { status: 404 });

		const isAuthor = proposal.submittedBy === managerKey;
		const isCommissioner = locals.member?.role === 'commissioner';

		if (action === 'open') {
			if (!isAuthor) return json({ error: 'Only the author can open this.' }, { status: 403 });
			if (proposal.status !== 'draft') {
				return json({ error: 'This proposal is not a draft.' }, { status: 409 });
			}

			const now = new Date();
			const [updated] = await db
				.update(ruleProposal)
				.set({
					status: 'active',
					votingStartDate: now,
					votingEndDate: new Date(now.getTime() + VOTING_WINDOW_DAYS * 24 * 60 * 60 * 1000),
					updatedAt: now
				})
				.where(eq(ruleProposal.proposalKey, proposalKey))
				.returning();

			return json({ success: true, proposal: updated });
		}

		if (action === 'withdraw') {
			if (!isAuthor && !isCommissioner) {
				return json({ error: 'Only the author or commissioner can withdraw this.' }, { status: 403 });
			}
			if (proposal.status !== 'draft' && proposal.status !== 'active') {
				return json({ error: 'This proposal is already settled.' }, { status: 409 });
			}

			const [updated] = await db
				.update(ruleProposal)
				.set({ status: 'withdrawn', settledAt: new Date(), updatedAt: new Date() })
				.where(eq(ruleProposal.proposalKey, proposalKey))
				.returning();

			return json({ success: true, proposal: updated });
		}

		return json({ error: 'Unknown action' }, { status: 400 });
	} catch (error) {
		console.error('Error updating rule proposal:', error);
		return json({ error: 'Failed to update proposal' }, { status: 500 });
	}
};

/** Only drafts can be deleted. Once the league has voted, the record stays. */
export const DELETE: RequestHandler = async ({ request, locals }) => {
	const managerKey = requireManagerKey(locals);

	try {
		const { proposalKey } = await request.json();

		const [proposal] = await db
			.select()
			.from(ruleProposal)
			.where(eq(ruleProposal.proposalKey, proposalKey))
			.limit(1);

		if (!proposal) return json({ error: 'Proposal not found' }, { status: 404 });
		if (proposal.submittedBy !== managerKey) {
			return json({ error: 'You can only delete your own proposals' }, { status: 403 });
		}
		if (proposal.status !== 'draft') {
			return json(
				{ error: 'This proposal has been put to the league — withdraw it instead.' },
				{ status: 409 }
			);
		}

		// Votes cascade via the FK on rule_vote.proposal_key.
		await db.delete(ruleProposal).where(eq(ruleProposal.proposalKey, proposalKey));

		return json({ success: true });
	} catch (error) {
		console.error('Error deleting rule proposal:', error);
		return json({ error: 'Failed to delete proposal' }, { status: 500 });
	}
};
