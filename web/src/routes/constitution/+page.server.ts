import { fail } from '@sveltejs/kit';
import { and, desc, eq, inArray } from 'drizzle-orm';
import { nanoid } from 'nanoid';
import { db } from '$lib/server/db';
import { dimManager, ruleAmendment, ruleProposal, ruleVote } from '$lib/server/db/schema';
import { requireManagerKey } from '$lib/server/auth-manager';
import { countEligibleVoters, listActiveMembers } from '$lib/server/members';
import { settleDueProposals, settleProposal, tallyVotes } from '$lib/server/constitution/evaluate';
import { computeOutcome } from '$lib/server/constitution/outcome';
import {
	CATEGORY_LABELS,
	isProposalCategory,
	resolveThreshold
} from '$lib/server/constitution/thresholds';
import { getCurrentVersion, loadVersionTree } from '$lib/server/constitution/versions';
import { notifyNewProposal, notifyProposalSettled } from '$lib/server/notify';
import type { Actions, PageServerLoad } from './$types';

const VOTING_WINDOW_DAYS = 7;
const PROPOSAL_TYPES = ['edit_clause', 'add_clause', 'delete_clause'] as const;

export const load: PageServerLoad = async ({ locals }) => {
	// Settle anything past its deadline before rendering. This is what makes the
	// lifecycle correct without a scheduler — a proposal cannot be observed
	// sitting in a stale `active` state, because looking at it settles it.
	await settleDueProposals();

	const now = new Date();
	const version = await getCurrentVersion(db, now);
	const sections = version ? await loadVersionTree(db, version.versionKey) : [];

	const managerKey = locals.member?.active ? locals.member.managerKey : null;

	const proposals = await db
		.select({
			proposalKey: ruleProposal.proposalKey,
			title: ruleProposal.title,
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
			votingEndDate: ruleProposal.votingEndDate,
			status: ruleProposal.status,
			requiredVotes: ruleProposal.requiredVotes,
			eligibleVoters: ruleProposal.eligibleVoters,
			yesVotes: ruleProposal.yesVotes,
			noVotes: ruleProposal.noVotes,
			abstainVotes: ruleProposal.abstainVotes,
			settledAt: ruleProposal.settledAt,
			authorName: dimManager.managerName
		})
		.from(ruleProposal)
		.leftJoin(dimManager, eq(ruleProposal.submittedBy, dimManager.managerKey))
		.orderBy(desc(ruleProposal.createdAt));

	// Drafts are private to their author until opened for voting.
	const visible = proposals.filter((p) => p.status !== 'draft' || p.submittedBy === managerKey);

	const keys = visible.map((p) => p.proposalKey);

	// Ballots are only loaded for signed-in members — hooks.server.ts already
	// gates /api/rule-votes for the same reason.
	const ballots = managerKey && keys.length
		? await db
				.select({
					proposalKey: ruleVote.proposalKey,
					managerKey: ruleVote.managerKey,
					vote: ruleVote.vote,
					comment: ruleVote.comment,
					votedAt: ruleVote.votedAt,
					managerName: dimManager.managerName
				})
				.from(ruleVote)
				.leftJoin(dimManager, eq(ruleVote.managerKey, dimManager.managerKey))
				.where(inArray(ruleVote.proposalKey, keys))
				.orderBy(desc(ruleVote.votedAt))
		: [];

	const enriched = visible.map((p) => {
		const tally = {
			yes: p.yesVotes,
			no: p.noVotes,
			abstain: p.abstainVotes
		};
		const category = isProposalCategory(p.category) ? p.category : 'general';
		// Rebuild the label from the STORED numbers. Recomputing it from
		// resolveThreshold subtracts the removal subject a second time — the stored
		// eligibleVoters already excludes them — so a removal card showed
		// "Unanimous (8 of 8)" above a "yes/9" counter.
		const base = resolveThreshold(category, p.eligibleVoters, {
			hasSubject: p.subjectManagerKey != null
		});
		const threshold = {
			...base,
			requiredYes: p.requiredVotes,
			eligibleVoters: p.eligibleVoters,
			label: `${base.label.split(' (')[0]} (${p.requiredVotes} of ${p.eligibleVoters})`
		};

		return {
			...p,
			categoryLabel: CATEGORY_LABELS[category],
			threshold,
			outcome: computeOutcome({
				status: p.status,
				votingEndDate: p.votingEndDate,
				threshold,
				tally,
				now
			}),
			myVote: ballots.find((b) => b.proposalKey === p.proposalKey && b.managerKey === managerKey)
				?.vote ?? null,
			ballots: ballots.filter((b) => b.proposalKey === p.proposalKey),
			canVote:
				managerKey != null && p.status === 'active' && p.subjectManagerKey !== managerKey,
			isAuthor: p.submittedBy === managerKey
		};
	});

	const amendments = await db
		.select({
			amendmentKey: ruleAmendment.amendmentKey,
			year: ruleAmendment.amendmentYear,
			title: ruleAmendment.amendmentTitle,
			description: ruleAmendment.amendmentDescription,
			effectiveSeason: ruleAmendment.effectiveSeason,
			voteResults: ruleAmendment.voteResults,
			approvedAt: ruleAmendment.approvedAt
		})
		.from(ruleAmendment)
		.orderBy(desc(ruleAmendment.amendmentYear), desc(ruleAmendment.amendmentKey));

	return {
		version,
		sections,
		proposals: enriched,
		// Mapped to a clean shape here rather than papered over in the template:
		// the old markup read .year/.title/.description while the API returned
		// amendmentYear/amendmentTitle/…, so every field rendered undefined.
		amendments: amendments.map((a) => ({
			...a,
			voteResults: a.voteResults ? JSON.parse(a.voteResults) : null
		})),
		eligibleVoters: await countEligibleVoters(),
		members: managerKey ? await listActiveMembers() : [],
		categoryLabels: CATEGORY_LABELS,
		canPropose: managerKey != null
	};
};

export const actions: Actions = {
	propose: async ({ request, locals }) => {
		const managerKey = requireManagerKey(locals);
		const form = await request.formData();

		const proposalType = String(form.get('proposalType') ?? '');
		const category = String(form.get('category') ?? '');
		const affectedSection = String(form.get('affectedSection') ?? '');
		const targetClauseUid = String(form.get('targetClauseUid') ?? '') || null;
		const proposedLanguage = String(form.get('proposedLanguage') ?? '').trim();
		const rationale = String(form.get('rationale') ?? '').trim();
		const title = String(form.get('title') ?? '').trim();
		const effectiveSeason = Number.parseInt(String(form.get('effectiveSeason') ?? ''), 10);
		const subjectRaw = form.get('subjectManagerKey');

		if (!PROPOSAL_TYPES.includes(proposalType as (typeof PROPOSAL_TYPES)[number])) {
			return fail(400, { error: 'Choose what kind of change this is.' });
		}
		if (!isProposalCategory(category)) {
			return fail(400, { error: 'Choose which constitutional rule this falls under.' });
		}
		if (!rationale) return fail(400, { error: 'Explain why this change is needed.' });
		if (proposalType !== 'delete_clause' && !proposedLanguage) {
			return fail(400, { error: 'Enter the proposed wording.' });
		}
		if (proposalType !== 'add_clause' && !targetClauseUid) {
			return fail(400, { error: 'Pick which clause this affects.' });
		}
		if (Number.isNaN(effectiveSeason)) {
			return fail(400, { error: 'Pick the season this takes effect.' });
		}

		// Validate the section against the live document. Without this a proposal
		// carrying an empty or unknown affectedSection is *guaranteed* to
		// pass-then-supersede: applyChange cannot resolve the section, returns
		// false, and the league is told a vote it held changed nothing.
		const version = await getCurrentVersion(db);
		const sections = version ? await loadVersionTree(db, version.versionKey) : [];
		const section = sections.find((s) => s.sectionId === affectedSection);
		if (!section) {
			return fail(400, { error: 'That article no longer exists in the current constitution.' });
		}

		// Same for the clause, and it must live in the section being amended —
		// a parent from another section would file the new clause under the wrong
		// article and renumber the wrong sibling group.
		if (targetClauseUid) {
			const inSection = (nodes: typeof section.clauses): boolean =>
				nodes.some((c) => c.clauseUid === targetClauseUid || inSection(c.children));
			if (!inSection(section.clauses)) {
				return fail(400, {
					error: 'That clause is no longer part of this article. Reload and try again.'
				});
			}
		}

		const subjectManagerKey =
			category === 'manager_removal' && subjectRaw ? Number(subjectRaw) : null;
		if (category === 'manager_removal' && subjectManagerKey == null) {
			return fail(400, { error: 'Name the manager this concerns.' });
		}

		// The bar is fixed now and stored on the row, so a manager joining or
		// leaving mid-vote cannot move it under a vote already in progress.
		const threshold = resolveThreshold(category, await countEligibleVoters(), {
			hasSubject: subjectManagerKey != null
		});

		const [created] = await db
			.insert(ruleProposal)
			.values({
				proposalId: `proposal-${nanoid(10)}`,
				title: (title || CATEGORY_LABELS[category]).slice(0, 255),
				description: rationale.slice(0, 2000),
				proposalType,
				category,
				affectedSection: affectedSection || null,
				targetClauseUid,
				subjectManagerKey,
				currentLanguage: String(form.get('currentLanguage') ?? '') || null,
				proposedLanguage,
				rationale,
				effectiveSeason,
				submittedBy: managerKey,
				// Draft, not active. The author reads it back before the league is
				// emailed about it.
				status: 'draft',
				requiredVotes: threshold.requiredYes,
				eligibleVoters: threshold.eligibleVoters
			})
			.returning();

		return { success: true, proposalKey: created.proposalKey, status: 'draft' };
	},

	open: async ({ request, locals }) => {
		const managerKey = requireManagerKey(locals);
		const proposalKey = Number(String((await request.formData()).get('proposalKey')));

		const [proposal] = await db
			.select()
			.from(ruleProposal)
			.where(eq(ruleProposal.proposalKey, proposalKey))
			.limit(1);

		if (!proposal) return fail(404, { error: 'Proposal not found.' });
		if (proposal.submittedBy !== managerKey) {
			return fail(403, { error: 'Only the author can open this for voting.' });
		}
		if (proposal.status !== 'draft') return fail(409, { error: 'This is not a draft.' });

		const now = new Date();
		await db
			.update(ruleProposal)
			.set({
				status: 'active',
				votingStartDate: now,
				votingEndDate: new Date(now.getTime() + VOTING_WINDOW_DAYS * 24 * 60 * 60 * 1000),
				updatedAt: now
			})
			.where(eq(ruleProposal.proposalKey, proposalKey));

		// After commit, never inside a transaction: a mail timeout must not roll
		// back a proposal the league is now expected to vote on.
		await notifyNewProposal(proposalKey);

		return { success: true };
	},

	vote: async ({ request, locals }) => {
		const managerKey = requireManagerKey(locals);
		const form = await request.formData();
		const proposalKey = Number(String(form.get('proposalKey')));
		const vote = String(form.get('vote') ?? '');
		const comment = String(form.get('comment') ?? '').trim() || null;

		if (!['yes', 'no', 'abstain'].includes(vote)) {
			return fail(400, { error: 'Invalid vote.' });
		}

		const now = new Date();

		const result = await db.transaction(async (tx) => {
			const [proposal] = await tx
				.select({
					status: ruleProposal.status,
					subjectManagerKey: ruleProposal.subjectManagerKey,
					votingEndDate: ruleProposal.votingEndDate
				})
				.from(ruleProposal)
				.where(eq(ruleProposal.proposalKey, proposalKey))
				.limit(1);

			if (!proposal) return { ok: false as const, error: 'Proposal not found.', status: 404 };
			if (proposal.status !== 'active') {
				return {
					ok: false as const,
					error: `Voting is closed — this proposal is ${proposal.status}.`,
					status: 409
				};
			}
			if (proposal.votingEndDate && now >= proposal.votingEndDate) {
				return { ok: false as const, error: 'Voting on this proposal has closed.', status: 409 };
			}
			// Article 8 IV — the subject of a removal does not vote on it.
			if (proposal.subjectManagerKey === managerKey) {
				return {
					ok: false as const,
					error: 'You cannot vote on a proposal to remove you.',
					status: 403
				};
			}

			await tx
				.insert(ruleVote)
				.values({ proposalKey, managerKey, vote, comment, votedAt: now })
				.onConflictDoUpdate({
					target: [ruleVote.proposalKey, ruleVote.managerKey],
					set: { vote, comment, votedAt: now }
				});

			// Settled in the same transaction as the vote: if this is the vote that
			// carries it over the line, the passage, the amendment record and the
			// new constitution version all commit with it, or none of them do.
			const outcome = await settleProposal(tx, proposalKey, now);
			return { ok: true as const, outcome };
		});

		if (!result.ok) return fail(result.status, { error: result.error });

		if (result.outcome && result.outcome.state !== 'open') {
			await notifyProposalSettled(proposalKey, result.outcome);
		}

		return { success: true };
	},

	withdraw: async ({ request, locals }) => {
		const managerKey = requireManagerKey(locals);
		const proposalKey = Number(String((await request.formData()).get('proposalKey')));

		const [proposal] = await db
			.select()
			.from(ruleProposal)
			.where(eq(ruleProposal.proposalKey, proposalKey))
			.limit(1);

		if (!proposal) return fail(404, { error: 'Proposal not found.' });

		const isAuthor = proposal.submittedBy === managerKey;
		const isCommissioner = locals.member?.role === 'commissioner';
		if (!isAuthor && !isCommissioner) {
			return fail(403, { error: 'Only the author or the commissioner can withdraw this.' });
		}
		if (proposal.status !== 'draft' && proposal.status !== 'active') {
			return fail(409, { error: 'This proposal is already settled.' });
		}

		// A draft nobody has seen is deleted; anything the league has been asked to
		// vote on leaves a record.
		if (proposal.status === 'draft') {
			await db.delete(ruleProposal).where(eq(ruleProposal.proposalKey, proposalKey));
		} else {
			await db
				.update(ruleProposal)
				.set({ status: 'withdrawn', settledAt: new Date(), updatedAt: new Date() })
				.where(eq(ruleProposal.proposalKey, proposalKey));
		}

		return { success: true };
	}
};
