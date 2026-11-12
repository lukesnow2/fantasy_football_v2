import { eq, sql } from 'drizzle-orm';
import type { Tx } from '$lib/server/db';
import { constitutionClause, constitutionSection, ruleAmendment, ruleProposal } from '$lib/server/db/schema';
import type { Tally } from './outcome';
import { cloneVersion, getLatestVersion, newClauseUid, resequenceSiblings } from './versions';

export interface ApplyResult {
	/** 'passed' when the text change landed, 'superseded' when its target is gone. */
	status: 'passed' | 'superseded';
	amendmentKey: number;
	versionKey: number | null;
}

/**
 * When a future-dated amendment takes effect.
 *
 * The NFL season opens in early September, so a change "effective 2027" starts
 * on 1 September 2027 rather than the calendar new year.
 */
function seasonStart(season: number): Date {
	return new Date(Date.UTC(season, 8, 1));
}

/**
 * Record the amendment and rewrite the constitution, in the caller's transaction.
 *
 * Everything here commits together with the vote that triggered it: the
 * amendment row, the cloned version, and the edited clause. A partial apply
 * would leave the league with an amendment nobody can point at, or a
 * constitution nobody voted for.
 */
export async function applyPassedProposal(
	tx: Tx,
	proposalKey: number,
	tally: Tally,
	now: Date
): Promise<ApplyResult> {
	const [proposal] = await tx
		.select()
		.from(ruleProposal)
		.where(eq(ruleProposal.proposalKey, proposalKey))
		.limit(1);

	if (!proposal) throw new Error(`Proposal ${proposalKey} not found`);

	const [amendment] = await tx
		.insert(ruleAmendment)
		.values({
			proposalKey,
			amendmentYear: now.getUTCFullYear(),
			amendmentTitle: proposal.title,
			amendmentDescription: proposal.rationale,
			effectiveSeason: proposal.effectiveSeason,
			voteResults: JSON.stringify({
				yes: tally.yes,
				no: tally.no,
				abstain: tally.abstain,
				requiredYes: proposal.requiredVotes,
				eligibleVoters: proposal.eligibleVoters
			}),
			approvedBy: proposal.submittedBy,
			approvedAt: now
		})
		.returning();

	// The latest version in the chain, not the one currently in force — an
	// amendment must build on any amendment already queued ahead of it.
	const current = await getLatestVersion(tx);
	if (!current) {
		// No seeded constitution to amend. The vote still stands and is recorded;
		// there is simply no document to rewrite yet.
		console.warn(`[constitution] Proposal ${proposalKey} passed with no seeded version to amend.`);
		return { status: 'passed', amendmentKey: amendment.amendmentKey, versionKey: null };
	}

	const effectiveAt =
		proposal.effectiveSeason > now.getUTCFullYear() ? seasonStart(proposal.effectiveSeason) : now;

	const { versionKey, clauseKeyByUid } = await cloneVersion(tx, current.versionKey, {
		effectiveAt,
		note: proposal.title,
		amendmentKey: amendment.amendmentKey
	});

	const applied = await applyChange(tx, versionKey, clauseKeyByUid, proposal);

	if (!applied) {
		// The clause this targeted no longer exists — another amendment deleted it
		// between proposal and passage. Recording 'superseded' with the amendment
		// still on file is more honest than throwing away a vote the league held,
		// and far better than crashing the transaction that recorded it.
		console.warn(
			`[constitution] Proposal ${proposalKey} targeted a clause that no longer exists; marking superseded.`
		);
		return { status: 'superseded', amendmentKey: amendment.amendmentKey, versionKey };
	}

	return { status: 'passed', amendmentKey: amendment.amendmentKey, versionKey };
}

async function applyChange(
	tx: Tx,
	versionKey: number,
	clauseKeyByUid: Map<string, number>,
	proposal: typeof ruleProposal.$inferSelect
): Promise<boolean> {
	const [section] = await tx
		.select()
		.from(constitutionSection)
		.where(
			sql`${constitutionSection.versionKey} = ${versionKey} and ${constitutionSection.sectionId} = ${proposal.affectedSection}`
		)
		.limit(1);

	if (!section) return false;

	switch (proposal.proposalType) {
		case 'edit_clause': {
			const clauseKey = proposal.targetClauseUid
				? clauseKeyByUid.get(proposal.targetClauseUid)
				: undefined;
			if (!clauseKey) return false;

			await tx
				.update(constitutionClause)
				.set({ body: proposal.proposedLanguage })
				.where(eq(constitutionClause.clauseKey, clauseKey));
			return true;
		}

		case 'add_clause': {
			// A new clause is appended to its parent group. targetClauseUid, when
			// present, names the parent it nests under.
			const parentKey = proposal.targetClauseUid
				? (clauseKeyByUid.get(proposal.targetClauseUid) ?? null)
				: null;

			if (proposal.targetClauseUid && parentKey == null) return false;

			const depth = parentKey == null ? 0 : await clauseDepth(tx, parentKey) + 1;

			const [{ nextOrder }] = await tx
				.select({ nextOrder: sql<number>`coalesce(max(${constitutionClause.sortOrder}), 0)::int + 1` })
				.from(constitutionClause)
				.where(
					parentKey == null
						? sql`${constitutionClause.sectionKey} = ${section.sectionKey} and ${constitutionClause.parentKey} is null`
						: sql`${constitutionClause.sectionKey} = ${section.sectionKey} and ${constitutionClause.parentKey} = ${parentKey}`
				);

			await tx.insert(constitutionClause).values({
				sectionKey: section.sectionKey,
				parentKey,
				clauseUid: newClauseUid(),
				depth,
				sortOrder: nextOrder,
				label: '', // resequence assigns the real label below
				body: proposal.proposedLanguage
			});

			await resequenceSiblings(tx, section.sectionKey, parentKey, depth);
			return true;
		}

		case 'delete_clause': {
			const clauseKey = proposal.targetClauseUid
				? clauseKeyByUid.get(proposal.targetClauseUid)
				: undefined;
			if (!clauseKey) return false;

			const [target] = await tx
				.select({ parentKey: constitutionClause.parentKey, depth: constitutionClause.depth })
				.from(constitutionClause)
				.where(eq(constitutionClause.clauseKey, clauseKey))
				.limit(1);

			// Children cascade via the self-referencing FK.
			await tx.delete(constitutionClause).where(eq(constitutionClause.clauseKey, clauseKey));

			if (target) {
				await resequenceSiblings(tx, section.sectionKey, target.parentKey, target.depth);
			}
			return true;
		}

		default:
			console.warn(`[constitution] Unknown proposal type "${proposal.proposalType}"`);
			return false;
	}
}

async function clauseDepth(tx: Tx, clauseKey: number): Promise<number> {
	const [row] = await tx
		.select({ depth: constitutionClause.depth })
		.from(constitutionClause)
		.where(eq(constitutionClause.clauseKey, clauseKey))
		.limit(1);
	return row?.depth ?? 0;
}
