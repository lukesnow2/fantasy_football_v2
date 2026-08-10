import { eq } from 'drizzle-orm';
import { db } from '$lib/server/db';
import { dimManager, leagueMember, ruleProposal, user as userTable } from '$lib/server/db/schema';
import { emailService, type EmailOptions } from '$lib/server/email';
import { ORIGIN } from '$lib/server/env';
import { parsePreferences, type NotificationPreferences } from '$lib/server/auth-manager';
import type { Outcome } from '$lib/server/constitution/outcome';

/**
 * Notification fan-out for the constitution module.
 *
 * Two rules hold everywhere in this file:
 *
 * 1. Called AFTER the transaction commits, never inside one. A mail provider
 *    timing out must not roll back a recorded vote or a passed amendment.
 * 2. Log, never throw. A failed send is not a reason to turn a successful vote
 *    into a 500 for the manager who cast it.
 */

interface Recipient {
	email: string;
	displayName: string | null;
	preferences: NotificationPreferences;
}

async function recipients(
	wants: (prefs: NotificationPreferences) => boolean,
	exclude: number | null = null
): Promise<Recipient[]> {
	const rows = await db
		.select({
			email: leagueMember.email,
			displayName: leagueMember.displayName,
			managerKey: leagueMember.managerKey,
			preferences: userTable.notificationPreferences
		})
		.from(leagueMember)
		.leftJoin(userTable, eq(leagueMember.managerKey, userTable.managerKey))
		.where(eq(leagueMember.active, true));

	return rows
		.filter((r) => r.managerKey !== exclude)
		.map((r) => ({
			email: r.email,
			displayName: r.displayName,
			// parsePreferences, not `?? defaults`: the column is text holding
			// JSON.stringify output, and the old code never parsed it — a non-empty
			// string is truthy, so every flag read as undefined and every
			// preference check silently failed.
			preferences: parsePreferences(r.preferences)
		}))
		.filter((r) => wants(r.preferences));
}

async function fanOut(list: Recipient[], build: (to: Recipient) => EmailOptions): Promise<void> {
	const results = await Promise.allSettled(
		list.map((to) => emailService.sendEmail(build(to)))
	);

	const failed = results.filter((r) => r.status === 'rejected' || r.value === false).length;
	if (failed > 0) {
		console.error(`[notify] ${failed} of ${list.length} notification emails failed to send.`);
	}
}

function layout(heading: string, body: string, cta: string): string {
	return `
		<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; max-width: 560px; margin: 0 auto; padding: 24px; background-color: #0f172a;">
			<div style="background-color: #1e293b; padding: 32px; border-radius: 12px; border: 1px solid #334155;">
				<h1 style="color: #f8fafc; margin: 0 0 4px 0; font-size: 20px;">🏆 The League</h1>
				<p style="color: #94a3b8; margin: 0 0 24px 0; font-size: 14px;">${heading}</p>
				${body}
				<div style="text-align: center; margin: 28px 0 0 0;">
					<a href="${ORIGIN}/constitution" style="background-color: #f59e0b; color: #0f172a; padding: 12px 28px; text-decoration: none; border-radius: 8px; font-weight: 600; display: inline-block;">${cta}</a>
				</div>
			</div>
		</div>
	`;
}

export async function notifyNewProposal(proposalKey: number): Promise<void> {
	try {
		const [proposal] = await db
			.select({
				title: ruleProposal.title,
				rationale: ruleProposal.rationale,
				requiredVotes: ruleProposal.requiredVotes,
				eligibleVoters: ruleProposal.eligibleVoters,
				effectiveSeason: ruleProposal.effectiveSeason,
				votingEndDate: ruleProposal.votingEndDate,
				submittedBy: ruleProposal.submittedBy,
				authorName: dimManager.managerName
			})
			.from(ruleProposal)
			.leftJoin(dimManager, eq(ruleProposal.submittedBy, dimManager.managerKey))
			.where(eq(ruleProposal.proposalKey, proposalKey))
			.limit(1);

		if (!proposal) return;

		// The author already knows; excluded so they are not emailed about their
		// own proposal.
		const list = await recipients((p) => p.emailOnNewProposal, proposal.submittedBy);
		if (list.length === 0) return;

		const closes = proposal.votingEndDate
			? proposal.votingEndDate.toLocaleDateString('en-US', {
					weekday: 'long',
					month: 'long',
					day: 'numeric'
				})
			: 'soon';

		await fanOut(list, (to) => ({
			to: to.email,
			subject: `New rule proposal: ${proposal.title}`,
			html: layout(
				'A proposal is open for voting',
				`<p style="color: #e2e8f0;">${to.displayName ? `Hi ${to.displayName},` : 'Hi,'}</p>
				 <p style="color: #e2e8f0;"><strong>${proposal.authorName ?? 'A manager'}</strong> has proposed <strong>${proposal.title}</strong>.</p>
				 <p style="color: #cbd5e1; border-left: 3px solid #f59e0b; padding-left: 12px; margin: 20px 0;">${proposal.rationale}</p>
				 <p style="color: #94a3b8; font-size: 14px;">
					It needs ${proposal.requiredVotes} of ${proposal.eligibleVoters} votes to pass, would take effect in ${proposal.effectiveSeason}, and voting closes ${closes}.
					Not voting counts the same as voting no.
				 </p>`,
				'Cast your vote'
			),
			text: `${proposal.authorName ?? 'A manager'} has proposed "${proposal.title}".\n\n${proposal.rationale}\n\nNeeds ${proposal.requiredVotes} of ${proposal.eligibleVoters} votes. Voting closes ${closes}. Not voting counts the same as voting no.\n\n${ORIGIN}/constitution`
		}));
	} catch (error) {
		console.error('[notify] Failed to announce new proposal:', error);
	}
}

export async function notifyProposalSettled(
	proposalKey: number,
	outcome: Outcome
): Promise<void> {
	try {
		if (outcome.state === 'open') return;

		const [proposal] = await db
			.select({
				title: ruleProposal.title,
				effectiveSeason: ruleProposal.effectiveSeason,
				status: ruleProposal.status
			})
			.from(ruleProposal)
			.where(eq(ruleProposal.proposalKey, proposalKey))
			.limit(1);

		if (!proposal) return;

		const list = await recipients((p) => p.emailOnVoteResults);
		if (list.length === 0) return;

		const passed = outcome.state === 'passed';
		const { yes, no, abstain } = outcome.tally;

		const reason = passed
			? `It takes effect in ${proposal.effectiveSeason} and the constitution has been updated.`
			: outcome.reason === 'unreachable'
				? 'It could no longer reach the threshold, so voting closed early.'
				: 'Voting closed before it reached the threshold.';

		await fanOut(list, (to) => ({
			to: to.email,
			subject: `${passed ? 'Passed' : 'Did not pass'}: ${proposal.title}`,
			html: layout(
				passed ? 'A rule change passed' : 'A rule change did not pass',
				`<p style="color: #e2e8f0;">${to.displayName ? `Hi ${to.displayName},` : 'Hi,'}</p>
				 <p style="color: #e2e8f0;"><strong>${proposal.title}</strong> ${passed ? 'passed' : 'did not pass'}.</p>
				 <p style="color: #94a3b8; font-size: 14px;">
					Final vote: ${yes} yes, ${no} no, ${abstain} abstain — needed ${outcome.threshold.requiredYes} of ${outcome.threshold.eligibleVoters}.<br>${reason}
				 </p>`,
				passed ? 'Read the new wording' : 'See the record'
			),
			text: `"${proposal.title}" ${passed ? 'passed' : 'did not pass'}.\n\nFinal vote: ${yes} yes, ${no} no, ${abstain} abstain — needed ${outcome.threshold.requiredYes} of ${outcome.threshold.eligibleVoters}.\n${reason}\n\n${ORIGIN}/constitution`
		}));
	} catch (error) {
		console.error('[notify] Failed to announce settled proposal:', error);
	}
}
