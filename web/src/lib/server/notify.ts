import { eq } from 'drizzle-orm';
import { db } from '$lib/server/db';
import { dimManager, leagueMember, ruleProposal, user as userTable } from '$lib/server/db/schema';
import { emailService, type EmailOptions } from '$lib/server/email';
import { requireOrigin } from '$lib/server/env';
import { escapeHtml } from '$lib/server/html';
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
	if (list.length === 0) return;

	// One batched request rather than one request per manager. The previous
	// Promise.allSettled fired nine simultaneous requests at a provider whose
	// published limit is ten per second — no headroom, shared with the login
	// path across every API key — and reported the result as a single aggregate
	// count, so a rate-limited fan-out and a healthy one looked identical.
	//
	// The tradeoff taken knowingly: batching couples the sends, so one 500 loses
	// all nine where independent requests might have landed a few. Given the
	// previous behaviour was silent partial failure, and given the retry and
	// idempotency key behind this call, that is the better trade.
	const { sent, succeeded, failed } = await emailService.sendBatch(list.map(build));

	if (failed > 0) {
		// Naming the addresses is the point. A bare count tells nobody which
		// manager never heard about the vote.
		const missed = list.filter((_, i) => !sent[i]).map((r) => r.email);
		console.error(
			`[notify] ${failed} of ${list.length} notification emails failed (${succeeded} delivered): ${missed.join(', ')}`
		);
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
					<a href="${requireOrigin()}/constitution" style="background-color: #f59e0b; color: #0f172a; padding: 12px 28px; text-decoration: none; border-radius: 8px; font-weight: 600; display: inline-block;">${cta}</a>
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
				`<p style="color: #e2e8f0;">${to.displayName ? `Hi ${escapeHtml(to.displayName)},` : 'Hi,'}</p>
				 <p style="color: #e2e8f0;"><strong>${escapeHtml(proposal.authorName ?? 'A manager')}</strong> has proposed <strong>${escapeHtml(proposal.title)}</strong>.</p>
				 <p style="color: #cbd5e1; border-left: 3px solid #f59e0b; padding-left: 12px; margin: 20px 0;">${escapeHtml(proposal.rationale)}</p>
				 <p style="color: #94a3b8; font-size: 14px;">
					It needs ${proposal.requiredVotes} of ${proposal.eligibleVoters} votes to pass, would take effect in ${proposal.effectiveSeason}, and voting closes ${closes}.
					Not voting counts the same as voting no.
				 </p>`,
				'Cast your vote'
			),
			text: `${proposal.authorName ?? 'A manager'} has proposed "${proposal.title}".\n\n${proposal.rationale}\n\nNeeds ${proposal.requiredVotes} of ${proposal.eligibleVoters} votes. Voting closes ${closes}. Not voting counts the same as voting no.\n\n${requireOrigin()}/constitution`
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
		const superseded = outcome.state === 'superseded';
		const { yes, no, abstain } = outcome.tally;
		const verb = passed ? 'passed' : superseded ? 'passed, but could not be applied' : 'did not pass';

		const reason = passed
			? `It takes effect in ${proposal.effectiveSeason} and the constitution has been updated.`
			: superseded
				? 'The clause it changed no longer exists, so the constitution is unchanged. The commissioner will need to re-propose it against the current text.'
				: outcome.state === 'rejected' && outcome.reason === 'unreachable'
					? 'It could no longer reach the threshold, so voting closed early.'
					: 'Voting closed before it reached the threshold.';

		await fanOut(list, (to) => ({
			to: to.email,
			subject: `${passed ? 'Passed' : superseded ? 'Passed but not applied' : 'Did not pass'}: ${proposal.title}`,
			html: layout(
				passed ? 'A rule change passed' : superseded ? 'A rule change passed but could not be applied' : 'A rule change did not pass',
				`<p style="color: #e2e8f0;">${to.displayName ? `Hi ${escapeHtml(to.displayName)},` : 'Hi,'}</p>
				 <p style="color: #e2e8f0;"><strong>${escapeHtml(proposal.title)}</strong> ${verb}.</p>
				 <p style="color: #94a3b8; font-size: 14px;">
					Final vote: ${yes} yes, ${no} no, ${abstain} abstain — needed ${outcome.threshold.requiredYes} of ${outcome.threshold.eligibleVoters}.<br>${reason}
				 </p>`,
				passed ? 'Read the new wording' : 'See the record'
			),
			text: `"${proposal.title}" ${verb}.\n\nFinal vote: ${yes} yes, ${no} no, ${abstain} abstain — needed ${outcome.threshold.requiredYes} of ${outcome.threshold.eligibleVoters}.\n${reason}\n\n${requireOrigin()}/constitution`
		}));
	} catch (error) {
		console.error('[notify] Failed to announce settled proposal:', error);
	}
}
