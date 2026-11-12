import { eq } from 'drizzle-orm';
import { db } from '$lib/server/db';
import { dimManager, leagueMember, ruleProposal, user as userTable } from '$lib/server/db/schema';
import { emailService, type EmailOptions } from '$lib/server/email';
import { requireOrigin } from '$lib/server/env';
import { escapeHtml } from '$lib/server/html';
import {
	EMAIL_COLORS,
	EMAIL_FONTS,
	emailButton,
	emailFacts,
	emailFootnoteLink,
	emailGreeting,
	emailGreetingHtml,
	emailLayout,
	emailParagraph,
	type KickerTone
} from '$lib/server/email-layout';
import { getCurrentVersion, loadVersionTree } from '$lib/server/constitution/versions';
import { clauseRef } from '$lib/constitution/display';
import { parsePreferences, type NotificationPreferences } from '$lib/server/auth-manager';
import type { Outcome } from '$lib/server/constitution/outcome';
import { LEAGUE_TIME_ZONE } from '$lib/utils/time-zone';

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

// ---------------------------------------------------------------------------
// Templates
// ---------------------------------------------------------------------------

const C = EMAIL_COLORS;
const F = EMAIL_FONTS;

type Settled = Exclude<Outcome, { state: 'open' }>;

/** One email's addressee, as the builders need it. */
export interface EmailRecipient {
	email: string;
	displayName: string | null;
}

export interface NewProposalEmailData {
	title: string;
	rationale: string;
	requiredVotes: number;
	eligibleVoters: number;
	effectiveSeason: number | string;
	votingEndDate: Date | null;
	authorName: string | null;
	proposalType: string;
	currentLanguage: string | null;
	proposedLanguage: string;
	/** `clauseRef` against the current constitution; empty when it can't say. */
	clause: string;
}

export interface VoteResultEmailData {
	title: string;
	effectiveSeason: number | string;
	clause: string;
}

/**
 * "Sun, Nov 22" in the league's zone: an email can't correct itself in the
 * reader's browser, and the server runs in UTC, which would push a
 * late-evening deadline onto the next day.
 */
export function closesLabel(votingEndDate: Date | null): string {
	return votingEndDate
		? votingEndDate.toLocaleDateString('en-US', {
				weekday: 'short',
				month: 'short',
				day: 'numeric',
				timeZone: LEAGUE_TIME_ZONE
			})
		: 'Soon';
}

/** The parts of the wording box, by proposal type: a label and the text per half. */
export function wordingHalves(p: {
	proposalType: string;
	currentLanguage: string | null;
	proposedLanguage: string;
}): Array<{ side: 'current' | 'proposed'; label: string; text: string }> {
	const current = p.currentLanguage?.trim() ?? '';
	const proposed = p.proposedLanguage?.trim() ?? '';
	if (p.proposalType === 'add_clause') {
		return proposed ? [{ side: 'proposed', label: 'New clause', text: proposed }] : [];
	}
	if (p.proposalType === 'delete_clause') {
		return current ? [{ side: 'current', label: 'Clause to remove', text: current }] : [];
	}
	const halves: Array<{ side: 'current' | 'proposed'; label: string; text: string }> = [];
	if (current) halves.push({ side: 'current', label: 'Current wording', text: current });
	if (proposed) halves.push({ side: 'proposed', label: 'Proposed wording', text: proposed });
	return halves;
}

function label(text: string, color: string): string {
	return `<div style="font-family: ${F.sans}; font-size: 12px; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase; color: ${color};">${escapeHtml(text)}</div>`;
}

function wordingBox(halves: ReturnType<typeof wordingHalves>): string {
	if (halves.length === 0) return '';
	const rows = halves
		.map((h, i) => {
			const first = i === 0;
			const last = i === halves.length - 1;
			// The table's own radius doesn't clip a filled cell, so the cell rounds
			// its own corners, 1px inside the border.
			const top = first ? 9 : 0;
			const bottom = last ? 9 : 0;
			const corners = `border-radius: ${top}px ${top}px ${bottom}px ${bottom}px;`;
			const rule = last ? '' : ` border-bottom: 1px solid ${C.lineSoft};`;
			const isProposed = h.side === 'proposed';
			const fill = isProposed ? ` bgcolor="${C.fieldWash}"` : '';
			const background = isProposed ? ` background: ${C.fieldWash};` : '';
			return `<tr><td${fill} style="padding: 12px 16px;${background}${rule} ${corners}">${label(h.label, isProposed ? C.field : C.muted)}<div style="margin-top: 4px; font-family: ${F.sans}; font-size: 15px; line-height: 1.5; color: ${isProposed ? C.ink : C.ink2}; white-space: pre-line;">${escapeHtml(h.text)}</div></td></tr>`;
		})
		.join('');
	return `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse: separate; border: 1px solid ${C.line}; border-radius: 10px;">${rows}</table>`;
}

function whyBlock(rationale: string, marginTop: number): string {
	return `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr><td style="padding-top: ${marginTop}px;"><table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr><td style="padding-left: 14px; border-left: 3px solid ${C.fieldLine};">${label('Why', C.muted)}<div style="margin-top: 4px; font-family: ${F.sans}; font-size: 15px; line-height: 1.55; color: ${C.ink2}; white-space: pre-line;">${escapeHtml(rationale)}</div></td></tr></table></td></tr></table>`;
}

function spaced(html: string, top: number): string {
	return `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr><td style="padding-top: ${top}px;">${html}</td></tr></table>`;
}

const settingsLink = (origin: string) => emailFootnoteLink(`${origin}/settings`, 'email settings');

export function newProposalEmail(
	p: NewProposalEmailData,
	to: EmailRecipient,
	origin: string
): EmailOptions {
	const greeting = emailGreeting(to.displayName);
	const author = p.authorName ?? 'A manager';
	const closes = closesLabel(p.votingEndDate);
	// "Soon" is a value in the facts cell; in a sentence it reads lowercase.
	const closesInSentence = p.votingEndDate ? closes : 'soon';
	const meta = [`Proposed by ${author}`, `takes effect in ${p.effectiveSeason}`, p.clause]
		.filter(Boolean)
		.join(' · ');
	const halves = wordingHalves(p);
	const rationale = p.rationale?.trim() ?? '';
	const votes = `${p.requiredVotes} of ${p.eligibleVoters}`;

	const html = emailLayout({
		preheader: `${author} proposed a rule change. Voting closes ${closesInSentence}.`,
		kicker: 'Constitution · Voting open',
		kickerTone: 'pennant',
		title: p.title,
		titleSize: 28,
		meta,
		bodyHtml:
			emailGreetingHtml(greeting.html) +
			emailParagraph(escapeHtml("A rule change is open for voting. Here's what it changes."), 18) +
			wordingBox(halves) +
			(rationale ? whyBlock(rationale, halves.length ? 18 : 0) : '') +
			spaced(
				emailFacts([
					{ value: votes, label: 'Yes votes to pass' },
					{ value: closes, label: 'Voting closes' }
				]),
				20
			) +
			emailButton(`${origin}/constitution`, 'Cast your vote'),
		footnoteHtml: `Not voting counts the same as voting no. You're getting this because new proposals are on in your ${settingsLink(origin)}.`,
		origin
	});

	const text = [
		p.title,
		meta,
		...halves.map((h) => `${h.label}:\n${h.text}`),
		rationale ? `Why:\n${rationale}` : '',
		`${votes} yes votes to pass. Voting closes ${closesInSentence}.`,
		'Not voting counts the same as voting no.',
		`${origin}/constitution`
	]
		.filter(Boolean)
		.join('\n\n');

	return {
		to: to.email,
		subject: `New rule proposal: ${p.title}`,
		html,
		text: `${text}\n`
	};
}

/** The kicker, its tone, the subject's prefix, the sentence and the button, by outcome. */
export function voteResultCopy(
	outcome: Settled,
	season: number | string
): {
	kicker: string;
	tone: KickerTone;
	subjectPrefix: string;
	sentence: string;
	button: string;
} {
	const required = outcome.threshold.requiredYes;
	switch (outcome.state) {
		case 'passed':
			return {
				kicker: 'Constitution · Passed',
				tone: 'field',
				subjectPrefix: 'Passed',
				sentence: `It passed. The constitution now has the new wording, and it takes effect in ${season}.`,
				button: 'Read the new wording'
			};
		case 'superseded':
			return {
				kicker: 'Constitution · Passed, not applied',
				tone: 'pennant',
				subjectPrefix: 'Passed but not applied',
				sentence:
					"It passed, but the clause it changed no longer exists, so the constitution didn't change. The commissioner will need to propose it again against the current wording.",
				button: 'See the record'
			};
		case 'rejected':
			return {
				kicker: "Constitution · Didn't pass",
				tone: 'pennant',
				subjectPrefix: "Didn't pass",
				sentence:
					outcome.reason === 'unreachable'
						? `It didn't pass. It could no longer reach ${required} yes votes, so voting closed early.`
						: `It didn't pass. Voting closed before it reached ${required} yes votes.`,
				button: 'See the record'
			};
	}
}

export function voteResultEmail(
	p: VoteResultEmailData,
	outcome: Settled,
	to: EmailRecipient,
	origin: string
): EmailOptions {
	const greeting = emailGreeting(to.displayName);
	const copy = voteResultCopy(outcome, p.effectiveSeason);
	const { yes, no, abstain } = outcome.tally;
	const { requiredYes, eligibleVoters } = outcome.threshold;
	const meta =
		outcome.state === 'passed'
			? [`Takes effect in ${p.effectiveSeason}`, p.clause].filter(Boolean).join(' · ')
			: p.clause;

	const html = emailLayout({
		preheader: copy.sentence,
		kicker: copy.kicker,
		kickerTone: copy.tone,
		title: p.title,
		titleSize: 28,
		meta,
		bodyHtml:
			emailGreetingHtml(greeting.html) +
			emailParagraph(escapeHtml(copy.sentence), 18) +
			emailFacts([
				{ value: String(yes), label: 'Yes', color: C.field },
				{ value: String(no), label: 'No', color: C.pennantDark },
				{ value: String(abstain), label: 'Abstain' },
				{ value: `${requiredYes} of ${eligibleVoters}`, label: 'Needed to pass', weight: 1.3 }
			]) +
			emailButton(`${origin}/constitution`, copy.button),
		footnoteHtml: `You're getting this because vote results are on in your ${settingsLink(origin)}.`,
		origin
	});

	const text = `${p.title}

${copy.sentence}

Final vote: ${yes} yes, ${no} no, ${abstain} abstain. Needed ${requiredYes} of ${eligibleVoters}.

${origin}/constitution
`;

	return { to: to.email, subject: `${copy.subjectPrefix}: ${p.title}`, html, text };
}

/** The current constitution's sections, the way the Constitution page loads them. */
async function currentSections() {
	const version = await getCurrentVersion(db);
	return version ? await loadVersionTree(db, version.versionKey) : [];
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
				authorName: dimManager.managerName,
				proposalType: ruleProposal.proposalType,
				affectedSection: ruleProposal.affectedSection,
				targetClauseUid: ruleProposal.targetClauseUid,
				currentLanguage: ruleProposal.currentLanguage,
				proposedLanguage: ruleProposal.proposedLanguage
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

		const origin = requireOrigin();
		const clause = clauseRef(proposal, await currentSections());
		const data: NewProposalEmailData = { ...proposal, clause };

		await fanOut(list, (to) => newProposalEmail(data, to, origin));
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
				status: ruleProposal.status,
				proposalType: ruleProposal.proposalType,
				affectedSection: ruleProposal.affectedSection,
				targetClauseUid: ruleProposal.targetClauseUid
			})
			.from(ruleProposal)
			.where(eq(ruleProposal.proposalKey, proposalKey))
			.limit(1);

		if (!proposal) return;

		const list = await recipients((p) => p.emailOnVoteResults);
		if (list.length === 0) return;

		const origin = requireOrigin();
		const clause = clauseRef(proposal, await currentSections());
		const data: VoteResultEmailData = {
			title: proposal.title,
			effectiveSeason: proposal.effectiveSeason,
			clause
		};

		await fanOut(list, (to) => voteResultEmail(data, outcome, to, origin));
	} catch (error) {
		console.error('[notify] Failed to announce settled proposal:', error);
	}
}
