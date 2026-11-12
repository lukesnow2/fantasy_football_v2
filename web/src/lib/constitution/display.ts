/**
 * How the Constitution page shows the stored document and its proposals
 * (WP11). Pure: no database, no clock unless one is passed in. The stored
 * titles and labels belong to the versioned document, so these read them and
 * never rewrite them.
 */

const ROMAN: Array<[number, string]> = [
	[1000, 'M'],
	[900, 'CM'],
	[500, 'D'],
	[400, 'CD'],
	[100, 'C'],
	[90, 'XC'],
	[50, 'L'],
	[40, 'XL'],
	[10, 'X'],
	[9, 'IX'],
	[5, 'V'],
	[4, 'IV'],
	[1, 'I']
];

/** 1 is "I", 8 is "VIII". The server has its own copy, which the page can't import. */
export function toRoman(n: number): string {
	let rest = Math.floor(n);
	let out = '';
	for (const [value, numeral] of ROMAN) {
		while (rest >= value) {
			out += numeral;
			rest -= value;
		}
	}
	return out;
}

/**
 * "League Makeup and Construction" reads "League makeup and construction".
 * A word in capitals (NFL, FAAB) keeps them.
 */
export function sentenceCase(text: string): string {
	return text
		.trim()
		.split(/\s+/)
		.map((word, i) => {
			if (word.length > 1 && word === word.toUpperCase() && /[A-Z]/.test(word)) return word;
			const lower = word.toLowerCase();
			return i === 0 ? lower.charAt(0).toUpperCase() + lower.slice(1) : lower;
		})
		.join(' ');
}

export interface ArticleParts {
	/** "I" for Article 1, "A1" for Appendix 1, empty for a title that's neither. */
	numeral: string;
	name: string;
	/** How a citation names it: "Article II", "Appendix 1". */
	cite: string;
}

/**
 * "Article 1: League Makeup and Construction" is "I" and "League makeup and
 * construction"; "Appendix 1: Scoring System" is "A1" and "Scoring system". A
 * title in neither form keeps its stored words and gets no numeral.
 */
export function articleParts(section: { title: string }): ArticleParts {
	const match = section.title.match(/^\s*(article|appendix)\s+(\d+)\s*:\s*(.+)$/i);
	if (!match) return { numeral: '', name: section.title.trim(), cite: section.title.trim() };
	const n = Number(match[2]);
	const isAppendix = match[1].toLowerCase() === 'appendix';
	return {
		numeral: isAppendix ? `A${n}` : toRoman(n),
		name: sentenceCase(match[3]),
		cite: isAppendix ? `Appendix ${n}` : `Article ${toRoman(n)}`
	};
}

/** The stored label at depth 0 ("III"), with a dot at depths 1 and 2 ("a.", "ii."). */
export function clauseLabel(depth: number, label: string): string {
	return depth >= 1 ? `${label}.` : label;
}

interface ClauseLike {
	clauseUid: string;
	label: string;
	children: ClauseLike[];
}

interface SectionLike {
	sectionId: string;
	title: string;
	clauses: ClauseLike[];
}

/** The labels from the top of the tree down to a clause, or null when it's gone. */
function labelPath(nodes: ClauseLike[], uid: string): string[] | null {
	for (const node of nodes) {
		if (node.clauseUid === uid) return [node.label];
		const below = labelPath(node.children ?? [], uid);
		if (below) return [node.label, ...below];
	}
	return null;
}

/**
 * Where a proposal points, the way the league cites it: "Article II, clause
 * III", "Article I, clause VII, b", "Article II, new clause under III" for a
 * sub-clause, "Article II, new clause" for a new top-level clause. A clause
 * that's no longer in the document leaves just "Article II".
 */
export function clauseRef(
	proposal: {
		proposalType: string;
		affectedSection: string | null;
		targetClauseUid: string | null;
	},
	sections: SectionLike[]
): string {
	const section = sections.find((s) => s.sectionId === proposal.affectedSection);
	if (!section) return '';
	const { cite } = articleParts(section);
	if (proposal.proposalType === 'add_clause' && !proposal.targetClauseUid) {
		return `${cite}, new clause`;
	}
	const path = proposal.targetClauseUid ? labelPath(section.clauses, proposal.targetClauseUid) : null;
	if (!path) return cite;
	if (proposal.proposalType === 'add_clause') return `${cite}, new clause under ${path.join(', ')}`;
	return `${cite}, clause ${path.join(', ')}`;
}

/** The bar's width: yes over the votes needed, as a percent held to 0 to 100. */
export function voteProgress(yes: number, requiredYes: number): number {
	if (!(requiredYes > 0)) return yes > 0 ? 100 : 0;
	return Math.max(0, Math.min(100, (yes / requiredYes) * 100));
}

/** "6 days left", "1 hour left", "closing now". Empty with no deadline. */
export function closesIn(date: string | Date | null, now: Date = new Date()): string {
	if (!date) return '';
	const ms = new Date(date).getTime() - now.getTime();
	if (Number.isNaN(ms)) return '';
	if (ms <= 0) return 'closing now';
	const days = Math.floor(ms / 86_400_000);
	if (days >= 1) return `${days} day${days === 1 ? '' : 's'} left`;
	const hours = Math.max(1, Math.floor(ms / 3_600_000));
	return `${hours} hour${hours === 1 ? '' : 's'} left`;
}

export type StatusTagVariant = 'pending' | 'clinched' | 'passed' | 'champion' | 'missed';

/** A proposal's status as a Tag: its words and its variant. */
export function statusTag(status: string): { label: string; variant: StatusTagVariant } {
	switch (status) {
		case 'draft':
			return { label: 'Draft', variant: 'pending' };
		case 'active':
			return { label: 'Voting open', variant: 'clinched' };
		case 'passed':
			return { label: 'Passed', variant: 'passed' };
		case 'rejected':
			return { label: 'Rejected', variant: 'champion' };
		case 'withdrawn':
			return { label: 'Withdrawn', variant: 'missed' };
		case 'superseded':
			return { label: 'Superseded', variant: 'missed' };
		default:
			return { label: sentenceCase(status.replace(/_/g, ' ')), variant: 'pending' };
	}
}

/** "Article 8, II" is stored; "Article VIII, II" is shown. */
export function citationLabel(citation: string): string {
	return citation.replace(/\b(Article|Appendix)\s+(\d+)/g, (_, word: string, n: string) =>
		word === 'Article' ? `Article ${toRoman(Number(n))}` : `Appendix ${n}`
	);
}

const THRESHOLD_HINTS: Record<string, string> = {
	general: 'Other changes need a super-majority.',
	scoring: 'Scoring changes need a simple majority.',
	roster_size: 'Roster size changes need a simple majority.',
	league_size: 'League size changes need every vote.',
	manager_removal: "Removing a manager needs every other manager's vote.",
	commissioner_impeachment: 'Impeaching the commissioner needs a super-majority.',
	collusion_pick_penalty: 'A draft pick penalty for collusion needs a simple majority.',
	collusion_expulsion: 'Expulsion for collusion needs a super-majority.'
};

/** The second sentence under the dialog's "What kind of change is this?". */
export function thresholdHint(category: string): string {
	return THRESHOLD_HINTS[category] ?? THRESHOLD_HINTS.general;
}

/** "Super-majority (7 of 10)" from the stored label, without the count. */
export function ruleName(label: string): string {
	return label.split(' (')[0];
}

export interface BallotRow {
	managerKey: number;
	name: string;
	/** "✓ Yes", "✗ No", "Abstain", "Not yet" ("Didn't vote" once closed), "Doesn't vote". */
	mark: string;
	tone: 'yes' | 'no' | 'abstain' | 'pending' | 'excluded';
	voted: boolean;
	comment: string | null;
}

const MARKS = {
	yes: '✓ Yes',
	no: '✗ No',
	abstain: 'Abstain'
} as const;

/**
 * One row per active member: those who voted first, newest vote first, then
 * everyone else by name. A ballot from someone no longer active still counts,
 * so it still shows. The subject of a removal reads "Doesn't vote". Once the
 * vote has closed, anyone who didn't vote reads "Didn't vote".
 */
export function ballotRows(
	members: Array<{ managerKey: number; displayName: string | null }>,
	ballots: Array<{
		managerKey: number;
		managerName: string | null;
		vote: string;
		comment: string | null;
		votedAt: string | Date | null;
	}>,
	subjectKey: number | null,
	closed = false
): BallotRow[] {
	const names = new Map(members.map((m) => [m.managerKey, m.displayName]));
	const time = (d: string | Date | null) => (d ? new Date(d).getTime() : 0);

	const voted: BallotRow[] = [...ballots]
		.sort((a, b) => time(b.votedAt) - time(a.votedAt))
		.map((b) => {
			const vote = (b.vote in MARKS ? b.vote : 'abstain') as keyof typeof MARKS;
			return {
				managerKey: b.managerKey,
				name: names.get(b.managerKey) ?? b.managerName ?? 'Unknown',
				mark: MARKS[vote],
				tone: vote,
				voted: true,
				comment: b.comment || null
			};
		});

	const seen = new Set(voted.map((r) => r.managerKey));
	const waiting: BallotRow[] = members
		.filter((m) => !seen.has(m.managerKey))
		.map((m) => {
			const excluded = m.managerKey === subjectKey;
			return {
				managerKey: m.managerKey,
				name: m.displayName ?? 'Unknown',
				mark: excluded ? "Doesn't vote" : closed ? "Didn't vote" : 'Not yet',
				tone: excluded ? ('excluded' as const) : ('pending' as const),
				voted: false,
				comment: null
			};
		})
		.sort((a, b) => a.name.localeCompare(b.name));

	return [...voted, ...waiting];
}

/**
 * "Passed 7 to 2 · takes effect in 2027", with ", 1 abstained" after the no
 * count when anyone did.
 */
export function amendmentTally(
	results: { yes: number; no: number; abstain?: number | null },
	season: number | string | null
): string {
	const abstained = results.abstain ? `, ${results.abstain} abstained` : '';
	const effect = season != null && season !== '' ? ` · takes effect in ${season}` : '';
	return `Passed ${results.yes} to ${results.no}${abstained}${effect}`;
}

/** "Jan 1, 2025" in UTC, as the Versions list shows an effective date. */
export function shortDate(date: string | Date): string {
	return new Date(date).toLocaleDateString('en-US', {
		month: 'short',
		day: 'numeric',
		year: 'numeric',
		timeZone: 'UTC'
	});
}

/** "January 1, 2025" in UTC, for the kicker. */
export function longDate(date: string | Date): string {
	return new Date(date).toLocaleDateString('en-US', {
		month: 'long',
		day: 'numeric',
		year: 'numeric',
		timeZone: 'UTC'
	});
}
