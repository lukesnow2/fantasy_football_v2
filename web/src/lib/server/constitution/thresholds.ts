/**
 * Article 8 thresholds, as code.
 *
 * The category is chosen explicitly on the proposal form. It is deliberately not
 * inferred from the proposal text: the previous implementation keyword-matched
 * 'scoring' and 'roster' against the free-text body, so a constitutional
 * amendment whose *rationale* happened to mention scoring silently downgraded
 * its own bar from super-majority to simple majority.
 */

export const PROPOSAL_CATEGORIES = [
	'scoring',
	'roster_size',
	'league_size',
	'manager_removal',
	'commissioner_impeachment',
	'collusion_pick_penalty',
	'collusion_expulsion',
	'general'
] as const;

export type ProposalCategory = (typeof PROPOSAL_CATEGORIES)[number];

export type ThresholdRule = 'simple_majority' | 'super_majority' | 'unanimous';

export interface Threshold {
	rule: ThresholdRule;
	/** Yes votes needed to pass. */
	requiredYes: number;
	/** The denominator: everyone entitled to vote on this proposal. */
	eligibleVoters: number;
	/** Where in the constitution this bar comes from, for display. */
	citation: string;
	/** Human-readable, e.g. "Simple majority (6 of 10)". */
	label: string;
}

/**
 * More than half.
 *
 * Note `Math.floor(n / 2) + 1`, not `Math.ceil(n / 2)`. For an even league the
 * latter returns 5 of 10 — a tie, not a majority. That was a real bug: the code
 * returned 5 while the page told managers it needed 6.
 */
export function simpleMajority(n: number): number {
	return Math.floor(n / 2) + 1;
}

/**
 * At least two thirds.
 *
 * superMajority(10) === 7, which matches Article 5's literal "seven votes to
 * impeach the commissioner" — an independent check that the rounding is right.
 */
export function superMajority(n: number): number {
	return Math.ceil((n * 2) / 3);
}

const CATEGORY_RULES: Record<ProposalCategory, { rule: ThresholdRule; citation: string }> = {
	scoring: { rule: 'simple_majority', citation: 'Article 8, I' },
	roster_size: { rule: 'simple_majority', citation: 'Article 8, I' },
	league_size: { rule: 'unanimous', citation: 'Article 8, III' },
	manager_removal: { rule: 'unanimous', citation: 'Article 8, IV' },
	commissioner_impeachment: { rule: 'super_majority', citation: 'Article 5, II' },
	collusion_pick_penalty: { rule: 'simple_majority', citation: 'Article 6, III' },
	collusion_expulsion: { rule: 'super_majority', citation: 'Article 6, III' },
	general: { rule: 'super_majority', citation: 'Article 8, II' }
};

export const CATEGORY_LABELS: Record<ProposalCategory, string> = {
	scoring: 'Scoring change',
	roster_size: 'Roster size change',
	league_size: 'League size change',
	manager_removal: 'Remove a manager',
	commissioner_impeachment: 'Impeach the commissioner',
	collusion_pick_penalty: 'Collusion — draft pick penalty',
	collusion_expulsion: 'Collusion — expulsion',
	general: 'Other constitutional change'
};

export function isProposalCategory(value: unknown): value is ProposalCategory {
	return typeof value === 'string' && (PROPOSAL_CATEGORIES as readonly string[]).includes(value);
}

const RULE_NAMES: Record<ThresholdRule, string> = {
	simple_majority: 'Simple majority',
	super_majority: 'Super-majority',
	unanimous: 'Unanimous'
};

/**
 * Resolve the bar for a proposal.
 *
 * `activeMemberCount` must come from the league_member allowlist — the set of
 * people who can actually sign in and vote — never from a hardcoded 10 and never
 * from edw.dim_manager, which the Python ETL rewrites and which would therefore
 * let a data pipeline run change what counts as a super-majority.
 *
 * For `manager_removal`, Article 8 IV says "unanimous minus the manager in
 * question": the subject is removed from the denominator and barred from voting,
 * so the requirement falls out of the same formula rather than being special-cased.
 */
export function resolveThreshold(
	category: ProposalCategory,
	activeMemberCount: number,
	options: { hasSubject?: boolean } = {}
): Threshold {
	const { rule, citation } = CATEGORY_RULES[category];

	const eligibleVoters =
		category === 'manager_removal' && options.hasSubject
			? Math.max(activeMemberCount - 1, 0)
			: activeMemberCount;

	const requiredYes =
		rule === 'unanimous'
			? eligibleVoters
			: rule === 'super_majority'
				? superMajority(eligibleVoters)
				: simpleMajority(eligibleVoters);

	return {
		rule,
		requiredYes,
		eligibleVoters,
		citation,
		label: `${RULE_NAMES[rule]} (${requiredYes} of ${eligibleVoters})`
	};
}
