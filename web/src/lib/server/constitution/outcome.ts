import type { Threshold } from './thresholds';

export interface Tally {
	yes: number;
	no: number;
	abstain: number;
}

export type Outcome =
	| {
			state: 'open';
			threshold: Threshold;
			tally: Tally;
			/** False once yes can no longer reach the bar even if everyone left votes yes. */
			canStillPass: boolean;
			closesAt: Date | null;
	  }
	| { state: 'passed'; threshold: Threshold; tally: Tally }
	| {
			state: 'rejected';
			threshold: Threshold;
			tally: Tally;
			reason: 'unreachable' | 'deadline';
	  };

/**
 * Decide where a proposal stands. Pure — no database, no clock, no I/O.
 *
 * This is the single source of truth for passage, and the reason it is a
 * separate pure function is that two call sites need the answer (recording a
 * vote, and loading the page) and they must never be able to disagree.
 *
 * Abstentions count against the threshold, exactly as not voting does: the bar
 * is measured against every eligible voter, not against ballots cast. That is
 * the literal reading of "unanimous" and of Article 5's "seven votes", and it
 * means quorum needs no separate rule — non-participation is already a no.
 */
export function computeOutcome(args: {
	status: string;
	votingEndDate: Date | null;
	threshold: Threshold;
	tally: Tally;
	now: Date;
}): Outcome {
	const { status, votingEndDate, threshold, tally, now } = args;

	// Terminal states are reported back as-is; settling is not re-litigated.
	if (status === 'passed') return { state: 'passed', threshold, tally };
	if (status === 'rejected') {
		return { state: 'rejected', threshold, tally, reason: 'deadline' };
	}

	// Passed the moment the bar is met. No later vote can unmake it, so holding
	// the proposal open until a deadline would be theatre.
	if (tally.yes >= threshold.requiredYes) {
		return { state: 'passed', threshold, tally };
	}

	// Every eligible voter who has not voted yes is a potential yes. Once there
	// are too few of them left, the outcome is decided and we close early rather
	// than run out the clock.
	const potentialYes = threshold.eligibleVoters - tally.no - tally.abstain;
	const canStillPass = potentialYes >= threshold.requiredYes;

	if (!canStillPass) {
		return { state: 'rejected', threshold, tally, reason: 'unreachable' };
	}

	if (votingEndDate && now.getTime() >= votingEndDate.getTime()) {
		return { state: 'rejected', threshold, tally, reason: 'deadline' };
	}

	return { state: 'open', threshold, tally, canStillPass, closesAt: votingEndDate };
}
