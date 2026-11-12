import { describe, expect, it } from 'vitest';
import {
	amendmentTally,
	articleParts,
	ballotRows,
	citationLabel,
	clauseLabel,
	clauseRef,
	closesIn,
	longDate,
	sentenceCase,
	shortDate,
	statusTag,
	thresholdHint,
	voteProgress
} from './display';

describe('articleParts', () => {
	it('numbers an article in roman numerals and sentence-cases its name', () => {
		expect(articleParts({ title: 'Article 1: League Makeup and Construction' })).toEqual({
			numeral: 'I',
			name: 'League makeup and construction',
			cite: 'Article I'
		});
		expect(articleParts({ title: 'Article 8: Amendments to the Constitution' }).numeral).toBe('VIII');
		expect(articleParts({ title: 'Article 7: Line Up rules' }).name).toBe('Line up rules');
	});

	it('numbers the appendix A1', () => {
		expect(articleParts({ title: 'Appendix 1: Scoring System' })).toEqual({
			numeral: 'A1',
			name: 'Scoring system',
			cite: 'Appendix 1'
		});
	});

	it('leaves an unexpected title as stored, with no numeral', () => {
		expect(articleParts({ title: 'Preamble' })).toEqual({
			numeral: '',
			name: 'Preamble',
			cite: 'Preamble'
		});
	});
});

describe('sentenceCase', () => {
	it('keeps a word in capitals', () => {
		expect(sentenceCase('The NFL Draft')).toBe('The NFL draft');
	});
});

describe('clauseLabel', () => {
	it('adds a dot below the top level only', () => {
		expect(clauseLabel(0, 'III')).toBe('III');
		expect(clauseLabel(1, 'a')).toBe('a.');
		expect(clauseLabel(2, 'ii')).toBe('ii.');
	});
});

const sections = [
	{
		sectionId: 'article1',
		title: 'Article 1: League Makeup and Construction',
		clauses: [
			{ clauseUid: 'c1', label: 'I', children: [] },
			{
				clauseUid: 'c7',
				label: 'VII',
				children: [
					{ clauseUid: 'c7a', label: 'a', children: [] },
					{ clauseUid: 'c7b', label: 'b', children: [] }
				]
			}
		]
	},
	{
		sectionId: 'article2',
		title: 'Article 2: Trades',
		clauses: [{ clauseUid: 'c23', label: 'III', children: [] }]
	}
];

describe('clauseRef', () => {
	it('cites an edited clause', () => {
		expect(
			clauseRef({ proposalType: 'edit_clause', affectedSection: 'article2', targetClauseUid: 'c23' }, sections)
		).toBe('Article II, clause III');
	});

	it('cites a nested clause by its path', () => {
		expect(
			clauseRef({ proposalType: 'delete_clause', affectedSection: 'article1', targetClauseUid: 'c7b' }, sections)
		).toBe('Article I, clause VII, b');
	});

	it('cites a new sub-clause under its parent', () => {
		expect(
			clauseRef({ proposalType: 'add_clause', affectedSection: 'article2', targetClauseUid: 'c23' }, sections)
		).toBe('Article II, new clause under III');
	});

	it('cites a new top-level clause', () => {
		expect(
			clauseRef({ proposalType: 'add_clause', affectedSection: 'article2', targetClauseUid: null }, sections)
		).toBe('Article II, new clause');
	});

	it('falls back to the article when the clause is gone', () => {
		expect(
			clauseRef({ proposalType: 'edit_clause', affectedSection: 'article2', targetClauseUid: 'gone' }, sections)
		).toBe('Article II');
	});

	it('is empty when the article is gone', () => {
		expect(
			clauseRef({ proposalType: 'edit_clause', affectedSection: 'article99', targetClauseUid: 'c1' }, sections)
		).toBe('');
	});
});

describe('voteProgress', () => {
	it('is 0 with no yes votes', () => expect(voteProgress(0, 7)).toBe(0));
	it('is the share of the bar part way', () => expect(voteProgress(1, 7)).toBeCloseTo(14.29, 1));
	it('is 100 exactly at the bar', () => expect(voteProgress(7, 7)).toBe(100));
	it('holds at 100 past the bar', () => expect(voteProgress(9, 7)).toBe(100));
	it('handles a bar of zero', () => {
		expect(voteProgress(0, 0)).toBe(0);
		expect(voteProgress(1, 0)).toBe(100);
	});
});

describe('closesIn', () => {
	const now = new Date('2026-11-12T12:00:00Z');
	const at = (ms: number) => new Date(now.getTime() + ms);

	it('counts whole days', () => {
		expect(closesIn(at(6.5 * 86_400_000), now)).toBe('6 days left');
		expect(closesIn(at(1.2 * 86_400_000), now)).toBe('1 day left');
	});

	it('counts hours under a day', () => {
		expect(closesIn(at(3_600_000), now)).toBe('1 hour left');
		expect(closesIn(at(5.5 * 3_600_000), now)).toBe('5 hours left');
	});

	it('rounds anything under an hour up to one', () => {
		expect(closesIn(at(10 * 60_000), now)).toBe('1 hour left');
	});

	it('reads closing now past the deadline', () => {
		expect(closesIn(at(-1000), now)).toBe('closing now');
	});

	it('is empty with no deadline', () => expect(closesIn(null, now)).toBe(''));

	it('is empty for a date that does not parse', () => expect(closesIn('not a date', now)).toBe(''));
});

describe('statusTag', () => {
	it.each([
		['draft', 'Draft', 'pending'],
		['active', 'Voting open', 'clinched'],
		['passed', 'Passed', 'passed'],
		['rejected', 'Rejected', 'champion'],
		['withdrawn', 'Withdrawn', 'missed'],
		['superseded', 'Superseded', 'missed']
	])('%s reads %s', (status, label, variant) => {
		expect(statusTag(status)).toEqual({ label, variant });
	});
});

describe('citationLabel', () => {
	it('turns the article number into a numeral', () => {
		expect(citationLabel('Article 8, II')).toBe('Article VIII, II');
		expect(citationLabel('Article 5, II')).toBe('Article V, II');
	});
});

describe('thresholdHint', () => {
	it.each([
		['general', 'Other changes need a super-majority.'],
		['scoring', 'Scoring changes need a simple majority.'],
		['roster_size', 'Roster size changes need a simple majority.'],
		['league_size', 'League size changes need every vote.'],
		['manager_removal', "Removing a manager needs every other manager's vote."],
		['commissioner_impeachment', 'Impeaching the commissioner needs a super-majority.'],
		['collusion_pick_penalty', 'A draft pick penalty for collusion needs a simple majority.'],
		['collusion_expulsion', 'Expulsion for collusion needs a super-majority.']
	])('%s', (category, hint) => expect(thresholdHint(category)).toBe(hint));
});

describe('ballotRows', () => {
	const members = [
		{ managerKey: 1, displayName: 'Luke S' },
		{ managerKey: 2, displayName: 'Craig' },
		{ managerKey: 3, displayName: 'Omar' },
		{ managerKey: 4, displayName: 'Erik Snow' }
	];

	it('puts voters first, newest first, then everyone else by name', () => {
		const rows = ballotRows(
			members,
			[
				{ managerKey: 3, managerName: 'Omar', vote: 'no', comment: null, votedAt: '2026-11-10T00:00:00Z' },
				{ managerKey: 1, managerName: 'Luke S', vote: 'yes', comment: 'Long overdue', votedAt: '2026-11-11T00:00:00Z' }
			],
			null
		);
		expect(rows.map((r) => r.name)).toEqual(['Luke S', 'Omar', 'Craig', 'Erik Snow']);
		expect(rows.map((r) => r.mark)).toEqual(['✓ Yes', '✗ No', 'Not yet', 'Not yet']);
		expect(rows[0].comment).toBe('Long overdue');
		expect(rows[2].voted).toBe(false);
	});

	it('marks an abstention', () => {
		const [row] = ballotRows(
			members,
			[{ managerKey: 2, managerName: 'Craig', vote: 'abstain', comment: '', votedAt: null }],
			null
		);
		expect(row).toMatchObject({ mark: 'Abstain', tone: 'abstain', comment: null });
	});

	it("reads Doesn't vote for the subject of a removal", () => {
		const rows = ballotRows(members, [], 3);
		expect(rows.find((r) => r.managerKey === 3)).toMatchObject({ mark: "Doesn't vote", tone: 'excluded' });
	});

	it("reads Didn't vote once the vote has closed", () => {
		const rows = ballotRows(members, [], 3, true);
		expect(rows.find((r) => r.managerKey === 1)?.mark).toBe("Didn't vote");
		expect(rows.find((r) => r.managerKey === 3)?.mark).toBe("Doesn't vote");
	});

	it('keeps a ballot from someone no longer active', () => {
		const rows = ballotRows(
			members,
			[{ managerKey: 9, managerName: 'Bobby', vote: 'yes', comment: null, votedAt: null }],
			null
		);
		expect(rows[0]).toMatchObject({ name: 'Bobby', mark: '✓ Yes' });
		expect(rows).toHaveLength(5);
	});
});

describe('amendmentTally', () => {
	it('reads yes to no and the season', () => {
		expect(amendmentTally({ yes: 7, no: 2, abstain: 0 }, 2027)).toBe('Passed 7 to 2 · takes effect in 2027');
	});

	it('adds abstentions after the no count', () => {
		expect(amendmentTally({ yes: 7, no: 2, abstain: 1 }, 2027)).toBe(
			'Passed 7 to 2, 1 abstained · takes effect in 2027'
		);
	});
});

describe('dates', () => {
	it('formats in UTC, so a midnight date stays on its day', () => {
		expect(shortDate('2025-01-01T00:00:00Z')).toBe('Jan 1, 2025');
		expect(longDate('2025-01-01T00:00:00Z')).toBe('January 1, 2025');
	});
});
