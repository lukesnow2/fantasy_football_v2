import { describe, expect, it, vi } from 'vitest';

vi.mock('$env/dynamic/private', () => ({
	env: { EMAIL_PROVIDER: 'console', ORIGIN: 'https://league.example', NODE_ENV: 'test' }
}));

const { newProposalEmail, voteResultEmail, voteResultCopy, wordingHalves, closesLabel } =
	await import('./notify');

const ORIGIN = 'https://league.example';
const TO = { email: 'a@b.c', displayName: 'Luke S' };
const threshold = {
	rule: 'super_majority',
	requiredYes: 7,
	eligibleVoters: 10,
	citation: 'Article 8, II',
	label: 'Super-majority (7 of 10)'
} as never;
const tally = { yes: 8, no: 1, abstain: 1 };

const PROPOSAL = {
	title: 'Move the trade deadline back a week',
	rationale: 'More time to deal.',
	requiredVotes: 7,
	eligibleVoters: 10,
	effectiveSeason: 2027,
	votingEndDate: new Date('2026-11-22T18:00:00Z'),
	authorName: 'Luke S',
	proposalType: 'edit_clause',
	currentLanguage: 'The trade deadline starts after week 10.',
	proposedLanguage: 'The trade deadline starts after week 11.',
	clause: 'Article II, clause III'
};
const RESULT = { title: PROPOSAL.title, effectiveSeason: 2027, clause: 'Article II, clause III' };

describe('newProposalEmail', () => {
	const mail = newProposalEmail(PROPOSAL, TO, ORIGIN);

	it('keeps the subject', () => {
		expect(mail.subject).toBe('New rule proposal: Move the trade deadline back a week');
	});

	it('writes the board copy', () => {
		expect(mail.html).toContain('color: #9A3F14;">Constitution · Voting open</div>');
		expect(mail.html).toContain(
			'Proposed by Luke S · takes effect in 2027 · Article II, clause III</p>'
		);
		expect(mail.html).toContain('>Hi Luke,</p>');
		expect(mail.html).toContain('A rule change is open for voting. Here&#39;s what it changes.');
		expect(mail.html).toContain('>7 of 10</div>');
		expect(mail.html).toContain('>Yes votes to pass</div>');
		expect(mail.html).toContain('>Sun, Nov 22</div>');
		expect(mail.html).toContain('>Voting closes</div>');
		expect(mail.html).toMatch(/href="https:\/\/league\.example\/constitution"[^>]*>Cast your vote<\/a>/);
		expect(mail.html).toContain('href="https://league.example/settings"');
		expect(mail.html).toContain(
			'Not voting counts the same as voting no. You\'re getting this because new proposals are on in your'
		);
		expect(mail.html).toContain('Luke S proposed a rule change. Voting closes Sun, Nov 22.');
	});

	it('leaves the clause out of the meta line when there is none', () => {
		const html = newProposalEmail({ ...PROPOSAL, clause: '' }, TO, ORIGIN).html;
		expect(html).toContain('Proposed by Luke S · takes effect in 2027</p>');
	});

	it('reads "Soon" with no end date', () => {
		expect(closesLabel(null)).toBe('Soon');
		const m = newProposalEmail({ ...PROPOSAL, votingEndDate: null }, TO, ORIGIN);
		expect(m.html).toContain('>Soon</div>');
		expect(m.text).toContain('Voting closes soon.');
	});

	it('dates the deadline in the league time zone', () => {
		// 03:00 UTC Monday is still Sunday evening in the league's zone.
		expect(closesLabel(new Date('2026-11-23T03:00:00Z'))).toBe('Sun, Nov 22');
	});

	it('escapes free text', () => {
		const m = newProposalEmail(
			{ ...PROPOSAL, title: '<b>t</b>', rationale: '<a href="x">r</a>', proposedLanguage: '<i>p</i>' },
			TO,
			ORIGIN
		);
		expect(m.html).not.toContain('<b>t</b>');
		expect(m.html).not.toContain('<a href="x">');
		expect(m.html).not.toContain('<i>p</i>');
	});

	it('writes the text version', () => {
		expect(mail.text).toBe(`Move the trade deadline back a week

Proposed by Luke S · takes effect in 2027 · Article II, clause III

Current wording:
The trade deadline starts after week 10.

Proposed wording:
The trade deadline starts after week 11.

Why:
More time to deal.

7 of 10 yes votes to pass. Voting closes Sun, Nov 22.

Not voting counts the same as voting no.

https://league.example/constitution
`);
	});
});

describe('wording labels by proposal type', () => {
	it('edit_clause shows both halves', () => {
		const m = newProposalEmail(PROPOSAL, TO, ORIGIN);
		expect(m.html).toContain('>Current wording</div>');
		expect(m.html).toContain('>Proposed wording</div>');
	});

	it('add_clause shows only the new clause', () => {
		const p = { ...PROPOSAL, proposalType: 'add_clause', currentLanguage: null };
		expect(wordingHalves(p).map((h) => h.label)).toEqual(['New clause']);
		const m = newProposalEmail(p, TO, ORIGIN);
		expect(m.html).toContain('>New clause</div>');
		expect(m.html).not.toContain('>Current wording</div>');
		expect(m.html).not.toContain('>Proposed wording</div>');
		expect(m.text).toContain('New clause:\nThe trade deadline starts after week 11.');
	});

	it('delete_clause shows only the clause to remove', () => {
		const p = { ...PROPOSAL, proposalType: 'delete_clause' };
		expect(wordingHalves(p).map((h) => h.label)).toEqual(['Clause to remove']);
		const m = newProposalEmail(p, TO, ORIGIN);
		expect(m.html).toContain('>Clause to remove</div>');
		expect(m.html).not.toContain('week 11');
		expect(m.text).toContain('Clause to remove:\nThe trade deadline starts after week 10.');
	});
});

describe('voteResultEmail', () => {
	const cases = [
		{
			outcome: { state: 'passed', threshold, tally } as const,
			kicker: 'Constitution · Passed',
			color: '#1E6B45',
			subject: 'Passed: Move the trade deadline back a week',
			sentence: 'It passed. The constitution now has the new wording, and it takes effect in 2027.',
			button: 'Read the new wording',
			meta: 'Takes effect in 2027 · Article II, clause III'
		},
		{
			outcome: { state: 'superseded', threshold, tally } as const,
			kicker: 'Constitution · Passed, not applied',
			color: '#9A3F14',
			subject: 'Passed but not applied: Move the trade deadline back a week',
			sentence:
				"It passed, but the clause it changed no longer exists, so the constitution didn't change. The commissioner will need to propose it again against the current wording.",
			button: 'See the record',
			meta: 'Article II, clause III'
		},
		{
			outcome: { state: 'rejected', threshold, tally, reason: 'unreachable' } as const,
			kicker: "Constitution · Didn't pass",
			color: '#9A3F14',
			subject: "Didn't pass: Move the trade deadline back a week",
			sentence: "It didn't pass. It could no longer reach 7 yes votes, so voting closed early.",
			button: 'See the record',
			meta: 'Article II, clause III'
		},
		{
			outcome: { state: 'rejected', threshold, tally, reason: 'deadline' } as const,
			kicker: "Constitution · Didn't pass",
			color: '#9A3F14',
			subject: "Didn't pass: Move the trade deadline back a week",
			sentence: "It didn't pass. Voting closed before it reached 7 yes votes.",
			button: 'See the record',
			meta: 'Article II, clause III'
		}
	];

	for (const c of cases) {
		const label = c.outcome.state + ('reason' in c.outcome ? ` (${c.outcome.reason})` : '');
		it(`writes the ${label} email`, () => {
			const mail = voteResultEmail(RESULT, c.outcome, TO, ORIGIN);
			const esc = (s: string) => s.replace(/'/g, '&#39;');
			expect(mail.subject).toBe(c.subject);
			expect(mail.html).toContain(`color: ${c.color};">${esc(c.kicker)}</div>`);
			expect(mail.html).toContain(`${esc(c.sentence)}</p>`);
			expect(mail.html).toMatch(new RegExp(`>${c.button}</a>`));
			expect(mail.html).toContain(`${c.meta}</p>`);
			expect(voteResultCopy(c.outcome, 2027).button).toBe(c.button);
			expect(mail.text).toBe(`Move the trade deadline back a week

${c.sentence}

Final vote: 8 yes, 1 no, 1 abstain. Needed 7 of 10.

https://league.example/constitution
`);
		});
	}

	it('shows the tally in four cells', () => {
		const html = voteResultEmail(RESULT, cases[0].outcome, TO, ORIGIN).html;
		expect(html).toContain('color: #1E6B45;">8</div>');
		expect(html).toContain('color: #9A3F14;">1</div>');
		expect(html).toContain('color: #18231D;">1</div>');
		expect(html).toContain('>7 of 10</div>');
		for (const l of ['Yes', 'No', 'Abstain', 'Needed to pass']) expect(html).toContain(`>${l}</div>`);
		expect(html).toContain('vote results are on in your');
	});

	it('drops the meta line when a failed proposal has no clause', () => {
		const html = voteResultEmail({ ...RESULT, clause: '' }, cases[2].outcome, TO, ORIGIN).html;
		expect(html).toContain('margin: 8px 0 18px;');
	});
});
