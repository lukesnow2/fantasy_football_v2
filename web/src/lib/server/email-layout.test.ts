import { readFileSync } from 'node:fs';
import { describe, expect, it, vi } from 'vitest';

vi.mock('$env/dynamic/private', () => ({
	env: {
		EMAIL_PROVIDER: 'console',
		EMAIL_FROM: 'league@example.com',
		EMAIL_FROM_NAME: 'The League',
		ORIGIN: 'https://league.example',
		NODE_ENV: 'test'
	}
}));

const { EMAIL_COLORS, emailButton, emailGreeting, emailHost, emailLayout } = await import(
	'./email-layout'
);
const { generateMagicLinkEmail } = await import('./email');
const { newProposalEmail, voteResultEmail } = await import('./notify');

const ORIGIN = 'https://league.example';

/**
 * The WP1 tokens the emails may use, by name. Each value is checked against
 * web/src/app.css below, so a token that changes there fails here instead of
 * leaving the emails on a stale color.
 */
const ALLOWED: Record<string, string> = {
	clubhouse: '#15382A',
	'on-clubhouse-2': '#A9C7B6',
	card: '#FFFFFF',
	ink: '#18231D',
	'ink-2': '#45524A',
	muted: '#5D6A62',
	line: '#DCE0DA',
	'line-soft': '#E6E9E4',
	wash: '#EDEFEB',
	field: '#1E6B45',
	'field-wash': '#EDF4EF',
	'field-line': '#C9DDD0',
	'pennant-dark': '#9A3F14'
};

const threshold = {
	rule: 'super_majority',
	requiredYes: 7,
	eligibleVoters: 10,
	citation: 'Article 8, II',
	label: 'Super-majority (7 of 10)'
} as never;

function allFour() {
	const to = { email: 'a@b.c', displayName: 'Luke S' };
	const link = `${ORIGIN}/login/verify?token=abc`;
	return [
		generateMagicLinkEmail(link, 'a@b.c', { purpose: 'login', displayName: 'Luke S' }),
		generateMagicLinkEmail(link, 'a@b.c', { purpose: 'invite', displayName: 'Luke S' }),
		newProposalEmail(
			{
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
			},
			to,
			ORIGIN
		),
		voteResultEmail(
			{ title: 'Move the trade deadline back a week', effectiveSeason: 2027, clause: 'Article II, clause III' },
			{ state: 'passed', threshold, tally: { yes: 8, no: 1, abstain: 1 } },
			to,
			ORIGIN
		)
	];
}

/** Every hex color in a string, skipping numeric entities such as `&#39;`. */
function hexes(s: string): string[] {
	return [...s.matchAll(/(?<!&)#([0-9a-f]{8}|[0-9a-f]{6}|[0-9a-f]{3})\b/gi)].map((m) =>
		`#${m[1]}`.toUpperCase()
	);
}

describe('the token map', () => {
	it('matches the WP1 tokens in app.css', () => {
		const css = readFileSync(new URL('../../app.css', import.meta.url), 'utf8');
		for (const [name, hex] of Object.entries(ALLOWED)) {
			const match = css.match(new RegExp(`--color-${name}:\\s*(#[0-9a-fA-F]{3,8})\\s*;`));
			expect(match?.[1]?.toUpperCase(), name).toBe(hex);
		}
	});

	it('is the list the layout writes from', () => {
		expect(new Set(Object.values(EMAIL_COLORS))).toEqual(new Set(Object.values(ALLOWED)));
	});
});

describe('every email', () => {
	it('uses only WP1 token colors', () => {
		const allowed = new Set(Object.values(ALLOWED));
		for (const mail of allFour()) {
			const found = hexes(mail.html);
			expect(found.length).toBeGreaterThan(0);
			for (const hex of found) expect(allowed, `${mail.subject}: ${hex}`).toContain(hex);
		}
	});

	it('carries no em or en dash in its HTML, text or subject', () => {
		for (const mail of allFour()) {
			for (const part of [mail.html, mail.text ?? '', mail.subject]) {
				expect(part).not.toMatch(/[–—]/);
			}
		}
	});

	it('is a whole document asking for light mode, with the preheader first', () => {
		for (const mail of allFour()) {
			expect(mail.html.startsWith('<!doctype html>')).toBe(true);
			expect(mail.html).toContain('<html lang="en"');
			expect(mail.html).toContain('<meta charset="utf-8">');
			expect(mail.html).toContain('name="viewport" content="width=device-width');
			expect(mail.html).toContain('<meta name="color-scheme" content="light">');
			expect(mail.html).toContain('<meta name="supported-color-schemes" content="light">');
			expect(mail.html).toContain('fonts.googleapis.com');
			expect(mail.html.match(/<style>/g)).toHaveLength(1);
			const body = mail.html.slice(mail.html.indexOf('<body'));
			expect(body.indexOf('display: none; max-height: 0; overflow: hidden;')).toBeLessThan(
				body.indexOf('The League</span>')
			);
		}
	});

	it('signs off with the host, not the origin', () => {
		for (const mail of allFour()) {
			expect(mail.html).toContain('The League · <span style="font-family: \'IBM Plex Mono\'');
			expect(mail.html).toContain('>league.example</span>');
		}
	});
});

describe('emailLayout', () => {
	const base = {
		preheader: 'pre',
		kicker: 'kick',
		title: 'title',
		bodyHtml: '<p>body</p>',
		footnoteHtml: 'foot',
		origin: ORIGIN
	};

	it('escapes the title, kicker, meta and preheader', () => {
		const html = emailLayout({
			...base,
			preheader: '<b>pre</b>',
			kicker: '<i>kick</i>',
			title: '<script>t</script>',
			meta: '"meta" & <u>more</u>'
		});
		expect(html).not.toContain('<b>pre</b>');
		expect(html).not.toContain('<i>kick</i>');
		expect(html).not.toContain('<script>');
		expect(html).not.toContain('<u>more</u>');
		expect(html).toContain('&lt;b&gt;pre&lt;/b&gt;');
		expect(html).toContain('&lt;i&gt;kick&lt;/i&gt;');
		expect(html).toContain('&lt;script&gt;t&lt;/script&gt;');
		expect(html).toContain('&quot;meta&quot; &amp; &lt;u&gt;more&lt;/u&gt;');
	});

	it('leaves the body and footnote as the caller wrote them', () => {
		const html = emailLayout({ ...base, footnoteHtml: '<a href="x">link</a>' });
		expect(html).toContain('<p>body</p>');
		expect(html).toContain('<a href="x">link</a>');
	});

	it('sets the kicker in field, or pennant-dark', () => {
		expect(emailLayout(base)).toMatch(/text-transform: uppercase; color: #1E6B45;">kick</);
		expect(emailLayout({ ...base, kickerTone: 'pennant' })).toMatch(
			/text-transform: uppercase; color: #9A3F14;">kick</
		);
	});

	it('spaces the title for a meta line, and leaves the line out when empty', () => {
		const withMeta = emailLayout({ ...base, meta: 'Proposed by Luke' });
		expect(withMeta).toContain('margin: 8px 0 10px;');
		expect(withMeta).toContain('Proposed by Luke</p>');
		const without = emailLayout({ ...base, meta: '' });
		expect(without).toContain('margin: 8px 0 18px;');
		expect(without).not.toContain('margin: 0 0 20px;');
	});

	it('sets the title at 32px by default and 28px for proposals', () => {
		expect(emailLayout(base)).toContain('font-size: 32px; line-height: 1.1;');
		expect(emailLayout({ ...base, titleSize: 28 })).toContain('font-size: 28px; line-height: 1.15;');
	});
});

describe('emailButton', () => {
	it('escapes its href and its label', () => {
		const html = emailButton('https://x.y/?a=1&b="2"', '<b>Go</b>');
		expect(html).toContain('href="https://x.y/?a=1&amp;b=&quot;2&quot;"');
		expect(html).not.toContain('<b>Go</b>');
	});

	it('fills the cell, so Outlook keeps a green block', () => {
		const html = emailButton('https://x.y', 'Go');
		expect(html).toContain('bgcolor="#1E6B45"');
		expect(html).toContain('line-height: 50px;');
	});
});

describe('emailGreeting', () => {
	it('uses the first word of the display name', () => {
		expect(emailGreeting('Luke S')).toEqual({ html: 'Hi Luke,', text: 'Hi Luke,' });
		expect(emailGreeting('  Craig  ')).toEqual({ html: 'Hi Craig,', text: 'Hi Craig,' });
	});

	it('says "Hi," with no name', () => {
		expect(emailGreeting(null).text).toBe('Hi,');
		expect(emailGreeting('').text).toBe('Hi,');
		expect(emailGreeting('   ').html).toBe('Hi,');
	});

	it("escapes the HTML greeting only", () => {
		expect(emailGreeting("O'Brien")).toEqual({ html: 'Hi O&#39;Brien,', text: "Hi O'Brien," });
	});
});

describe('emailHost', () => {
	it('drops the scheme and any trailing slash', () => {
		expect(emailHost('https://oakdalepark.xyz')).toBe('oakdalepark.xyz');
		expect(emailHost('http://localhost:5173/')).toBe('localhost:5173');
	});
});
