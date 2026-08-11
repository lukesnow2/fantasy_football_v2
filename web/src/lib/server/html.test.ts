import { describe, expect, it } from 'vitest';
import { escapeHtml } from './html';

describe('escapeHtml', () => {
	it('escapes all five characters that can break out of a body or an attribute', () => {
		expect(escapeHtml(`&<>"'`)).toBe('&amp;&lt;&gt;&quot;&#39;');
	});

	it('replaces the ampersand first', () => {
		// The branch that matters. Reordering the chain so `&` runs last leaves
		// every entity the other rules produce double-escaped — the output reads
		// "&amp;lt;" as literal text in the mail client instead of "<".
		expect(escapeHtml('&lt;')).toBe('&amp;lt;');
		expect(escapeHtml('<')).toBe('&lt;');
	});

	it('renders an anchor in a rationale inert', () => {
		// The case the doc comment describes: a proposal rationale is free text
		// from one manager, mailed to nine others from the league's own address.
		const escaped = escapeHtml('<a href="https://evil.example">click</a>');
		expect(escaped).not.toContain('<a');
		expect(escaped).not.toContain('"');
	});

	it('leaves ordinary prose untouched', () => {
		// Guards the opposite failure: over-eager escaping mangling every
		// proposal title that contains no markup at all.
		const plain = 'Trade deadline moves to week 10';
		expect(escapeHtml(plain)).toBe(plain);
	});
});
