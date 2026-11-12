/**
 * S1.1: every token in the build spec's Tokens table is in app.css's @theme,
 * with the name and value shown there.
 */
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const css = readFileSync(fileURLToPath(new URL('../../../app.css', import.meta.url)), 'utf8');
const recordBook = css.slice(
	css.indexOf('@theme static {'),
	css.indexOf('}', css.indexOf('@theme static {'))
);

const TOKENS: Record<string, string> = {
	'--color-ground': '#F5F6F3',
	'--color-card': '#FFFFFF',
	'--color-ink': '#18231D',
	'--color-ink-2': '#45524A',
	'--color-muted': '#5D6A62',
	'--color-faint': '#6B776F',
	'--color-line': '#DCE0DA',
	'--color-line-soft': '#E6E9E4',
	'--color-line-strong': '#C7CDC6',
	'--color-wash': '#EDEFEB',
	'--color-clubhouse': '#15382A',
	'--color-clubhouse-2': '#1D4534',
	'--color-clubhouse-3': '#24503D',
	'--color-clubhouse-line': '#4F7A65',
	'--color-on-clubhouse': '#CFE0D6',
	'--color-on-clubhouse-2': '#A9C7B6',
	'--color-field': '#1E6B45',
	'--color-field-2': '#2C5A45',
	'--color-field-tint': '#E3EFE7',
	'--color-field-wash': '#EDF4EF',
	'--color-field-line': '#C9DDD0',
	'--color-sage': '#7FA790',
	'--color-sage-tint': '#B9D3C3',
	'--color-pennant': '#C4531E',
	'--color-pennant-dark': '#9A3F14',
	'--color-pennant-light': '#F0A06E',
	'--color-pennant-tint': '#FBE9DF',
	'--color-pennant-line': '#F2CDB7',
	'--color-pennant-wash': '#FFFBF8',
	'--color-mention': '#FFF6F0',
	'--color-ink-brown': '#6B3A1E',
	'--color-chart-teal': '#2E8B8B',
	'--color-chart-rose': '#C2549A',
	'--color-pos-qb': '#C8372D',
	'--color-pos-qb-tint': '#FBE4E1',
	'--color-pos-rb': '#2F63C7',
	'--color-pos-rb-tint': '#E1EBFA',
	'--color-pos-wr': '#2E8B4E',
	'--color-pos-wr-tint': '#E2F1E6',
	'--color-pos-te': '#B88A05',
	'--color-pos-te-tint': '#FBF1CF',
	'--color-pos-k': '#8A5A2B',
	'--color-pos-k-tint': '#F2E6DA',
	'--color-pos-def': '#7A4FC2',
	'--color-pos-def-tint': '#EDE4F7',
	'--font-slab': "'Zilla Slab', Georgia, serif",
	'--font-sans': "'Instrument Sans', system-ui, sans-serif",
	'--font-mono': "'IBM Plex Mono', ui-monospace, monospace",
	'--shadow-tip': '0 14px 36px rgba(21, 56, 42, 0.3)',
	'--shadow-popover': '0 16px 40px rgba(21, 56, 42, 0.18)',
	'--shadow-dark-pop': '0 12px 32px rgba(0, 0, 0, 0.28)',
	'--shadow-dialog': '0 24px 60px rgba(0, 0, 0, 0.3)',
	'--shadow-thumb': '0 1px 3px rgba(0, 0, 0, 0.2)',
	'--radius-card': '12px',
	'--radius-control': '8px',
	'--radius-tag': '6px'
};

describe('Record Book tokens', () => {
	it.each(Object.entries(TOKENS))('%s is %s', (name, value) => {
		expect(recordBook).toContain(`${name}: ${value};`);
	});

	it('says a literal hex outside @theme is a bug', () => {
		expect(recordBook).toMatch(/literal hex anywhere outside this\s+\*\s+@theme block is a bug/);
	});
});

describe('Record Book link rules', () => {
	const rule = (selector: string) =>
		css.match(new RegExp(`\\n\\s*${selector.replace('.', '\\.')}\\s*\\{([^}]*)\\}`))?.[1] ?? '';

	it('draws a link in ink, turning field on hover', () => {
		expect(rule('a')).toContain('color: var(--color-ink);');
		expect(rule('a:hover')).toContain('color: var(--color-field);');
	});

	it('draws a field link in field, turning clubhouse on hover', () => {
		expect(rule('a.field-link')).toContain('color: var(--color-field);');
		expect(rule('a.field-link:hover')).toContain('color: var(--color-clubhouse);');
	});
});
