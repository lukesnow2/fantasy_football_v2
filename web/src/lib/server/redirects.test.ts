import { describe, expect, it } from 'vitest';
import { safeRedirect } from './redirects';

describe('safeRedirect', () => {
	it('keeps ordinary same-origin paths', () => {
		expect(safeRedirect('/constitution')).toBe('/constitution');
		expect(safeRedirect('/settings?tab=email')).toBe('/settings?tab=email');
		expect(safeRedirect('/')).toBe('/');
	});

	it('rejects absolute URLs', () => {
		expect(safeRedirect('https://evil.com')).toBeNull();
		expect(safeRedirect('http://evil.com/x')).toBeNull();
	});

	it('rejects protocol-relative URLs', () => {
		// The whole point: these start with "/" but browsers treat them as
		// absolute, so a naive startsWith('/') check hands an attacker a
		// phishing hop off a trusted domain.
		expect(safeRedirect('//evil.com')).toBeNull();
		expect(safeRedirect('//evil.com/login')).toBeNull();
	});

	it('rejects backslash-escaped protocol-relative URLs', () => {
		expect(safeRedirect('/\\evil.com')).toBeNull();
	});

	it('rejects empty and missing values', () => {
		expect(safeRedirect(null)).toBeNull();
		expect(safeRedirect(undefined)).toBeNull();
		expect(safeRedirect('')).toBeNull();
	});

	it('rejects scheme-relative payloads that do not start with a slash', () => {
		expect(safeRedirect('javascript:alert(1)')).toBeNull();
		expect(safeRedirect('evil.com')).toBeNull();
	});
});
