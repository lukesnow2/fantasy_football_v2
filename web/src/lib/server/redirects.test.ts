import { describe, expect, it } from 'vitest';
import { safeRedirect } from './redirects';

describe('safeRedirect', () => {
	it('keeps ordinary same-origin paths', () => {
		expect(safeRedirect('/constitution')).toBe('/constitution');
		expect(safeRedirect('/settings?tab=email')).toBe('/settings?tab=email');
		expect(safeRedirect('/managers/luke-s#stats')).toBe('/managers/luke-s#stats');
		expect(safeRedirect('/')).toBe('/');
	});

	it('rejects absolute URLs', () => {
		expect(safeRedirect('https://evil.com')).toBeNull();
		expect(safeRedirect('http://evil.com/x')).toBeNull();
	});

	it('rejects protocol-relative URLs', () => {
		// These start with "/" but browsers treat them as absolute, so a naive
		// startsWith('/') check hands an attacker a phishing hop off a trusted
		// domain.
		expect(safeRedirect('//evil.com')).toBeNull();
		expect(safeRedirect('//evil.com/login')).toBeNull();
	});

	it('rejects backslash-escaped protocol-relative URLs', () => {
		// Backslash is treated as a slash in special schemes.
		expect(safeRedirect('/\\evil.com')).toBeNull();
		expect(safeRedirect('\\\\evil.com')).toBeNull();
	});

	it('rejects control characters that the URL parser strips before parsing', () => {
		// The load-bearing case, and the one the previous prefix-matching version
		// let through. WHATWG URL parsing removes ASCII tab/LF/CR from the input
		// *before* parsing, so "/\t/evil.com" is literally "//evil.com" by the
		// time a browser resolves the Location header — while the raw string
		// starts with "/" followed by a tab, defeating a prefix check.
		expect(safeRedirect('/\t/evil.com')).toBeNull();
		expect(safeRedirect('/\n/evil.com')).toBeNull();
		expect(safeRedirect('/\r/evil.com')).toBeNull();
		expect(safeRedirect('/\t\t/evil.com')).toBeNull();
		// Same trick against the backslash form.
		expect(safeRedirect('/\t\\evil.com')).toBeNull();
	});

	it('rejects CR/LF anywhere, which would also be header injection', () => {
		expect(safeRedirect('/ok\r\nSet-Cookie: a=b')).toBeNull();
		expect(safeRedirect('/ok\nLocation: https://evil.com')).toBeNull();
	});

	it('rejects empty and missing values', () => {
		expect(safeRedirect(null)).toBeNull();
		expect(safeRedirect(undefined)).toBeNull();
		expect(safeRedirect('')).toBeNull();
	});

	it('rejects scheme-relative payloads that do not start with a slash', () => {
		expect(safeRedirect('javascript:alert(1)')).toBeNull();
		expect(safeRedirect('evil.com')).toBeNull();
		expect(safeRedirect('data:text/html,<script>alert(1)</script>')).toBeNull();
	});

	it('normalises traversal rather than letting it escape the origin', () => {
		// Resolution happens against the sentinel, so ".." can at worst reach "/".
		expect(safeRedirect('/../../etc/passwd')).toBe('/etc/passwd');
		expect(safeRedirect('/a/../b')).toBe('/b');
	});

	it('never returns a value that re-parses to a different origin', () => {
		// The property the whole function exists to guarantee.
		const probes = [
			'/constitution',
			'/settings?tab=email',
			'//evil.com',
			'/\\evil.com',
			'/\t/evil.com',
			'/../../etc/passwd',
			'https://evil.com',
			'/a/../b'
		];
		for (const probe of probes) {
			const out = safeRedirect(probe);
			if (out === null) continue;
			expect(new URL(out, 'https://league.example').origin).toBe('https://league.example');
		}
	});
});
