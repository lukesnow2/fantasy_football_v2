/**
 * A sentinel the redirect target is resolved against.
 *
 * `.invalid` is reserved by RFC 2606 and can never be a real host, so if a
 * target resolves to this origin it is genuinely path-relative.
 */
const SENTINEL_ORIGIN = 'https://redirect-guard.invalid';

/**
 * Only same-origin, path-relative redirects survive.
 *
 * Resolved through the URL parser rather than pattern-matched, because
 * pattern-matching means enumerating every form a browser reads as
 * protocol-relative — and that list is longer than it looks:
 *
 *   //evil.com        the obvious one
 *   /\evil.com        backslash is treated as a slash in special schemes
 *   /<TAB>/evil.com   the URL parser strips ASCII tab/LF/CR *before* parsing,
 *                     so this is literally "//evil.com" by the time a browser
 *                     resolves the Location header
 *
 * The previous version rejected the first two by prefix and let the third
 * through. Delegating to the same parser the browser uses closes that class
 * rather than the three cases someone happened to think of.
 *
 * Control characters are rejected outright instead of stripped: a legitimate
 * internal path never contains one, and silently rewriting the target is how a
 * sanitizer ends up disagreeing with the parser it is trying to model.
 */
export function safeRedirect(target: string | null | undefined): string | null {
	if (!target) return null;

	// C0 controls and DEL. Covers tab/LF/CR (the parser-stripping bypass) and
	// CR/LF header injection in one check.
	if (/[\u0000-\u001F\u007F]/.test(target)) return null;

	// Must be path-relative. Rejects "https://evil.com" and "javascript:..."
	// before the parser ever sees them.
	if (!target.startsWith('/')) return null;

	let resolved: URL;
	try {
		resolved = new URL(target, SENTINEL_ORIGIN);
	} catch {
		return null;
	}

	// Anything that moved off the sentinel origin was not path-relative,
	// whatever it looked like as a string.
	if (resolved.origin !== SENTINEL_ORIGIN) return null;

	return `${resolved.pathname}${resolved.search}${resolved.hash}`;
}
