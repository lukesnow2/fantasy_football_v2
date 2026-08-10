/**
 * Only same-origin, path-relative redirects survive.
 *
 * `//evil.com` and `/\evil.com` are both read as protocol-relative URLs by
 * browsers, so `startsWith('/')` alone is not enough — the old login route
 * passed `?redirect=` straight into `redirect(302, ...)`, which is an open
 * redirect and a ready-made phishing hop off a trusted domain.
 */
export function safeRedirect(target: string | null | undefined): string | null {
	if (!target) return null;
	if (!target.startsWith('/')) return null;
	if (target.startsWith('//') || target.startsWith('/\\')) return null;
	return target;
}
