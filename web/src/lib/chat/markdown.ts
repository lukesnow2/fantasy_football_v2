import DOMPurify from 'dompurify';
import { marked } from 'marked';

/**
 * Chat-grade markdown: bold, italic, strikethrough, code, quotes, lists, links.
 *
 * Browser only, deliberately. DOMPurify needs a real DOM, and the alternative —
 * isomorphic-dompurify — drags jsdom into the server bundle to sanitize content
 * that is never server-rendered (the chat panel is auth-gated and client-fetched).
 */

marked.setOptions({
	gfm: true,
	// Chat, not prose: a single newline is a line break, because that is what
	// pressing shift+enter looks like it should do.
	breaks: true
});

const ALLOWED_TAGS = [
	'p', 'br', 'strong', 'em', 'del', 'code', 'pre',
	'blockquote', 'ul', 'ol', 'li', 'a', 'span'
];

const ALLOWED_ATTR = ['href', 'class', 'rel', 'target'];

let hooksInstalled = false;

function installHooks() {
	if (hooksInstalled || typeof window === 'undefined') return;

	DOMPurify.addHook('afterSanitizeAttributes', (node) => {
		if (!(node instanceof HTMLAnchorElement)) return;

		const href = node.getAttribute('href') ?? '';
		// Allowlist the schemes. DOMPurify already blocks javascript:, but data:
		// and blob: URLs in a link are still a way to hand someone a payload from
		// a message that looks like an ordinary link.
		if (!/^(https?:|mailto:)/i.test(href)) {
			node.removeAttribute('href');
			return;
		}

		node.setAttribute('target', '_blank');
		node.setAttribute('rel', 'noopener noreferrer nofollow');
	});

	hooksInstalled = true;
}

/**
 * Markdown to sanitized HTML.
 *
 * Headings, images and raw HTML are excluded by the allowlist rather than by the
 * parser: nobody needs an `<h1>` in trash talk, and an `<img>` from an arbitrary
 * URL is both an unbounded layout hazard and a tracking pixel.
 */
export function renderMarkdown(source: string): string {
	installHooks();
	const raw = marked.parse(source, { async: false }) as string;
	return DOMPurify.sanitize(raw, {
		ALLOWED_TAGS,
		ALLOWED_ATTR,
		ALLOW_DATA_ATTR: false
	});
}
