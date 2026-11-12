import { requireOrigin } from './env';
import { escapeHtml } from './html';

/**
 * The one layout every league email uses (WP13): the wordmark on the
 * clubhouse ground, a white card, the field button. Matches the sign-in cards
 * on the site.
 *
 * Mail clients can't read CSS variables or the site's stylesheet, and most
 * drop `<style>`, flex and grid, so this is the one place in the app that
 * writes literal hex, inline styles and tables. Every hex is a WP1 token from
 * web/src/app.css, named here, and email-layout.test.ts holds the output of
 * all four emails to this list.
 */
export const EMAIL_COLORS = {
	clubhouse: '#15382A', // --color-clubhouse
	onClubhouse2: '#A9C7B6', // --color-on-clubhouse-2
	card: '#FFFFFF', // --color-card
	ink: '#18231D', // --color-ink
	ink2: '#45524A', // --color-ink-2
	muted: '#5D6A62', // --color-muted
	line: '#DCE0DA', // --color-line
	lineSoft: '#E6E9E4', // --color-line-soft
	wash: '#EDEFEB', // --color-wash
	field: '#1E6B45', // --color-field
	fieldWash: '#EDF4EF', // --color-field-wash
	fieldLine: '#C9DDD0', // --color-field-line
	pennantDark: '#9A3F14' // --color-pennant-dark
} as const;

/**
 * The site's fonts with a fallback each. Apple Mail and iOS Mail load the web
 * fonts from the `<link>` below; Gmail and Outlook ignore it and use the rest
 * of the stack.
 */
export const EMAIL_FONTS = {
	slab: "'Zilla Slab', Georgia, serif",
	sans: "'Instrument Sans', -apple-system, 'Segoe UI', Helvetica, Arial, sans-serif",
	mono: "'IBM Plex Mono', Menlo, Consolas, monospace"
} as const;

const C = EMAIL_COLORS;
const F = EMAIL_FONTS;

const FONT_LINK =
	'https://fonts.googleapis.com/css2?family=Zilla+Slab:wght@500;600;700&family=Instrument+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap';

/** "https://oakdalepark.xyz" is "oakdalepark.xyz". */
export function emailHost(origin: string = requireOrigin()): string {
	return origin.replace(/^[a-z][a-z0-9+.-]*:\/\//i, '').replace(/\/+$/, '');
}

/**
 * The board's "Hi Luke,": the first word of the display name, "Hi," with none.
 *
 * Two greetings, not one. The HTML must be escaped or a display name is an
 * injection point; the text must NOT be, or a manager called O'Brien reads
 * "Hi O&#39;Brien," in their mail client.
 */
export function emailGreeting(displayName: string | null | undefined): {
	html: string;
	text: string;
} {
	const first = (displayName ?? '').trim().split(/\s+/)[0] ?? '';
	const text = first ? `Hi ${first},` : 'Hi,';
	const html = first ? `Hi ${escapeHtml(first)},` : 'Hi,';
	return { html, text };
}

/** The greeting line: 16px ink, 12px above the next paragraph. */
export function emailGreetingHtml(greetingHtml: string): string {
	return `<p style="margin: 0 0 12px; font-family: ${F.sans}; font-size: 16px; line-height: 1.55; color: ${C.ink};">${greetingHtml}</p>`;
}

/** A body paragraph: 16px ink-2. `html` is the caller's, already escaped. */
export function emailParagraph(html: string, marginBottom = 0): string {
	return `<p style="margin: 0 0 ${marginBottom}px; font-family: ${F.sans}; font-size: 16px; line-height: 1.55; color: ${C.ink2};">${html}</p>`;
}

/** A link inside a footnote: field, 600, no underline. Mono for a bare host. */
export function emailFootnoteLink(url: string, label: string, opts: { mono?: boolean } = {}): string {
	const font = opts.mono ? `font-family: ${F.mono}; font-weight: 400;` : 'font-weight: 600;';
	return `<a href="${escapeHtml(url)}" style="color: ${C.field}; ${font} text-decoration: none;">${escapeHtml(label)}</a>`;
}

/**
 * The paste-this-link line and the box under the button. The link is plain
 * text, not a second anchor, and escaped like an href.
 */
export function emailLinkFallback(url: string): string {
	return `<p style="margin: 24px 0 8px; font-family: ${F.sans}; font-size: 14px; line-height: 1.5; color: ${C.muted};">If the button doesn't work, paste this link into your browser:</p>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse: separate;">
<tr><td bgcolor="${C.wash}" style="background: ${C.wash}; border-radius: 8px; padding: 12px 14px; font-family: ${F.mono}; font-size: 13px; line-height: 1.5; color: ${C.ink2}; word-break: break-all;">${escapeHtml(url)}</td></tr>
</table>`;
}

/**
 * The field button: a full-width table whose cell carries the fill, which is
 * what keeps it a green block in Outlook, around a 50px block link.
 */
export function emailButton(url: string, label: string): string {
	// Escaped even though today's URLs cannot break out: the token is
	// encodeURIComponent'd and ORIGIN is operator-set, but that invariant lives
	// two modules away and is one refactor from being false.
	//
	// Turning `&` into `&amp;` inside an href is CORRECT. Attribute values are
	// entity-decoded before the URL is resolved, so a multi-parameter link
	// still works. Nobody should "fix" this back.
	const href = escapeHtml(url);
	return `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
<tr><td style="padding-top: 24px;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse: separate;">
<tr><td class="tl-btn" align="center" bgcolor="${C.field}" style="background: ${C.field}; border-radius: 10px;">
<a class="tl-btn" href="${href}" style="display: block; height: 50px; line-height: 50px; text-align: center; font-family: ${F.sans}; font-size: 16px; font-weight: 700; color: ${C.card}; text-decoration: none; border-radius: 10px;">${escapeHtml(label)}</a>
</td></tr>
</table>
</td></tr>
</table>`;
}

export type KickerTone = 'field' | 'pennant';

export interface EmailLayoutOptions {
	/** The line the inbox shows after the subject. */
	preheader: string;
	kicker: string;
	kickerTone?: KickerTone;
	title: string;
	/** 32 for the sign-in cards, 28 for proposal titles. */
	titleSize?: 32 | 28;
	/** The muted line under the title. Omitted when empty. */
	meta?: string | null;
	/** The caller's markup, already escaped. */
	bodyHtml: string;
	/** The caller's markup, already escaped. */
	footnoteHtml: string;
	/** For tests and previews; defaults to `requireOrigin()`. */
	origin?: string;
}

/**
 * The whole document. Every string argument but `bodyHtml` and
 * `footnoteHtml` is escaped here; callers escape what they put in those two.
 */
export function emailLayout(opts: EmailLayoutOptions): string {
	const kickerColor = opts.kickerTone === 'pennant' ? C.pennantDark : C.field;
	const titleSize = opts.titleSize ?? 32;
	// The board sets 32px titles at 1.1 and 28px proposal titles at 1.15.
	const titleLeading = titleSize === 28 ? 1.15 : 1.1;
	const meta = opts.meta?.trim() ? opts.meta.trim() : '';
	const host = emailHost(opts.origin ?? requireOrigin());

	const titleMargin = meta ? '8px 0 10px' : '8px 0 18px';
	const metaHtml = meta
		? `<p style="margin: 0 0 20px; font-family: ${F.sans}; font-size: 14px; line-height: 1.5; color: ${C.muted};">${escapeHtml(meta)}</p>`
		: '';

	// A run of zero-width joiners and spaces after the preheader, so the inbox
	// preview stops at our line instead of pulling in the wordmark and kicker.
	const preheaderPad = '&zwnj;&nbsp;'.repeat(60);

	return `<!doctype html>
<html lang="en" xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="light">
<meta name="supported-color-schemes" content="light">
<title>${escapeHtml(opts.title)}</title>
<link rel="stylesheet" href="${escapeHtml(FONT_LINK)}">
<style>
@media only screen and (max-width: 480px) {
  .tl-ground { padding: 24px 12px !important; }
  .tl-card { padding: 24px !important; }
  .tl-title-32 { font-size: 28px !important; }
  .tl-btn { height: 52px !important; line-height: 52px !important; }
}
</style>
</head>
<body style="margin: 0; padding: 0; background: ${C.clubhouse};">
<div style="display: none; max-height: 0; overflow: hidden; mso-hide: all; font-size: 1px; line-height: 1px; color: ${C.clubhouse}; opacity: 0;">${escapeHtml(opts.preheader)}${preheaderPad}</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="${C.clubhouse}" style="background: ${C.clubhouse};">
<tr><td class="tl-ground" align="center" style="padding: 32px 24px 28px;">
<!--[if mso]><table role="presentation" width="520" cellpadding="0" cellspacing="0" border="0"><tr><td><![endif]-->
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width: 520px; margin: 0 auto;">
<tr><td style="padding: 0 4px 20px; font-family: ${F.sans}; text-align: left;"><span style="font-family: ${F.slab}; font-weight: 700; font-size: 24px; color: ${C.card};">The League</span><span style="margin-left: 10px; font-family: ${F.sans}; font-size: 11px; font-weight: 600; letter-spacing: 0.1em; text-transform: uppercase; color: ${C.onClubhouse2};">Est. 2005</span></td></tr>
<tr><td class="tl-card" bgcolor="${C.card}" style="background: ${C.card}; border-radius: 16px; padding: 36px; text-align: left; font-family: ${F.sans}; color: ${C.ink};">
<div style="font-family: ${F.sans}; font-size: 13px; font-weight: 700; letter-spacing: 0.1em; text-transform: uppercase; color: ${kickerColor};">${escapeHtml(opts.kicker)}</div>
<h1 class="tl-title-${titleSize}" style="margin: ${titleMargin}; font-family: ${F.slab}; font-weight: 700; font-size: ${titleSize}px; line-height: ${titleLeading}; color: ${C.ink};">${escapeHtml(opts.title)}</h1>
${metaHtml}${opts.bodyHtml}
<p style="margin: 24px 0 0; padding-top: 16px; border-top: 1px solid ${C.lineSoft}; font-family: ${F.sans}; font-size: 14px; line-height: 1.5; color: ${C.muted};">${opts.footnoteHtml}</p>
</td></tr>
<tr><td style="padding: 18px 4px 0; font-family: ${F.sans}; font-size: 13px; line-height: 1.5; color: ${C.onClubhouse2}; text-align: left;">The League · <span style="font-family: ${F.mono};">${escapeHtml(host)}</span></td></tr>
</table>
<!--[if mso]></td></tr></table><![endif]-->
</td></tr>
</table>
</body>
</html>`;
}

/**
 * The width inside the facts table at desktop: the 520px column, less the
 * card's 36px padding each side and the table's 1px border each side.
 */
const FACTS_INNER_WIDTH = 520 - 2 * 36 - 2;
const FACTS_CELL_PADDING = 2 * 14;

/**
 * The facts row under a proposal: line-bordered, 10px radius, one cell per
 * fact with a line-soft rule between. `weight` is the board's flex value.
 *
 * The board's `flex: n` shares out the width left after each cell's padding
 * and rule, not the whole width, so each cell's percentage is worked out the
 * same way at desktop width. A plain weight-over-total split puts the
 * vote-result dividers a few pixels off.
 */
export function emailFacts(
	cells: Array<{ value: string; label: string; color?: string; weight?: number }>
): string {
	const total = cells.reduce((sum, c) => sum + (c.weight ?? 1), 0);
	const rules = cells.length - 1;
	const free = FACTS_INNER_WIDTH - cells.length * FACTS_CELL_PADDING - rules;
	const tds = cells
		.map((c, i) => {
			const hasRule = i < rules;
			const px = FACTS_CELL_PADDING + (hasRule ? 1 : 0) + (free * (c.weight ?? 1)) / total;
			const width = ((px / FACTS_INNER_WIDTH) * 100).toFixed(2);
			const rule = hasRule ? ` border-right: 1px solid ${C.lineSoft};` : '';
			return `<td width="${width}%" valign="top" style="padding: 12px 14px;${rule}"><div style="font-family: ${F.slab}; font-weight: 700; font-size: 22px; color: ${c.color ?? C.ink};">${escapeHtml(c.value)}</div><div style="font-family: ${F.sans}; font-size: 12px; color: ${C.muted};">${escapeHtml(c.label)}</div></td>`;
		})
		.join('');
	return `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse: separate; border: 1px solid ${C.line}; border-radius: 10px;"><tr>${tds}</tr></table>`;
}
