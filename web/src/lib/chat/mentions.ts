import type { RosterMember } from './types';

/**
 * @mentions, resolved textually against the roster.
 *
 * No sentinel syntax. A `<@14>` form would be unambiguous but leaks into every
 * copied message, every notification email and every plain-text export. With a
 * ten-person roster whose names are known, matching the literal text people
 * actually type is both simpler and what they expect.
 *
 * Resolution is server-authoritative: the handler runs `findMentions` over the
 * content it is about to persist and writes the result. The client never gets to
 * declare who it mentioned, which matters as soon as a mention triggers an email.
 */

/** Names a mention can be written as, longest first. */
function candidateNames(member: RosterMember): string[] {
	const names = new Set<string>();
	if (member.displayName) names.add(member.displayName);
	if (member.name) names.add(member.name);
	return [...names];
}

function escapeRegExp(value: string): string {
	return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * One alternation over every roster name, **longest first**.
 *
 * The ordering is the whole trick: this league contains both "Gabe" and "Gabe
 * the Younger". Shortest-first would match "Gabe" inside "@Gabe the Younger"
 * and attribute the mention to the wrong person.
 */
function buildPattern(roster: RosterMember[]): { regex: RegExp; owner: Map<string, number> } {
	const owner = new Map<string, number>();
	for (const member of roster) {
		for (const name of candidateNames(member)) {
			const key = name.toLowerCase();
			// First writer wins, so a display name never gets stolen by a later
			// member who happens to share it.
			if (!owner.has(key)) owner.set(key, member.managerKey);
		}
	}

	const names = [...owner.keys()].sort((a, b) => b.length - a.length);
	if (names.length === 0) return { regex: /(?!)/g, owner };

	const alternation = names.map(escapeRegExp).join('|');
	// (?<![\w@]) so "email@Gabe" and "@@Gabe" don't count; (?!\w) so "@Nickel"
	// doesn't resolve to Nick.
	return { regex: new RegExp(`(?<![\\w@])@(${alternation})(?!\\w)`, 'gi'), owner };
}

/** Manager keys mentioned in `content`, de-duplicated, in first-appearance order. */
export function findMentions(content: string, roster: RosterMember[]): number[] {
	if (!content.includes('@') || roster.length === 0) return [];

	const { regex, owner } = buildPattern(roster);
	const found: number[] = [];

	for (const match of content.matchAll(regex)) {
		const key = owner.get(match[1].toLowerCase());
		if (key !== undefined && !found.includes(key)) found.push(key);
	}

	return found;
}

export interface MentionSpan {
	start: number;
	end: number;
	managerKey: number;
	text: string;
}

/** Character ranges of each mention, for highlighting. */
export function findMentionSpans(content: string, roster: RosterMember[]): MentionSpan[] {
	if (!content.includes('@') || roster.length === 0) return [];

	const { regex, owner } = buildPattern(roster);
	const spans: MentionSpan[] = [];

	for (const match of content.matchAll(regex)) {
		const key = owner.get(match[1].toLowerCase());
		if (key === undefined || match.index === undefined) continue;
		spans.push({
			start: match.index,
			end: match.index + match[0].length,
			managerKey: key,
			text: match[0]
		});
	}

	return spans;
}

/**
 * Wrap mentions in `<span>`s inside already-sanitized HTML.
 *
 * Runs over text nodes of a parsed fragment rather than the raw string, for two
 * reasons: doing it before sanitization means DOMPurify strips the markup we
 * just added, and doing it as a string replace highlights `@Nick` inside a code
 * fence and inside href attributes.
 *
 * Browser only — it needs a DOM.
 */
export function applyMentionMarkup(
	html: string,
	roster: RosterMember[],
	meManagerKey: number | null
): string {
	if (!html.includes('@') || roster.length === 0) return html;
	if (typeof document === 'undefined') return html;

	const { regex, owner } = buildPattern(roster);
	const template = document.createElement('template');
	template.innerHTML = html;

	const walker = document.createTreeWalker(template.content, NodeFilter.SHOW_TEXT);
	const textNodes: Text[] = [];
	while (walker.nextNode()) {
		const node = walker.currentNode as Text;
		// Skip anything inside code or pre: a mention there is quoted, not addressed.
		if (node.parentElement?.closest('code, pre')) continue;
		if (node.nodeValue?.includes('@')) textNodes.push(node);
	}

	for (const node of textNodes) {
		const text = node.nodeValue ?? '';
		regex.lastIndex = 0;

		const fragment = document.createDocumentFragment();
		let cursor = 0;

		for (const match of text.matchAll(regex)) {
			const key = owner.get(match[1].toLowerCase());
			if (key === undefined || match.index === undefined) continue;

			if (match.index > cursor) {
				fragment.appendChild(document.createTextNode(text.slice(cursor, match.index)));
			}

			const span = document.createElement('span');
			span.className =
				key === meManagerKey
					? 'rounded bg-amber-500/30 px-1 font-medium text-amber-200'
					: 'rounded bg-amber-500/15 px-1 text-amber-300';
			span.textContent = match[0];
			fragment.appendChild(span);

			cursor = match.index + match[0].length;
		}

		if (cursor === 0) continue;
		if (cursor < text.length) {
			fragment.appendChild(document.createTextNode(text.slice(cursor)));
		}
		node.replaceWith(fragment);
	}

	return template.innerHTML;
}
